#include "Service.h"
#include "common/Clock.h"
#include "common/config.h"
#include "include/stringify.h"
#include "include/ceph_assert.h"
#include "mon/MonOpRequest.h"
#include "SMRProtocol.h"
#include "AbstractMonitor.h"

using std::ostream;
using std::string;

using ceph::bufferlist;

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, smr_protocol, service_name, get_first_committed(), get_last_committed())

static ostream& _prefix(std::ostream *_dout, AbstractMonitor &mon, SMRProtocol &smr_protocol, string service_name,
                        version_t fc, version_t lc) {
    return *_dout << "mon." << mon.name << "@" << mon.rank
                  << "(" << mon.get_state_name()
                  << ").paxosservice(" << service_name << " " << fc << ".." << lc << ") ";
}

void Service::load_health() {
    bufferlist bl;
    mon.store->get("health", service_name, bl);
    if (bl.length()) {
        auto p = bl.cbegin();
        using ceph::decode;
        decode(health_checks, p);
    }
}

void Service::_active()
{
    if (is_proposing()) {
        dout(10) << __func__ << " - proposing" << dendl;
        return;
    }
    if (!is_active()) {
        dout(10) << __func__ << " - not active" << dendl;
        /**
         * Callback used to make sure we call the PaxosService::_active function
         * whenever a condition is fulfilled.
         *
         * This is used in multiple situations, from waiting for the Paxos to commit
         * our proposed value, to waiting for the Paxos to become active once an
         * election is finished.
         */
        class C_Active : public Context {
            Service *svc;
        public:
            explicit C_Active(Service *s) : svc(s) {}

            void finish(int r) override {
                if (r >= 0)
                    svc->_active();
            }
        };

        wait_for_active_ctx(new C_Active(this));
        return;
    }
    dout(10) << __func__ << dendl;

    // create pending state?
    if (mon.is_leader()) {
        dout(7) << __func__ << " creating new pending" << dendl;
        if (!have_pending) {
            create_pending();
            have_pending = true;
        }

        if (get_last_committed() == 0) {
            // create initial state
            create_initial();
            propose_pending();
            return;
        }
    } else {
        dout(7) << __func__ << " we are not the leader, hence we propose nothing!" << dendl;
    }

    // wake up anyone who came in while we were proposing.  note that
    // anyone waiting for the previous proposal to commit is no longer
    // on this list; it is on Paxos's.
    finish_contexts(g_ceph_context, waiting_for_finished_proposal, 0);

    if (mon.is_leader())
        upgrade_format();

    // NOTE: it's possible that this will get called twice if we commit
    // an old paxos value.  Implementations should be mindful of that.
    on_active();
}

void Service::propose_pending()
{
    dout(10) << __func__ << dendl;
    ceph_assert(have_pending);
    ceph_assert(!proposing);
    //TODO: Fix this?
    ceph_assert(mon.is_leader());
    ceph_assert(is_active());

    if (proposal_timer) {
        dout(10) << " canceling proposal_timer " << proposal_timer << dendl;
        mon.timer.cancel_event(proposal_timer);
        proposal_timer = NULL;
    }

    /**
     * @note What we contribute to the pending Paxos transaction is
     *	   obtained by calling a function that must be implemented by
     *	   the class implementing us.  I.e., the function
     *	   encode_pending will be the one responsible to encode
     *	   whatever is pending on the implementation class into a
     *	   bufferlist, so we can then propose that as a value through
     *	   Paxos.
     */
    MonitorDBStore::TransactionRef t = smr_protocol.get_pending_transaction();

    if (should_stash_full())
        encode_full(t);

    encode_pending(t);
    have_pending = false;

    if (format_version > 0) {
        t->put(get_service_name(), "format_version", format_version);
    }

    // apply to paxos
    proposing = true;
    /**
     * Callback class used to mark us as active once a proposal finishes going
     * through Paxos.
     *
     * We should wake people up *only* *after* we inform the service we
     * just went active. And we should wake people up only once we finish
     * going active. This is why we first go active, avoiding to wake up the
     * wrong people at the wrong time, such as waking up a C_RetryMessage
     * before waking up a C_Active, thus ending up without a pending value.
     */
    class C_Committed : public Context {
        Service *ps;
    public:
        explicit C_Committed(Service *p) : ps(p) { }
        void finish(int r) override {
            ps->proposing = false;
            if (r >= 0)
                ps->_active();
            else if (r == -ECANCELED || r == -EAGAIN)
                return;
            else
                ceph_abort_msg("bad return value for C_Committed");
        }
    };

    smr_protocol.queue_pending_finisher(new C_Committed(this));
    smr_protocol.trigger_propose();
}

bool Service::should_propose(double &delay) {
    // simple default policy: quick startup, then some damping.
    if (get_last_committed() <= 1) {
        delay = 0.0;
    } else {
        utime_t now = ceph_clock_now();
        if ((now - smr_protocol.get_last_commit_time()) > g_conf()->paxos_propose_interval)
            delay = (double) g_conf()->paxos_min_wait;
        else
            delay = (double) (g_conf()->paxos_propose_interval + smr_protocol.get_last_commit_time()
                              - now);
    }
    return true;
}



void Service::election_finished()
{
    dout(10) << __func__ << dendl;

    finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

    // make sure we update our state
    _active();
}


void Service::shutdown() {
    cancel_events();

    if (proposal_timer) {
        dout(10) << " canceling proposal_timer " << proposal_timer << dendl;
        mon.timer.cancel_event(proposal_timer);
        proposal_timer = 0;
    }

    finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

    on_shutdown();
}

void Service::restart()
{
    dout(10) << __func__ << dendl;
    if (proposal_timer) {
        dout(10) << " canceling proposal_timer " << proposal_timer << dendl;
        mon.timer.cancel_event(proposal_timer);
        proposal_timer = 0;
    }

    finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

    if (have_pending) {
        discard_pending();
        have_pending = false;
    }

    proposing = false;

    on_restart();
}

bool Service::dispatch(MonOpRequestRef op) {
    ceph_assert(op->is_type_service() || op->is_type_command());
    auto m = op->get_req<PaxosServiceMessage>();
    op->mark_event("psvc:dispatch");

    dout(10) << __func__ << " " << m << " " << *m
             << " from " << m->get_orig_source_inst()
             << " con " << m->get_connection() << dendl;

    if (mon.is_shutdown()) {
        return true;
    }

    // make sure this message isn't forwarded from a previous election epoch
    if (m->rx_election_epoch &&
        m->rx_election_epoch < mon.get_epoch()) {
        dout(10) << " discarding forwarded message from previous election epoch "
                 << m->rx_election_epoch << " < " << mon.get_epoch() << dendl;
        return true;
    }

    // make sure the client is still connected.  note that a proxied
    // connection will be disconnected with a null message; don't drop
    // those.  also ignore loopback (e.g., log) messages.
    if (m->get_connection() &&
        !m->get_connection()->is_connected() &&
        m->get_connection() != mon.con_self &&
        m->get_connection()->get_messenger() != NULL) {
        dout(10) << " discarding message from disconnected client "
                 << m->get_source_inst() << " " << *m << dendl;
        return true;
    }

    // make sure our map is readable and up to date
    if (!is_readable(m->version)) {
        dout(10) << " waiting for paxos -> readable (v" << m->version << ")" << dendl;
        wait_for_readable(op, new C_RetryMessage(this, op), m->version);
        return true;
    }

    // preprocess
    if (preprocess_query(op))
        return true;  // easy!

    // leader?
    //TODO: This is no fckn good.
    //How do we do it?
    if (!mon.is_leader()) {
        mon.forward_request_leader(op);
        return true;
    }

    // writeable?
    if (!is_writeable()) {
        dout(10) << " waiting for paxos -> writeable" << dendl;
        wait_for_writeable(op, new C_RetryMessage(this, op));
        return true;
    }

    // update
    if (!prepare_update(op)) {
        // no changes made.
        return true;
    }

    if (need_immediate_propose) {
        dout(10) << __func__ << " forced immediate propose" << dendl;
        need_immediate_propose = false;
        propose_pending();
        return true;
    }

    double delay = 0.0;
    if (!should_propose(delay)) {
        dout(10) << " not proposing" << dendl;
        return true;
    }

    if (delay == 0.0) {
        propose_pending();
        return true;
    }

    // delay a bit
    if (!proposal_timer) {
        /**
           * Callback class used to propose the pending value once the proposal_timer
           * fires up.
           */
        auto do_propose = new C_MonContext{&mon, [this](int r) {
            proposal_timer = 0;
            if (r >= 0) {
                propose_pending();
            } else if (r == -ECANCELED || r == -EAGAIN) {
                return;
            } else {
                ceph_abort_msg("bad return value for proposal_timer");
            }
        }};
        dout(10) << " setting proposal_timer " << do_propose
                 << " with delay of " << delay << dendl;
        proposal_timer = mon.timer.add_event_after(delay, do_propose);
    } else {
        dout(10) << " proposal_timer already set" << dendl;
    }
    return true;
}

void Service::maybe_trim() {
    if (!is_writeable())
        return;

    const version_t first_committed = get_first_committed();
    version_t trim_to = get_trim_to();
    dout(20) << __func__ << " " << first_committed << "~" << trim_to << dendl;

    if (trim_to < first_committed) {
        dout(10) << __func__ << " trim_to " << trim_to << " < first_committed "
                 << first_committed << dendl;
        return;
    }

    version_t to_remove = trim_to - first_committed;
    const version_t trim_min = g_conf().get_val<version_t>("paxos_service_trim_min");
    if (trim_min > 0 &&
        to_remove < trim_min) {
        dout(10) << __func__ << " trim_to " << trim_to << " would only trim " << to_remove
                 << " < paxos_service_trim_min " << trim_min << dendl;
        return;
    }

    to_remove = [to_remove, trim_to, this] {
        const version_t trim_max = g_conf().get_val<version_t>("paxos_service_trim_max");
        if (trim_max == 0 || to_remove < trim_max) {
            return to_remove;
        }
        if (to_remove < trim_max * 1.5) {
            dout(10) << __func__ << " trim to " << trim_to << " would only trim " << to_remove
                     << " > paxos_service_trim_max, limiting to " << trim_max
                     << dendl;
            return trim_max;
        }
        const version_t new_trim_max = (trim_max + to_remove) / 2;
        const uint64_t trim_max_multiplier = g_conf().get_val<uint64_t>("paxos_service_trim_max_multiplier");
        if (trim_max_multiplier) {
            return std::min(new_trim_max, trim_max * trim_max_multiplier);
        } else {
            return new_trim_max;
        }
    }();
    trim_to = first_committed + to_remove;

    dout(10) << __func__ << " trimming to " << trim_to << ", " << to_remove << " states" << dendl;
    MonitorDBStore::TransactionRef t = smr_protocol.get_pending_transaction();
    trim(t, first_committed, trim_to);
    put_first_committed(t, trim_to);
    cached_first_committed = trim_to;

    // let the service add any extra stuff
    encode_trim_extra(t, trim_to);

    smr_protocol.trigger_propose();
}

void Service::refresh(bool *need_bootstrap) {
    // update cached versions
    cached_first_committed = get_value(first_committed_name);
    cached_last_committed = get_value(last_committed_name);

    version_t new_format = get_value("format_version");
    if (new_format != format_version) {
        dout(1) << __func__ << " upgraded, format " << format_version << " -> " << new_format << dendl;
        on_upgrade();
    }
    format_version = new_format;

    dout(10) << __func__ << dendl;

    update_from_smr(need_bootstrap);
}

void Service::post_refresh()
{
    dout(10) << __func__ << dendl;

    post_smr_update();

    //TODO: Fix this
    /*
    if (mon.is_peon() && !waiting_for_finished_proposal.empty()) {
        finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);
    }
     */
}

void Service::trim(MonitorDBStore::TransactionRef t,
                   version_t from, version_t to) {
    dout(10) << __func__ << " from " << from << " to " << to << dendl;
    ceph_assert(from != to);

    for (version_t v = from; v < to; ++v) {
        dout(20) << __func__ << " " << v << dendl;
        t->erase(get_service_name(), v);

        std::string full_key = smr_protocol.combine_strings("full", v);
        if (smr_protocol.exists_in_service(get_service_name(), full_key)) {
            dout(20) << __func__ << " " << full_key << dendl;
            t->erase(get_service_name(), full_key);
        }
    }

    if (g_conf()->mon_compact_on_trim) {
        dout(20) << " compacting prefix " << get_service_name() << dendl;
        t->compact_range(get_service_name(), stringify(from - 1), stringify(to));
        t->compact_range(get_service_name(),
                         smr_protocol.combine_strings(full_prefix_name, from - 1),
                         smr_protocol.combine_strings(full_prefix_name, to));
    }
}

bool Service::should_stash_full()
{
    version_t latest_full = get_version_latest_full();
    /* @note The first member of the condition is moot and it is here just for
     *	   clarity's sake. The second member would end up returing true
     *	   nonetheless because, in that event,
     *	      latest_full == get_trim_to() == 0.
     */
    return (!latest_full ||
            (latest_full <= get_trim_to()) ||
            (get_last_committed() - latest_full > (version_t)g_conf()->paxos_stash_full_interval));
}