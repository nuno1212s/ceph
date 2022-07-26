#include "Service.h"


void Service::load_health()
{
    bufferlist bl;
    mon.store->get("health", service_name, bl);
    if (bl.length()) {
        auto p = bl.cbegin();
        using ceph::decode;
        decode(health_checks, p);
    }
}

bool Service::should_propose(double& delay)
{
    // simple default policy: quick startup, then some damping.
    if (get_last_committed() <= 1) {
        delay = 0.0;
    } else {
        utime_t now = ceph_clock_now();
        if ((now - paxos.last_commit_time) > g_conf()->paxos_propose_interval)
            delay = (double)g_conf()->paxos_min_wait;
        else
            delay = (double)(g_conf()->paxos_propose_interval + paxos.last_commit_time
                             - now);
    }
    return true;
}

bool Service::dispatch(MonOpRequestRef op)
{
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


void Service::maybe_trim()
{
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
    MonitorDBStore::TransactionRef t = paxos.get_pending_transaction();
    trim(t, first_committed, trim_to);
    put_first_committed(t, trim_to);
    cached_first_committed = trim_to;

    // let the service add any extra stuff
    encode_trim_extra(t, trim_to);

    //TODO
    smr_protocol.trigger_propose();
}

void Service::trim(MonitorDBStore::TransactionRef t,
                        version_t from, version_t to)
{
    dout(10) << __func__ << " from " << from << " to " << to << dendl;
    ceph_assert(from != to);

    for (version_t v = from; v < to; ++v) {
        dout(20) << __func__ << " " << v << dendl;
        t->erase(get_service_name(), v);

        string full_key = mon.store->combine_strings("full", v);
        if (mon.store->exists(get_service_name(), full_key)) {
            dout(20) << __func__ << " " << full_key << dendl;
            t->erase(get_service_name(), full_key);
        }
    }
    if (g_conf()->mon_compact_on_trim) {
        dout(20) << " compacting prefix " << get_service_name() << dendl;
        t->compact_range(get_service_name(), stringify(from - 1), stringify(to));
        t->compact_range(get_service_name(),
                         mon.store->combine_strings(full_prefix_name, from - 1),
                         mon.store->combine_strings(full_prefix_name, to));
    }
}