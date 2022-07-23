#include "PaxosMonitor.h"


#include <iterator>
#include <sstream>
#include <tuple>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>
#include <cstring>
#include <boost/scope_exit.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "json_spirit/json_spirit_reader.h"
#include "json_spirit/json_spirit_writer.h"

#include "common/version.h"
#include "common/blkdev.h"
#include "common/cmdparse.h"
#include "common/signal.h"

#include "osd/OSDMap.h"

#include "MonitorDBStore.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonSync.h"
#include "messages/MMonScrub.h"
#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MTimeCheck2.h"
#include "messages/MPing.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "global/signal_handler.h"
#include "common/Formatter.h"
#include "include/stringify.h"
#include "include/color.h"
#include "include/ceph_fs.h"
#include "include/str_list.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "MonmapMonitor.h"
#include "LogMonitor.h"
#include "AuthMonitor.h"
#include "MgrMonitor.h"
#include "MgrStatMonitor.h"
#include "ConfigMonitor.h"
#include "KVMonitor.h"
#include "mon/HealthMonitor.h"
#include "common/config.h"
#include "common/cmdparse.h"
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "perfglue/heap_profiler.h"

#include "auth/none/AuthNoneClientHandler.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
using namespace TOPNSPC::common;

using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::ErasureCodeInterfaceRef;
using ceph::ErasureCodeProfile;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;

void AbstractMonitor::_quorum_status(Formatter *f, ostream &ss) {
    bool free_formatter = false;

    if (!f) {
        // louzy/lazy hack: default to json if no formatter has been defined
        f = new JSONFormatter();
        free_formatter = true;
    }
    f->open_object_section("quorum_status");
    f->dump_int("election_epoch", get_epoch());

    f->open_array_section("quorum");
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
        f->dump_int("mon", *p);
    f->close_section(); // quorum

    list<string> quorum_names = get_quorum_names();
    f->open_array_section("quorum_names");
    for (list<string>::iterator p = quorum_names.begin(); p != quorum_names.end(); ++p)
        f->dump_string("mon", *p);
    f->close_section(); // quorum_names

    f->dump_string("quorum_leader_name", quorum.empty() ? string() : monmap->get_name(leader));

    if (!quorum.empty()) {
        f->dump_int(
                "quorum_age",
                quorum_age());
    }

    f->open_object_section("features");
    f->dump_stream("quorum_con") << quorum_con_features;
    quorum_mon_features.dump(f, "quorum_mon");
    f->close_section();

    f->open_object_section("monmap");
    monmap->dump(f);
    f->close_section(); // monmap

    f->close_section(); // quorum_status
    f->flush(ss);
    if (free_formatter)
        delete f;
}

void AbstractMonitor::get_mon_status(Formatter *f) {
    f->open_object_section("mon_status");
    f->dump_string("name", name);
    f->dump_int("rank", rank);
    f->dump_string("state", get_state_name());
    f->dump_int("election_epoch", get_epoch());

    f->open_array_section("quorum");
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p) {
        f->dump_int("mon", *p);
    }
    f->close_section(); // quorum

    if (!quorum.empty()) {
        f->dump_int(
                "quorum_age",
                quorum_age());
    }

    f->open_object_section("features");
    f->dump_stream("required_con") << required_features;
    mon_feature_t req_mon_features = get_required_mon_features();
    req_mon_features.dump(f, "required_mon");
    f->dump_stream("quorum_con") << quorum_con_features;
    quorum_mon_features.dump(f, "quorum_mon");
    f->close_section(); // features

    f->open_array_section("outside_quorum");
    for (set<string>::iterator p = outside_quorum.begin(); p != outside_quorum.end(); ++p)
        f->dump_string("mon", *p);
    f->close_section(); // outside_quorum

    f->open_array_section("extra_probe_peers");
    for (set<entity_addrvec_t>::iterator p = extra_probe_peers.begin();
         p != extra_probe_peers.end();
         ++p) {
        f->dump_object("peer", *p);
    }
    f->close_section(); // extra_probe_peers

    f->open_array_section("sync_provider");
    for (map<uint64_t, SyncProvider>::const_iterator p = sync_providers.begin();
         p != sync_providers.end();
         ++p) {
        f->dump_unsigned("cookie", p->second.cookie);
        f->dump_object("addrs", p->second.addrs);
        f->dump_stream("timeout") << p->second.timeout;
        f->dump_unsigned("last_committed", p->second.last_committed);
        f->dump_stream("last_key") << p->second.last_key;
    }
    f->close_section();

    if (is_synchronizing()) {
        f->open_object_section("sync");
        f->dump_stream("sync_provider") << sync_provider;
        f->dump_unsigned("sync_cookie", sync_cookie);
        f->dump_unsigned("sync_start_version", sync_start_version);
        f->close_section();
    }

    if (g_conf()->mon_sync_provider_kill_at > 0)
        f->dump_int("provider_kill_at", g_conf()->mon_sync_provider_kill_at);
    if (g_conf()->mon_sync_requester_kill_at > 0)
        f->dump_int("requester_kill_at", g_conf()->mon_sync_requester_kill_at);

    f->open_object_section("monmap");
    monmap->dump(f);
    f->close_section();

    f->dump_object("feature_map", session_map.feature_map);
    f->dump_bool("stretch_mode", stretch_mode_engaged);
    f->close_section(); // mon_status
}


// ------------------------
// request/reply routing
//
// a client/mds/osd will connect to a random monitor.  we need to forward any
// messages requiring state updates to the leader, and then route any replies
// back via the correct monitor and back to them.  (the monitor will not
// initiate any connections.)

void PaxosMonitor::forward_request_leader(MonOpRequestRef op) {
    op->mark_event(__func__);

    int mon = get_leader();
    MonSession *session = op->get_session();
    PaxosServiceMessage *req = op->get_req<PaxosServiceMessage>();

    if (req->get_source().is_mon() && req->get_source_addrs() != messenger->get_myaddrs()) {
        dout(10) << "forward_request won't forward (non-local) mon request " << *req << dendl;
    } else if (session->proxy_con) {
        dout(10) << "forward_request won't double fwd request " << *req << dendl;
    } else if (!session->closed) {
        RoutedRequest *rr = new RoutedRequest;
        rr->tid = ++routed_request_tid;
        rr->con = req->get_connection();
        rr->con_features = rr->con->get_features();
        encode_message(req, CEPH_FEATURES_ALL, rr->request_bl);   // for my use only; use all features
        rr->session = static_cast<MonSession *>(session->get());
        rr->op = op;
        routed_requests[rr->tid] = rr;
        session->routed_request_tids.insert(rr->tid);

        dout(10) << "forward_request " << rr->tid << " request " << *req
                 << " features " << rr->con_features << dendl;

        MForward *forward = new MForward(rr->tid,
                                         req,
                                         rr->con_features,
                                         rr->session->caps);
        forward->set_priority(req->get_priority());
        if (session->auth_handler) {
            forward->entity_name = session->entity_name;
        } else if (req->get_source().is_mon()) {
            forward->entity_name.set_type(CEPH_ENTITY_TYPE_MON);
        }
        send_mon_message(forward, mon);
        op->mark_forwarded();
        ceph_assert(op->get_req()->get_type() != 0);
    } else {
        dout(10) << "forward_request no session for request " << *req << dendl;
    }
}

// fake connection attached to forwarded messages
struct AnonConnection : public Connection {
    entity_addr_t socket_addr;

    int send_message(Message *m) override {
        ceph_assert(!"send_message on anonymous connection");
    }

    void send_keepalive() override {
        ceph_assert(!"send_keepalive on anonymous connection");
    }

    void mark_down() override {
        // silently ignore
    }

    void mark_disposable() override {
        // silengtly ignore
    }

    bool is_connected() override { return false; }

    entity_addr_t get_peer_socket_addr() const override {
        return socket_addr;
    }

private:
    FRIEND_MAKE_REF(AnonConnection);

    explicit AnonConnection(CephContext *cct, const entity_addr_t &sa)
            : Connection(cct, nullptr),
              socket_addr(sa) {}
};

//extract the original message and put it into the regular dispatch function
void PaxosMonitor::handle_forward(MonOpRequestRef op) {
    auto m = op->get_req<MForward>();
    dout(10) << "received forwarded message from "
             << ceph_entity_type_name(m->client_type)
             << " " << m->client_addrs
             << " via " << m->get_source_inst() << dendl;
    MonSession *session = op->get_session();
    ceph_assert(session);

    if (!session->is_capable("mon", MON_CAP_X)) {
        dout(0) << "forward from entity with insufficient caps! "
                << session->caps << dendl;
    } else {
        // see PaxosService::dispatch(); we rely on this being anon
        // (c->msgr == NULL)
        PaxosServiceMessage *req = m->claim_message();
        ceph_assert(req != NULL);

        auto c = ceph::make_ref<AnonConnection>(cct, m->client_socket_addr);
        MonSession *s = new MonSession(static_cast<Connection *>(c.get()));
        s->_ident(req->get_source(),
                  req->get_source_addrs());
        c->set_priv(RefCountedPtr{s, false});
        c->set_peer_addrs(m->client_addrs);
        c->set_peer_type(m->client_type);
        c->set_features(m->con_features);

        s->authenticated = true;
        s->caps = m->client_caps;
        dout(10) << " caps are " << s->caps << dendl;
        s->entity_name = m->entity_name;
        dout(10) << " entity name '" << s->entity_name << "' type "
                 << s->entity_name.get_type() << dendl;
        s->proxy_con = m->get_connection();
        s->proxy_tid = m->tid;

        req->set_connection(c);

        // not super accurate, but better than nothing.
        req->set_recv_stamp(m->get_recv_stamp());

        /*
         * note which election epoch this is; we will drop the message if
         * there is a future election since our peers will resend routed
         * requests in that case.
         */
        req->rx_election_epoch = get_epoch();

        dout(10) << " mesg " << req << " from " << m->get_source_addr() << dendl;
        _ms_dispatch(req);

        // break the session <-> con ref loop by removing the con->session
        // reference, which is no longer needed once the MonOpRequest is
        // set up.
        c->set_priv(NULL);
    }
}

void PaxosMonitor::resend_routed_requests() {
    dout(10) << "resend_routed_requests" << dendl;
    int mon = get_leader();
    list<Context *> retry;
    for (map<uint64_t, RoutedRequest *>::iterator p = routed_requests.begin();
         p != routed_requests.end();
         ++p) {
        RoutedRequest *rr = p->second;

        if (mon == rank) {
            dout(10) << " requeue for self tid " << rr->tid << dendl;
            rr->op->mark_event("retry routed request");
            retry.push_back(new C_RetryMessage(this, rr->op));
            if (rr->session) {
                ceph_assert(rr->session->routed_request_tids.count(p->first));
                rr->session->routed_request_tids.erase(p->first);
            }
            delete rr;
        } else {
            auto q = rr->request_bl.cbegin();
            PaxosServiceMessage *req =
                    (PaxosServiceMessage *) decode_message(cct, 0, q);
            rr->op->mark_event("resend forwarded message to leader");
            dout(10) << " resend to mon." << mon << " tid " << rr->tid << " " << *req
                     << dendl;
            MForward *forward = new MForward(rr->tid,
                                             req,
                                             rr->con_features,
                                             rr->session->caps);
            req->put();  // forward takes its own ref; drop ours.
            forward->client_type = rr->con->get_peer_type();
            forward->client_addrs = rr->con->get_peer_addrs();
            forward->client_socket_addr = rr->con->get_peer_socket_addr();
            forward->set_priority(req->get_priority());
            send_mon_message(forward, mon);
        }
    }
    if (mon == rank) {
        routed_requests.clear();
        finish_contexts(g_ceph_context, retry);
    }
}

void PaxosMonitor::set_mon_crush_location(const string &loc) {
    if (loc.empty()) {
        return;
    }
    vector<string> loc_vec;
    loc_vec.push_back(loc);
    CrushWrapper::parse_loc_map(loc_vec, &crush_loc);
    need_set_crush_loc = true;
}

void PaxosMonitor::notify_new_monmap(bool can_change_external_state) {
    if (need_set_crush_loc) {
        auto my_info_i = monmap->mon_info.find(name);
        if (my_info_i != monmap->mon_info.end() &&
            my_info_i->second.crush_loc == crush_loc) {
            need_set_crush_loc = false;
        }
    }
    elector.notify_strategy_maybe_changed(monmap->strategy);
    dout(30) << __func__ << "we have " << monmap->removed_ranks.size() << " removed ranks" << dendl;
    for (auto i = monmap->removed_ranks.rbegin();
         i != monmap->removed_ranks.rend(); ++i) {
        int rank = *i;
        dout(10) << __func__ << "removing rank " << rank << dendl;
        elector.notify_rank_removed(rank);
    }

    if (monmap->stretch_mode_enabled) {
        try_engage_stretch_mode();
    }

    if (is_stretch_mode()) {
        if (!monmap->stretch_marked_down_mons.empty()) {
            set_degraded_stretch_mode();
        }
    }

    set_elector_disallowed_leaders(can_change_external_state);
}

void PaxosMonitor::waitlist_or_zap_client(MonOpRequestRef op) {
    /**
     * Wait list the new session until we're in the quorum, assuming it's
     * sufficiently new.
     * tick() will periodically send them back through so we can send
     * the client elsewhere if we don't think we're getting back in.
     *
     * But we allow a few sorts of messages:
     * 1) Monitors can talk to us at any time, of course.
     * 2) auth messages. It's unlikely to go through much faster, but
     * it's possible we've just lost our quorum status and we want to take...
     * 3) command messages. We want to accept these under all possible
     * circumstances.
     */
    Message *m = op->get_req();
    MonSession *s = op->get_session();
    ConnectionRef con = op->get_connection();
    utime_t too_old = ceph_clock_now();
    too_old -= g_ceph_context->_conf->mon_lease;
    if (m->get_recv_stamp() > too_old &&
        con->is_connected()) {
        dout(5) << "waitlisting message " << *m << dendl;
        maybe_wait_for_quorum.push_back(new C_RetryMessage(this, op));
        op->mark_wait_for_quorum();
    } else {
        dout(5) << "discarding message " << *m << " and sending client elsewhere" << dendl;
        con->mark_down();
        // proxied sessions aren't registered and don't have a con; don't remove
        // those.
        if (!s->proxy_con) {
            std::lock_guard l(session_map_lock);
            remove_session(s);
        }
        op->mark_zap();
    }
}

void PaxosMonitor::_ms_dispatch(Message *m) {
    if (is_shutdown()) {
        m->put();
        return;
    }

    MonOpRequestRef op = op_tracker.create_request<MonOpRequest>(m);
    bool src_is_mon = op->is_src_mon();
    op->mark_event("mon:_ms_dispatch");
    MonSession *s = op->get_session();
    if (s && s->closed) {
        return;
    }

    if (src_is_mon && s) {
        ConnectionRef con = m->get_connection();
        if (con->get_messenger() && con->get_features() != s->con_features) {
            // only update features if this is a non-anonymous connection
            dout(10) << __func__ << " feature change for " << m->get_source_inst()
                     << " (was " << s->con_features
                     << ", now " << con->get_features() << ")" << dendl;
            // connection features changed - recreate session.
            if (s->con && s->con != con) {
                dout(10) << __func__ << " connection for " << m->get_source_inst()
                         << " changed from session; mark down and replace" << dendl;
                s->con->mark_down();
            }
            if (s->item.is_on_list()) {
                // forwarded messages' sessions are not in the sessions map and
                // exist only while the op is being handled.
                std::lock_guard l(session_map_lock);
                remove_session(s);
            }
            s = nullptr;
        }
    }

    if (!s) {
        // if the sender is not a monitor, make sure their first message for a
        // session is an MAuth.  If it is not, assume it's a stray message,
        // and considering that we are creating a new session it is safe to
        // assume that the sender hasn't authenticated yet, so we have no way
        // of assessing whether we should handle it or not.
        if (!src_is_mon && (m->get_type() != CEPH_MSG_AUTH &&
                            m->get_type() != CEPH_MSG_MON_GET_MAP &&
                            m->get_type() != CEPH_MSG_PING)) {
            dout(1) << __func__ << " dropping stray message " << *m
                    << " from " << m->get_source_inst() << dendl;
            return;
        }

        ConnectionRef con = m->get_connection();
        {
            std::lock_guard l(session_map_lock);
            s = session_map.new_session(m->get_source(),
                                        m->get_source_addrs(),
                                        con.get());
        }
        ceph_assert(s);
        con->set_priv(RefCountedPtr{s, false});
        dout(10) << __func__ << " new session " << s << " " << *s
                 << " features 0x" << std::hex
                 << s->con_features << std::dec << dendl;
        op->set_session(s);

        logger->set(l_mon_num_sessions, session_map.get_size());
        logger->inc(l_mon_session_add);

        if (src_is_mon) {
            // give it monitor caps; the peer type has been authenticated
            dout(5) << __func__ << " setting monitor caps on this connection" << dendl;
            if (!s->caps.is_allow_all()) // but no need to repeatedly copy
                s->caps = mon_caps;
            s->authenticated = true;
        }
    } else {
        dout(20) << __func__ << " existing session " << s << " for " << s->name
                 << dendl;
    }

    ceph_assert(s);

    s->session_timeout = ceph_clock_now();
    s->session_timeout += g_conf()->mon_session_timeout;

    if (s->auth_handler) {
        s->entity_name = s->auth_handler->get_entity_name();
        s->global_id = s->auth_handler->get_global_id();
        s->global_id_status = s->auth_handler->get_global_id_status();
    }
    dout(20) << " entity_name " << s->entity_name
             << " global_id " << s->global_id
             << " (" << s->global_id_status
             << ") caps " << s->caps.get_str() << dendl;

    if (!session_stretch_allowed(s, op)) {
        return;
    }

    if ((is_synchronizing() ||
         (!s->authenticated && !exited_quorum.is_zero())) &&
        !src_is_mon &&
        m->get_type() != CEPH_MSG_PING) {
        waitlist_or_zap_client(op);
    } else {
        dispatch_op(op);
    }
    return;
}

int Monitor::do_admin_command(
        std::string_view command,
        const cmdmap_t &cmdmap,
        Formatter *f,
        std::ostream &err,
        std::ostream &out) {
    std::lock_guard l(lock);

    int r = 0;
    string args;
    for (auto p = cmdmap.begin();
         p != cmdmap.end(); ++p) {
        if (p->first == "prefix")
            continue;
        if (!args.empty())
            args += ", ";
        args += cmd_vartype_stringify(p->second);
    }
    args = "[" + args + "]";

    bool read_only = (command == "mon_status" ||
                      command == "mon metadata" ||
                      command == "quorum_status" ||
                      command == "ops" ||
                      command == "sessions");

    (read_only ? audit_clog->debug() : audit_clog->info())
            << "from='admin socket' entity='admin socket' "
            << "cmd='" << command << "' args=" << args << ": dispatch";

    if (command == "mon_status") {
        get_mon_status(f);
    } else if (command == "quorum_status") {
        _quorum_status(f, out);
    } else if (command == "sync_force") {
        bool validate = false;
        if (!cmd_getval(cmdmap, "yes_i_really_mean_it", validate)) {
            std::string v;
            if (cmd_getval(cmdmap, "validate", v) &&
                v == "--yes-i-really-mean-it") {
                validate = true;
            }
        }
        if (!validate) {
            err << "are you SURE? this will mean the monitor store will be erased "
                   "the next time the monitor is restarted.  pass "
                   "'--yes-i-really-mean-it' if you really do.";
            r = -EPERM;
            goto abort;
        }
        sync_force(f);
    } else if (command.compare(0, 23, "add_bootstrap_peer_hint") == 0 ||
               command.compare(0, 24, "add_bootstrap_peer_hintv") == 0) {
        if (!_add_bootstrap_peer_hint(command, cmdmap, out))
            goto abort;
    } else if (command == "quorum enter") {
        elector.start_participating();
        start_election();
        out << "started responding to quorum, initiated new election";
    } else if (command == "quorum exit") {
        start_election();
        elector.stop_participating();
        out << "stopped responding to quorum, initiated new election";
    } else if (command == "ops") {
        (void) op_tracker.dump_ops_in_flight(f);
    } else if (command == "sessions") {
        f->open_array_section("sessions");
        for (auto p: session_map.sessions) {
            f->dump_object("session", *p);
        }
        f->close_section();
    } else if (command == "dump_historic_ops") {
        if (!op_tracker.dump_historic_ops(f)) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "dump_historic_ops_by_duration") {
        if (op_tracker.dump_historic_ops(f, true)) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "dump_historic_slow_ops") {
        if (op_tracker.dump_historic_slow_ops(f, {})) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "quorum") {
        string quorumcmd;
        cmd_getval(cmdmap, "quorumcmd", quorumcmd);
        if (quorumcmd == "exit") {
            start_election();
            elector.stop_participating();
            out << "stopped responding to quorum, initiated new election" << std::endl;
        } else if (quorumcmd == "enter") {
            elector.start_participating();
            start_election();
            out << "started responding to quorum, initiated new election" << std::endl;
        } else {
            err << "needs a valid 'quorum' command" << std::endl;
        }
    } else if (command == "connection scores dump") {
        if (!get_quorum_mon_features().contains_all(
                ceph::features::mon::FEATURE_PINGING)) {
            err << "Not all monitors support changing election strategies; "
                   "please upgrade them first!";
        }
        elector.dump_connection_scores(f);
    } else if (command == "connection scores reset") {
        if (!get_quorum_mon_features().contains_all(
                ceph::features::mon::FEATURE_PINGING)) {
            err << "Not all monitors support changing election strategies;"
                   " please upgrade them first!";
        }
        elector.notify_clear_peer_state();
    } else if (command == "smart") {
        string want_devid;
        cmd_getval(cmdmap, "devid", want_devid);

        string devname = store->get_devname();
        if (devname.empty()) {
            err << "could not determine device name for " << store->get_path();
            r = -ENOENT;
            goto abort;
        }
        set<string> devnames;
        get_raw_devices(devname, &devnames);
        json_spirit::mObject json_map;
        uint64_t smart_timeout = cct->_conf.get_val<uint64_t>(
                "mon_smart_report_timeout");
        for (auto &devname: devnames) {
            string err;
            string devid = get_device_id(devname, &err);
            if (want_devid.size() && want_devid != devid) {
                derr << "get_device_id failed on " << devname << ": " << err << dendl;
                continue;
            }
            json_spirit::mValue smart_json;
            if (block_device_get_metrics(devname, smart_timeout,
                                         &smart_json)) {
                dout(10) << "block_device_get_metrics failed for /dev/" << devname
                         << dendl;
                continue;
            }
            json_map[devid] = smart_json;
        }
        json_spirit::write(json_map, out, json_spirit::pretty_print);
    } else if (command == "heap") {
        if (!ceph_using_tcmalloc()) {
            err << "could not issue heap profiler command -- not using tcmalloc!";
            r = -EOPNOTSUPP;
            goto abort;
        }
        string cmd;
        if (!cmd_getval(cmdmap, "heapcmd", cmd)) {
            err << "unable to get value for command \"" << cmd << "\"";
            r = -EINVAL;
            goto abort;
        }
        std::vector<std::string> cmd_vec;
        get_str_vec(cmd, cmd_vec);
        string val;
        if (cmd_getval(cmdmap, "value", val)) {
            cmd_vec.push_back(val);
        }
        ceph_heap_profiler_handle_command(cmd_vec, out);
    } else if (command == "compact") {
        dout(1) << "triggering manual compaction" << dendl;
        auto start = ceph::coarse_mono_clock::now();
        store->compact_async();
        auto end = ceph::coarse_mono_clock::now();
        auto duration = ceph::to_seconds<double>(end - start);
        dout(1) << "finished manual compaction in "
                << duration << " seconds" << dendl;
        out << "compacted " << g_conf().get_val<std::string>("mon_keyvaluedb")
            << " in " << duration << " seconds";
    } else {
        ceph_abort_msg("bad AdminSocket command binding");
    }
    (read_only ? audit_clog->debug() : audit_clog->info())
            << "from='admin socket' "
            << "entity='admin socket' "
            << "cmd=" << command << " "
            << "args=" << args << ": finished";
    return r;

    abort:
    (read_only ? audit_clog->debug() : audit_clog->info())
            << "from='admin socket' "
            << "entity='admin socket' "
            << "cmd=" << command << " "
            << "args=" << args << ": aborted";
    return r;
}


void PaxosMonitor::handle_timecheck_leader(MonOpRequestRef op) {
    auto m = op->get_req<MTimeCheck2>();
    dout(10) << __func__ << " " << *m << dendl;
    /* handles PONG's */
    ceph_assert(m->op == MTimeCheck2::OP_PONG);

    int other = m->get_source().num();
    if (m->epoch < get_epoch()) {
        dout(1) << __func__ << " got old timecheck epoch " << m->epoch
                << " from " << other
                << " curr " << get_epoch()
                << " -- severely lagged? discard" << dendl;
        return;
    }
    ceph_assert(m->epoch == get_epoch());

    if (m->round < timecheck_round) {
        dout(1) << __func__ << " got old round " << m->round
                << " from " << other
                << " curr " << timecheck_round << " -- discard" << dendl;
        return;
    }

    utime_t curr_time = ceph_clock_now();

    ceph_assert(timecheck_waiting.count(other) > 0);
    utime_t timecheck_sent = timecheck_waiting[other];
    timecheck_waiting.erase(other);
    if (curr_time < timecheck_sent) {
        // our clock was readjusted -- drop everything until it all makes sense.
        dout(1) << __func__ << " our clock was readjusted --"
                << " bump round and drop current check"
                << dendl;
        timecheck_cancel_round();
        return;
    }

    /* update peer latencies */
    double latency = (double) (curr_time - timecheck_sent);

    if (timecheck_latencies.count(other) == 0)
        timecheck_latencies[other] = latency;
    else {
        double avg_latency = ((timecheck_latencies[other] * 0.8) + (latency * 0.2));
        timecheck_latencies[other] = avg_latency;
    }

    /*
     * update skews
     *
     * some nasty thing goes on if we were to do 'a - b' between two utime_t,
     * and 'a' happens to be lower than 'b'; so we use double instead.
     *
     * latency is always expected to be >= 0.
     *
     * delta, the difference between theirs timestamp and ours, may either be
     * lower or higher than 0; will hardly ever be 0.
     *
     * The absolute skew is the absolute delta minus the latency, which is
     * taken as a whole instead of an rtt given that there is some queueing
     * and dispatch times involved and it's hard to assess how long exactly
     * it took for the message to travel to the other side and be handled. So
     * we call it a bounded skew, the worst case scenario.
     *
     * Now, to math!
     *
     * Given that the latency is always positive, we can establish that the
     * bounded skew will be:
     *
     *  1. positive if the absolute delta is higher than the latency and
     *     delta is positive
     *  2. negative if the absolute delta is higher than the latency and
     *     delta is negative.
     *  3. zero if the absolute delta is lower than the latency.
     *
     * On 3. we make a judgement call and treat the skew as non-existent.
     * This is because that, if the absolute delta is lower than the
     * latency, then the apparently existing skew is nothing more than a
     * side-effect of the high latency at work.
     *
     * This may not be entirely true though, as a severely skewed clock
     * may be masked by an even higher latency, but with high latencies
     * we probably have worse issues to deal with than just skewed clocks.
     */
    ceph_assert(latency >= 0);

    double delta = ((double) m->timestamp) - ((double) curr_time);
    double abs_delta = (delta > 0 ? delta : -delta);
    double skew_bound = abs_delta - latency;
    if (skew_bound < 0)
        skew_bound = 0;
    else if (delta < 0)
        skew_bound = -skew_bound;

    ostringstream ss;
    health_status_t status = timecheck_status(ss, skew_bound, latency);
    if (status != HEALTH_OK) {
        clog->health(status) << other << " " << ss.str();
    }

    dout(10) << __func__ << " from " << other << " ts " << m->timestamp
             << " delta " << delta << " skew_bound " << skew_bound
             << " latency " << latency << dendl;

    timecheck_skews[other] = skew_bound;

    timecheck_acks++;
    if (timecheck_acks == quorum.size()) {
        dout(10) << __func__ << " got pongs from everybody ("
                 << timecheck_acks << " total)" << dendl;
        ceph_assert(timecheck_skews.size() == timecheck_acks);
        ceph_assert(timecheck_waiting.empty());
        // everyone has acked, so bump the round to finish it.
        timecheck_finish_round();
    }
}

void PaxosMonitor::handle_timecheck_peon(MonOpRequestRef op) {
    auto m = op->get_req<MTimeCheck2>();
    dout(10) << __func__ << " " << *m << dendl;

    ceph_assert(is_peon());
    ceph_assert(m->op == MTimeCheck2::OP_PING || m->op == MTimeCheck2::OP_REPORT);

    if (m->epoch != get_epoch()) {
        dout(1) << __func__ << " got wrong epoch "
                << "(ours " << get_epoch()
                << " theirs: " << m->epoch << ") -- discarding" << dendl;
        return;
    }

    if (m->round < timecheck_round) {
        dout(1) << __func__ << " got old round " << m->round
                << " current " << timecheck_round
                << " (epoch " << get_epoch() << ") -- discarding" << dendl;
        return;
    }

    timecheck_round = m->round;

    if (m->op == MTimeCheck2::OP_REPORT) {
        ceph_assert((timecheck_round % 2) == 0);
        timecheck_latencies.swap(m->latencies);
        timecheck_skews.swap(m->skews);
        return;
    }

    ceph_assert((timecheck_round % 2) != 0);
    MTimeCheck2 *reply = new MTimeCheck2(MTimeCheck2::OP_PONG);
    utime_t curr_time = ceph_clock_now();
    reply->timestamp = curr_time;
    reply->epoch = m->epoch;
    reply->round = m->round;
    dout(10) << __func__ << " send " << *m
             << " to " << m->get_source_inst() << dendl;
    m->get_connection()->send_message(reply);
}

void PaxosMonitor::handle_timecheck(MonOpRequestRef op) {
    auto m = op->get_req<MTimeCheck2>();
    dout(10) << __func__ << " " << *m << dendl;

    if (is_leader()) {
        if (m->op != MTimeCheck2::OP_PONG) {
            dout(1) << __func__ << " drop unexpected msg (not pong)" << dendl;
        } else {
            handle_timecheck_leader(op);
        }
    } else if (is_peon()) {
        if (m->op != MTimeCheck2::OP_PING && m->op != MTimeCheck2::OP_REPORT) {
            dout(1) << __func__ << " drop unexpected msg (not ping or report)" << dendl;
        } else {
            handle_timecheck_peon(op);
        }
    } else {
        dout(1) << __func__ << " drop unexpected msg" << dendl;
    }
}

void PaxosMonitor::handle_get_version(MonOpRequestRef op) {
    auto m = op->get_req<MMonGetVersion>();
    dout(10) << "handle_get_version " << *m << dendl;
    PaxosService *svc = NULL;

    MonSession *s = op->get_session();
    ceph_assert(s);

    if (!is_leader() && !is_peon()) {
        dout(10) << " waiting for quorum" << dendl;
        waitfor_quorum.push_back(new C_RetryMessage(this, op));
        goto out;
    }

    if (m->what == "mdsmap") {
        svc = mdsmon();
    } else if (m->what == "fsmap") {
        svc = mdsmon();
    } else if (m->what == "osdmap") {
        svc = osdmon();
    } else if (m->what == "monmap") {
        svc = monmon();
    } else {
        derr << "invalid map type " << m->what << dendl;
    }

    if (svc) {
        if (!svc->is_readable()) {
            svc->wait_for_readable(op, new C_RetryMessage(this, op));
            goto out;
        }

        MMonGetVersionReply *reply = new MMonGetVersionReply();
        reply->handle = m->handle;
        reply->version = svc->get_last_committed();
        reply->oldest_version = svc->get_first_committed();
        reply->set_tid(m->get_tid());

        m->get_connection()->send_message(reply);
    }
    out:
    return;
}

bool Monitor::_add_bootstrap_peer_hint(std::string_view cmd,
                                       const cmdmap_t &cmdmap,
                                       ostream &ss) {
    if (is_leader() || is_peon()) {
        ss << "mon already active; ignoring bootstrap hint";
        return true;
    }

    entity_addrvec_t addrs;
    string addrstr;
    if (cmd_getval(cmdmap, "addr", addrstr)) {
        dout(10) << "_add_bootstrap_peer_hint '" << cmd << "' addr '"
                 << addrstr << "'" << dendl;

        entity_addr_t addr;
        if (!addr.parse(addrstr, entity_addr_t::TYPE_ANY)) {
            ss << "failed to parse addrs '" << addrstr
               << "'; syntax is 'add_bootstrap_peer_hint ip[:port]'";
            return false;
        }

        addrs.v.push_back(addr);
        if (addr.get_port() == 0) {
            addrs.v[0].set_type(entity_addr_t::TYPE_MSGR2);
            addrs.v[0].set_port(CEPH_MON_PORT_IANA);
            addrs.v.push_back(addr);
            addrs.v[1].set_type(entity_addr_t::TYPE_LEGACY);
            addrs.v[1].set_port(CEPH_MON_PORT_LEGACY);
        } else if (addr.get_type() == entity_addr_t::TYPE_ANY) {
            if (addr.get_port() == CEPH_MON_PORT_LEGACY) {
                addrs.v[0].set_type(entity_addr_t::TYPE_LEGACY);
            } else {
                addrs.v[0].set_type(entity_addr_t::TYPE_MSGR2);
            }
        }
    } else if (cmd_getval(cmdmap, "addrv", addrstr)) {
        dout(10) << "_add_bootstrap_peer_hintv '" << cmd << "' addrv '"
                 << addrstr << "'" << dendl;
        const char *end = 0;
        if (!addrs.parse(addrstr.c_str(), &end)) {
            ss << "failed to parse addrs '" << addrstr
               << "'; syntax is 'add_bootstrap_peer_hintv v2:ip:port[,v1:ip:port]'";
            return false;
        }
    } else {
        ss << "no addr or addrv provided";
        return false;
    }

    extra_probe_peers.insert(addrs);
    ss << "adding peer " << addrs << " to list: " << extra_probe_peers;
    return true;
}

PaxosMonitor::PaxosMonitor(CephContext *cct_, MonitorDBStore *store, std::string nm, Messenger *m, Messenger *mgr_m,
                           MonMap *map)
        : AbstractMonitor(cct_, store, nm, m, mgr_m, map),
          elector(this, map->strategy),
          required_features(0),
          leader(0),
        // sync state
          sync_provider_count(0),
          sync_cookie(0),
          sync_full(false),
          sync_start_version(0),
          sync_timeout_event(NULL),
          sync_last_committed_floor(0),
        // scrub
          scrub_version(0),
          scrub_event(NULL),
          scrub_timeout_event(NULL),
{

}