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

void PaxosMonitor::_quorum_status(Formatter *f, ostream &ss) {
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

void PaxosMonitor::get_mon_status(Formatter *f) {
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

const utime_t& PaxosMonitor::get_leader_since() const
{
    ceph_assert(state == STATE_LEADER);
    return leader_since;
}

void PaxosMonitor::prepare_new_fingerprint(MonitorDBStore::TransactionRef t)
{
    uuid_d nf;
    nf.generate_random();
    dout(10) << __func__ << " proposing cluster_fingerprint " << nf << dendl;

    bufferlist bl;
    encode(nf, bl);
    t->put(MONITOR_NAME, "cluster_fingerprint", bl);
}

struct CMonEnableStretchMode : public Context {
    PaxosMonitor *m;
    CMonEnableStretchMode(PaxosMonitor *mon) : m(mon) {}
    void finish(int r) {
        m->try_engage_stretch_mode();
    }
};

void PaxosMonitor::do_stretch_mode_election_work()
{
    dout(20) << __func__ << dendl;
    if (!is_stretch_mode() ||
        !is_leader()) return;
    dout(20) << "checking for degraded stretch mode" << dendl;
    map<string, set<string>> old_dead_buckets;
    old_dead_buckets.swap(dead_mon_buckets);
    up_mon_buckets.clear();
    // identify if we've lost a CRUSH bucket, request OSDMonitor check for death
    map<string,set<string>> down_mon_buckets;
    for (unsigned i = 0; i < monmap->size(); ++i) {
        const auto &mi = monmap->mon_info[monmap->get_name(i)];
        auto ci = mi.crush_loc.find(stretch_bucket_divider);
        ceph_assert(ci != mi.crush_loc.end());
        if (quorum.count(i)) {
            up_mon_buckets.insert(ci->second);
        } else {
            down_mon_buckets[ci->second].insert(mi.name);
        }
    }
    dout(20) << "prior dead_mon_buckets: " << old_dead_buckets
             << "; down_mon_buckets: " << down_mon_buckets
             << "; up_mon_buckets: " << up_mon_buckets << dendl;
    for (const auto& di : down_mon_buckets) {
        if (!up_mon_buckets.count(di.first)) {
            dead_mon_buckets[di.first] = di.second;
        }
    }
    dout(20) << "new dead_mon_buckets " << dead_mon_buckets << dendl;

    if (dead_mon_buckets != old_dead_buckets &&
        dead_mon_buckets.size() >= old_dead_buckets.size()) {
        maybe_go_degraded_stretch_mode();
    }
}

struct CMonGoDegraded : public Context {
    PaxosMonitor *m;
    CMonGoDegraded(PaxosMonitor *mon) : m(mon) {}
    void finish(int r) {
        m->maybe_go_degraded_stretch_mode();
    }
};

struct CMonGoRecovery : public Context {
    PaxosMonitor *m;
    CMonGoRecovery(PaxosMonitor *mon) : m(mon) {}
    void finish(int r) {
        m->go_recovery_stretch_mode();
    }
};

void PaxosMonitor::go_recovery_stretch_mode()
{
    dout(20) << __func__ << dendl;
    if (!is_leader()) return;
    if (!is_degraded_stretch_mode()) return;
    if (is_recovering_stretch_mode()) return;

    if (dead_mon_buckets.size()) {
        ceph_assert( 0 == "how did we try and do stretch recovery while we have dead monitor buckets?");
        // we can't recover if we are missing monitors in a zone!
        return;
    }

    if (!osdmon()->is_readable()) {
        osdmon()->wait_for_readable_ctx(new CMonGoRecovery(this));
        return;
    }

    if (!osdmon()->is_writeable()) {
        osdmon()->wait_for_writeable_ctx(new CMonGoRecovery(this));
    }
    osdmon()->trigger_recovery_stretch_mode();
}

void PaxosMonitor::set_recovery_stretch_mode()
{
    degraded_stretch_mode = true;
    recovering_stretch_mode = true;
    osdmon()->set_recovery_stretch_mode();
}

void PaxosMonitor::try_engage_stretch_mode()
{
    dout(20) << __func__ << dendl;
    if (stretch_mode_engaged) return;
    if (!osdmon()->is_readable()) {
        osdmon()->wait_for_readable_ctx(new CMonEnableStretchMode(this));
    }
    if (osdmon()->osdmap.stretch_mode_enabled &&
        monmap->stretch_mode_enabled) {
        dout(10) << "Engaging stretch mode!" << dendl;
        stretch_mode_engaged = true;
        int32_t stretch_divider_id = osdmon()->osdmap.stretch_mode_bucket;
        stretch_bucket_divider = osdmon()->osdmap.
                crush->get_type_name(stretch_divider_id);
        disconnect_disallowed_stretch_sessions();
    }
}

void PaxosMonitor::maybe_go_degraded_stretch_mode()
{
    dout(20) << __func__ << dendl;
    if (is_degraded_stretch_mode()) return;
    if (!is_leader()) return;
    if (dead_mon_buckets.empty()) return;
    if (!osdmon()->is_readable()) {
        osdmon()->wait_for_readable_ctx(new CMonGoDegraded(this));
        return;
    }
    ceph_assert(monmap->contains(monmap->tiebreaker_mon));
    // filter out the tiebreaker zone and check if remaining sites are down by OSDs too
    const auto &mi = monmap->mon_info[monmap->tiebreaker_mon];
    auto ci = mi.crush_loc.find(stretch_bucket_divider);
    map<string, set<string>> filtered_dead_buckets = dead_mon_buckets;
    filtered_dead_buckets.erase(ci->second);

    set<int> matched_down_buckets;
    set<string> matched_down_mons;
    bool dead = osdmon()->check_for_dead_crush_zones(filtered_dead_buckets,
                                                     &matched_down_buckets,
                                                     &matched_down_mons);
    if (dead) {
        if (!osdmon()->is_writeable()) {
            osdmon()->wait_for_writeable_ctx(new CMonGoDegraded(this));
        }
        if (!monmon()->is_writeable()) {
            monmon()->wait_for_writeable_ctx(new CMonGoDegraded(this));
        }
        trigger_degraded_stretch_mode(matched_down_mons, matched_down_buckets);
    }
}

void PaxosMonitor::trigger_degraded_stretch_mode(const set<string>& dead_mons,
                                            const set<int>& dead_buckets)
{
    dout(20) << __func__ << dendl;
    ceph_assert(osdmon()->is_writeable());
    ceph_assert(monmon()->is_writeable());

    // figure out which OSD zone(s) remains alive by removing
    // tiebreaker mon from up_mon_buckets
    set<string> live_zones = up_mon_buckets;
    ceph_assert(monmap->contains(monmap->tiebreaker_mon));
    const auto &mi = monmap->mon_info[monmap->tiebreaker_mon];
    auto ci = mi.crush_loc.find(stretch_bucket_divider);
    live_zones.erase(ci->second);
    ceph_assert(live_zones.size() == 1); // only support 2 zones right now

    osdmon()->trigger_degraded_stretch_mode(dead_buckets, live_zones);
    monmon()->trigger_degraded_stretch_mode(dead_mons);
    set_degraded_stretch_mode();
}

void PaxosMonitor::set_degraded_stretch_mode()
{
    degraded_stretch_mode = true;
    recovering_stretch_mode = false;
    osdmon()->set_degraded_stretch_mode();
}

struct CMonGoHealthy : public Context {
    PaxosMonitor *m;
    CMonGoHealthy(PaxosMonitor *mon) : m(mon) {}
    void finish(int r) {
        m->trigger_healthy_stretch_mode();
    }
};


void PaxosMonitor::trigger_healthy_stretch_mode()
{
    dout(20) << __func__ << dendl;
    if (!is_degraded_stretch_mode()) return;
    if (!is_leader()) return;
    if (!osdmon()->is_writeable()) {
        osdmon()->wait_for_writeable_ctx(new CMonGoHealthy(this));
    }
    if (!monmon()->is_writeable()) {
        monmon()->wait_for_writeable_ctx(new CMonGoHealthy(this));
    }

    ceph_assert(osdmon()->osdmap.recovering_stretch_mode);
    osdmon()->trigger_healthy_stretch_mode();
    monmon()->trigger_healthy_stretch_mode();
}

void PaxosMonitor::set_healthy_stretch_mode()
{
    degraded_stretch_mode = false;
    recovering_stretch_mode = false;
    osdmon()->set_healthy_stretch_mode();
}

bool PaxosMonitor::session_stretch_allowed(MonSession *s, MonOpRequestRef& op)
{
    if (!is_stretch_mode()) return true;
    if (s->proxy_con) return true;
    if (s->validated_stretch_connection) return true;
    if (!s->con) return true;
    if (s->con->peer_is_osd()) {
        dout(20) << __func__ << "checking OSD session" << s << dendl;
        // okay, check the crush location
        int barrier_id = [&] {
            auto type_id = osdmon()->osdmap.crush->get_validated_type_id(
                    stretch_bucket_divider);
            ceph_assert(type_id.has_value());
            return *type_id;
        }();
        int osd_bucket_id = osdmon()->osdmap.crush->get_parent_of_type(s->con->peer_id,
                                                                       barrier_id);
        const auto &mi = monmap->mon_info.find(name);
        ceph_assert(mi != monmap->mon_info.end());
        auto ci = mi->second.crush_loc.find(stretch_bucket_divider);
        ceph_assert(ci != mi->second.crush_loc.end());
        int mon_bucket_id = osdmon()->osdmap.crush->get_item_id(ci->second);

        if (osd_bucket_id != mon_bucket_id) {
            dout(5) << "discarding session " << *s
                    << " and sending OSD to matched zone" << dendl;
            s->con->mark_down();
            std::lock_guard l(session_map_lock);
            remove_session(s);
            if (op) {
                op->mark_zap();
            }
            return false;
        }
    }

    s->validated_stretch_connection = true;
    return true;
}

void PaxosMonitor::disconnect_disallowed_stretch_sessions()
{
    dout(20) << __func__ << dendl;
    MonOpRequestRef blank;
    auto i = session_map.sessions.begin();
    while (i != session_map.sessions.end()) {
        auto j = i;
        ++i;
        session_stretch_allowed(*j, blank);
    }
}

void PaxosMonitor::set_elector_disallowed_leaders(bool allow_election)
{
    set<int> dl;
    for (auto name : monmap->disallowed_leaders) {
        dl.insert(monmap->get_rank(name));
    }
    if (is_stretch_mode()) {
        for (auto name : monmap->stretch_marked_down_mons) {
            dl.insert(monmap->get_rank(name));
        }
        dl.insert(monmap->get_rank(monmap->tiebreaker_mon));
    }

    bool disallowed_changed = elector.set_disallowed_leaders(dl);
    if (disallowed_changed && allow_election) {
        elector.call_election();
    }
}

// called by bootstrap(), or on leader|peon -> electing
void PaxosMonitor::_reset()
{
    dout(10) << __func__ << dendl;

    // disable authentication
    {
        std::lock_guard l(auth_lock);
        authmon()->_set_mon_num_rank(0, 0);
    }

    cancel_probe_timeout();
    timecheck_finish();
    health_events_cleanup();
    health_check_log_times.clear();
    scrub_event_cancel();

    leader_since = utime_t();
    quorum_since = {};
    if (!quorum.empty()) {
        exited_quorum = ceph_clock_now();
    }
    quorum.clear();
    outside_quorum.clear();
    quorum_feature_map.clear();

    scrub_reset();

    paxos->restart();

    for (auto& svc : services) {
        svc->restart();
    }
}

void PaxosMonitor::wait_for_paxos_write()
{
    if (paxos->is_writing() || paxos->is_writing_previous()) {
        dout(10) << __func__ << " flushing pending write" << dendl;
        lock.unlock();
        store->flush();
        lock.lock();
        dout(10) << __func__ << " flushed pending write" << dendl;
    }
}

void PaxosMonitor::_finish_svc_election()
{
    ceph_assert(state == STATE_LEADER || state == STATE_PEON);

    for (auto& svc : services) {
        // we already called election_finished() on monmon(); avoid callig twice
        if (state == STATE_LEADER && svc.get() == monmon())
            continue;
        svc->election_finished();
    }
}

void PaxosMonitor::respawn()
{
    // --- WARNING TO FUTURE COPY/PASTERS ---
    // You must also add a call like
    //
    //   ceph_pthread_setname(pthread_self(), "ceph-mon");
    //
    // to main() so that /proc/$pid/stat field 2 contains "(ceph-mon)"
    // instead of "(exe)", so that killall (and log rotation) will work.

    dout(0) << __func__ << dendl;

    char *new_argv[orig_argc+1];
    dout(1) << " e: '" << orig_argv[0] << "'" << dendl;
    for (int i=0; i<orig_argc; i++) {
        new_argv[i] = (char *)orig_argv[i];
        dout(1) << " " << i << ": '" << orig_argv[i] << "'" << dendl;
    }
    new_argv[orig_argc] = NULL;

    /* Determine the path to our executable, test if Linux /proc/self/exe exists.
     * This allows us to exec the same executable even if it has since been
     * unlinked.
     */
    char exe_path[PATH_MAX] = "";
#ifdef PROCPREFIX
    if (readlink(PROCPREFIX "/proc/self/exe", exe_path, PATH_MAX-1) != -1) {
        dout(1) << "respawning with exe " << exe_path << dendl;
        strcpy(exe_path, PROCPREFIX "/proc/self/exe");
    } else {
#else
        {
#endif
        /* Print CWD for the user's interest */
        char buf[PATH_MAX];
        char *cwd = getcwd(buf, sizeof(buf));
        ceph_assert(cwd);
        dout(1) << " cwd " << cwd << dendl;

        /* Fall back to a best-effort: just running in our CWD */
        strncpy(exe_path, orig_argv[0], PATH_MAX-1);
    }

    dout(1) << " exe_path " << exe_path << dendl;

    unblock_all_signals(NULL);
    execv(exe_path, new_argv);

    dout(0) << "respawn execv " << orig_argv[0]
            << " failed with " << cpp_strerror(errno) << dendl;

    // We have to assert out here, because suicide() returns, and callers
    // to respawn expect it never to return.
    ceph_abort();
}

void PaxosMonitor::bootstrap()
{
    dout(10) << "bootstrap" << dendl;
    wait_for_paxos_write();

    sync_reset_requester();
    unregister_cluster_logger();
    cancel_probe_timeout();

    if (monmap->get_epoch() == 0) {
        dout(10) << "reverting to legacy ranks for seed monmap (epoch 0)" << dendl;
        monmap->calc_legacy_ranks();
    }
    dout(10) << "monmap " << *monmap << dendl;
    {
        auto from_release = monmap->min_mon_release;
        ostringstream err;
        if (!can_upgrade_from(from_release, "min_mon_release", err)) {
            derr << "current monmap has " << err.str() << " stopping." << dendl;
            exit(0);
        }
    }
    // note my rank
    int newrank = monmap->get_rank(messenger->get_myaddrs());
    if (newrank < 0 && rank >= 0) {
        // was i ever part of the quorum?
        if (has_ever_joined) {
            dout(0) << " removed from monmap, suicide." << dendl;
            exit(0);
        }
        elector.notify_clear_peer_state();
    }
    if (newrank >= 0 &&
        monmap->get_addrs(newrank) != messenger->get_myaddrs()) {
        dout(0) << " monmap addrs for rank " << newrank << " changed, i am "
                << messenger->get_myaddrs()
                << ", monmap is " << monmap->get_addrs(newrank) << ", respawning"
                << dendl;

        if (monmap->get_epoch()) {
            // store this map in temp mon_sync location so that we use it on
            // our next startup
            derr << " stashing newest monmap " << monmap->get_epoch()
                 << " for next startup" << dendl;
            bufferlist bl;
            monmap->encode(bl, -1);
            auto t(std::make_shared<MonitorDBStore::Transaction>());
            t->put("mon_sync", "temp_newer_monmap", bl);
            store->apply_transaction(t);
        }

        respawn();
    }
    if (newrank != rank) {
        dout(0) << " my rank is now " << newrank << " (was " << rank << ")" << dendl;
        messenger->set_myname(entity_name_t::MON(newrank));
        rank = newrank;
        elector.notify_rank_changed(rank);

        // reset all connections, or else our peers will think we are someone else.
        messenger->mark_down_all();
    }

    // reset
    state = STATE_PROBING;

    _reset();

    // sync store
    if (g_conf()->mon_compact_on_bootstrap) {
        dout(10) << "bootstrap -- triggering compaction" << dendl;
        store->compact();
        dout(10) << "bootstrap -- finished compaction" << dendl;
    }

    // stretch mode bits
    set_elector_disallowed_leaders(false);

    // singleton monitor?
    if (monmap->size() == 1 && rank == 0) {
        win_standalone_election();
        return;
    }

    reset_probe_timeout();

    // i'm outside the quorum
    if (monmap->contains(name))
        outside_quorum.insert(name);

    // probe monitors
    dout(10) << "probing other monitors" << dendl;
    for (unsigned i = 0; i < monmap->size(); i++) {
        if ((int)i != rank)
            send_mon_message(
                    new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined,
                                  ceph_release()),
                    i);
    }
    for (auto& av : extra_probe_peers) {
        if (av != messenger->get_myaddrs()) {
            messenger->send_to_mon(
                    new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined,
                                  ceph_release()),
                    av);
        }
    }
}

void PaxosMonitor::join_election()
{
    dout(10) << __func__ << dendl;
    wait_for_paxos_write();
    _reset();
    state = STATE_ELECTING;

    logger->inc(l_mon_num_elections);
}

void PaxosMonitor::start_election()
{
    dout(10) << "start_election" << dendl;
    wait_for_paxos_write();
    _reset();
    state = STATE_ELECTING;

    logger->inc(l_mon_num_elections);
    logger->inc(l_mon_election_call);

    clog->info() << "mon." << name << " calling monitor election";
    elector.call_election();
}

void PaxosMonitor::win_standalone_election()
{
    dout(1) << "win_standalone_election" << dendl;

    // bump election epoch, in case the previous epoch included other
    // monitors; we need to be able to make the distinction.
    elector.declare_standalone_victory();

    rank = monmap->get_rank(name);
    ceph_assert(rank == 0);
    set<int> q;
    q.insert(rank);

    map<int,Metadata> metadata;
    collect_metadata(&metadata[0]);

    win_election(elector.get_epoch(), q,
                 CEPH_FEATURES_ALL,
                 ceph::features::mon::get_supported(),
                 ceph_release(),
                 metadata);
}


void PaxosMonitor::win_election(epoch_t epoch, const set<int>& active, uint64_t features,
                           const mon_feature_t& mon_features,
                           ceph_release_t min_mon_release,
                           const map<int,Metadata>& metadata) {
    dout(10) << __func__ << " epoch " << epoch << " quorum " << active
             << " features " << features
             << " mon_features " << mon_features
             << " min_mon_release " << min_mon_release
             << dendl;
    ceph_assert(is_electing());
    state = STATE_LEADER;
    leader_since = ceph_clock_now();
    quorum_since = mono_clock::now();
    leader = rank;
    quorum = active;
    quorum_con_features = features;
    quorum_mon_features = mon_features;
    quorum_min_mon_release = min_mon_release;
    pending_metadata = metadata;
    outside_quorum.clear();

    clog->info() << "mon." << name << " is new leader, mons " << get_quorum_names()
                 << " in quorum (ranks " << quorum << ")";

    set_leader_commands(get_local_commands(mon_features));

    paxos->leader_init();
    // NOTE: tell monmap monitor first.  This is important for the
    // bootstrap case to ensure that the very first paxos proposal
    // codifies the monmap.  Otherwise any manner of chaos can ensue
    // when monitors are call elections or participating in a paxos
    // round without agreeing on who the participants are.
    monmon()->election_finished();
    _finish_svc_election();

    logger->inc(l_mon_election_win);

    // inject new metadata in first transaction.
    {
        // include previous metadata for missing mons (that aren't part of
        // the current quorum).
        map<int, Metadata> m = metadata;
        for (unsigned rank = 0; rank < monmap->size(); ++rank) {
            if (m.count(rank) == 0 &&
                mon_metadata.count(rank)) {
                m[rank] = mon_metadata[rank];
            }
        }

        // FIXME: This is a bit sloppy because we aren't guaranteed to submit
        // a new transaction immediately after the election finishes.  We should
        // do that anyway for other reasons, though.
        MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
        bufferlist bl;
        encode(m, bl);
        t->put(MONITOR_STORE_PREFIX, "last_metadata", bl);
    }

    finish_election();
    if (monmap->size() > 1 &&
        monmap->get_epoch() > 0) {
        timecheck_start();
        health_tick_start();

        // Freshen the health status before doing health_to_clog in case
        // our just-completed election changed the health
        healthmon()->wait_for_active_ctx(new LambdaContext([this](int r) {
            dout(20) << "healthmon now active" << dendl;
            healthmon()->tick();
            if (healthmon()->is_proposing()) {
                dout(20) << __func__ << " healthmon proposing, waiting" << dendl;
                healthmon()->wait_for_finished_proposal(nullptr, new C_MonContext{this,
                                                                                  [this](int r) {
                                                                                      ceph_assert(
                                                                                              ceph_mutex_is_locked_by_me(
                                                                                                      lock));
                                                                                      do_health_to_clog_interval();
                                                                                  }});

            } else {
                do_health_to_clog_interval();
            }
        }));

        scrub_event_start();
    }
}

void PaxosMonitor::lose_election(epoch_t epoch, set<int> &q, int l,
                            uint64_t features,
                            const mon_feature_t& mon_features,
                            ceph_release_t min_mon_release)
{
    state = STATE_PEON;
    leader_since = utime_t();
    quorum_since = mono_clock::now();
    leader = l;
    quorum = q;
    outside_quorum.clear();
    quorum_con_features = features;
    quorum_mon_features = mon_features;
    quorum_min_mon_release = min_mon_release;
    dout(10) << "lose_election, epoch " << epoch << " leader is mon" << leader
             << " quorum is " << quorum << " features are " << quorum_con_features
             << " mon_features are " << quorum_mon_features
             << " min_mon_release " << min_mon_release
             << dendl;

    paxos->peon_init();
    _finish_svc_election();

    logger->inc(l_mon_election_lose);

    finish_election();
}

void PaxosMonitor::finish_election()
{
    apply_quorum_to_compatset_features();
    apply_monmap_to_compatset_features();
    timecheck_finish();
    exited_quorum = utime_t();
    finish_contexts(g_ceph_context, waitfor_quorum);
    finish_contexts(g_ceph_context, maybe_wait_for_quorum);
    resend_routed_requests();
    update_logger();
    register_cluster_logger();

    // enable authentication
    {
        std::lock_guard l(auth_lock);
        authmon()->_set_mon_num_rank(monmap->size(), rank);
    }

    // am i named and located properly?
    string cur_name = monmap->get_name(messenger->get_myaddrs());
    const auto my_infop = monmap->mon_info.find(cur_name);
    const map<string,string>& map_crush_loc = my_infop->second.crush_loc;

    if (cur_name != name ||
        (need_set_crush_loc && map_crush_loc != crush_loc)) {
        dout(10) << " renaming/moving myself from " << cur_name << "/"
                 << map_crush_loc <<" -> " << name << "/" << crush_loc << dendl;
        send_mon_message(new MMonJoin(monmap->fsid, name, messenger->get_myaddrs(),
                                      crush_loc, need_set_crush_loc),
                         leader);
        return;
    }
    do_stretch_mode_election_work();
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

void PaxosMonitor::_dispatch_op(MonOpRequestRef op) {
    /* messages that should only be sent by another monitor */
    switch (op->get_req()->get_type()) {

        case MSG_ROUTE:
            handle_route(op);
            return;

        case MSG_MON_PROBE:
            handle_probe(op);
            return;

            // Sync (i.e., the new slurp, but on steroids)
        case MSG_MON_SYNC:
            handle_sync(op);
            return;
        case MSG_MON_SCRUB:
            handle_scrub(op);
            return;

            /* log acks are sent from a monitor we sent the MLog to, and are
               never sent by clients to us. */
        case MSG_LOGACK:
            log_client.handle_log_ack((MLogAck*)op->get_req());
            return;

            // monmap
        case MSG_MON_JOIN:
            op->set_type_service();
            services[PAXOS_MONMAP]->dispatch(op);
            return;

            // paxos
        case MSG_MON_PAXOS:
        {
            op->set_type_paxos();
            auto pm = op->get_req<MMonPaxos>();
            if (!op->get_session()->is_capable("mon", MON_CAP_X)) {
                //can't send these!
                return;
            }

            if (state == STATE_SYNCHRONIZING) {
                // we are synchronizing. These messages would do us no
                // good, thus just drop them and ignore them.
                dout(10) << __func__ << " ignore paxos msg from "
                         << pm->get_source_inst() << dendl;
                return;
            }

            // sanitize
            if (pm->epoch > get_epoch()) {
                bootstrap();
                return;
            }
            if (pm->epoch != get_epoch()) {
                return;
            }

            paxos->dispatch(op);
        }
            return;

            // elector messages
        case MSG_MON_ELECTION:
            op->set_type_election_or_ping();
            //check privileges here for simplicity
            if (!op->get_session()->is_capable("mon", MON_CAP_X)) {
                dout(0) << "MMonElection received from entity without enough caps!"
                        << op->get_session()->caps << dendl;
                return;;
            }
            if (!is_probing() && !is_synchronizing()) {
                elector.dispatch(op);
            }
            return;

        case MSG_MON_PING:
            op->set_type_election_or_ping();
            elector.dispatch(op);
            return;

        case MSG_FORWARD:
            handle_forward(op);
            return;

        case MSG_TIMECHECK:
            dout(5) << __func__ << " ignoring " << op << dendl;
            return;
        case MSG_TIMECHECK2:
            handle_timecheck(op);
            return;

        case MSG_MON_HEALTH:
            dout(5) << __func__ << " dropping deprecated message: "
                    << *op->get_req() << dendl;
            break;
        case MSG_MON_HEALTH_CHECKS:
            op->set_type_service();
            services[PAXOS_HEALTH]->dispatch(op);
            return;
    }
    dout(1) << "dropping unexpected " << *(op->get_req()) << dendl;
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


void PaxosMonitor::timecheck_start()
{
    dout(10) << __func__ << dendl;
    timecheck_cleanup();
    if (get_quorum_mon_features().contains_all(
            ceph::features::mon::FEATURE_NAUTILUS)) {
        timecheck_start_round();
    }
}

void PaxosMonitor::timecheck_finish()
{
    dout(10) << __func__ << dendl;
    timecheck_cleanup();
}

void PaxosMonitor::timecheck_start_round()
{
    dout(10) << __func__ << " curr " << timecheck_round << dendl;
    ceph_assert(is_leader());

    if (monmap->size() == 1) {
        ceph_abort_msg("We are alone; this shouldn't have been scheduled!");
        return;
    }

    if (timecheck_round % 2) {
        dout(10) << __func__ << " there's a timecheck going on" << dendl;
        utime_t curr_time = ceph_clock_now();
        double max = g_conf()->mon_timecheck_interval*3;
        if (curr_time - timecheck_round_start < max) {
            dout(10) << __func__ << " keep current round going" << dendl;
            goto out;
        } else {
            dout(10) << __func__
                     << " finish current timecheck and start new" << dendl;
            timecheck_cancel_round();
        }
    }

    ceph_assert(timecheck_round % 2 == 0);
    timecheck_acks = 0;
    timecheck_round ++;
    timecheck_round_start = ceph_clock_now();
    dout(10) << __func__ << " new " << timecheck_round << dendl;

    timecheck();
    out:
    dout(10) << __func__ << " setting up next event" << dendl;
    timecheck_reset_event();
}

void PaxosMonitor::timecheck_finish_round(bool success)
{
    dout(10) << __func__ << " curr " << timecheck_round << dendl;
    ceph_assert(timecheck_round % 2);
    timecheck_round ++;
    timecheck_round_start = utime_t();

    if (success) {
        ceph_assert(timecheck_waiting.empty());
        ceph_assert(timecheck_acks == quorum.size());
        timecheck_report();
        timecheck_check_skews();
        return;
    }

    dout(10) << __func__ << " " << timecheck_waiting.size()
             << " peers still waiting:";
            for (auto& p : timecheck_waiting) {
                *_dout << " mon." << p.first;
            }
            *_dout << dendl;
    timecheck_waiting.clear();

    dout(10) << __func__ << " finished to " << timecheck_round << dendl;
}

void PaxosMonitor::timecheck_cancel_round()
{
    timecheck_finish_round(false);
}

void PaxosMonitor::timecheck_cleanup()
{
    timecheck_round = 0;
    timecheck_acks = 0;
    timecheck_round_start = utime_t();

    if (timecheck_event) {
        timer.cancel_event(timecheck_event);
        timecheck_event = NULL;
    }
    timecheck_waiting.clear();
    timecheck_skews.clear();
    timecheck_latencies.clear();

    timecheck_rounds_since_clean = 0;
}

void PaxosMonitor::timecheck_reset_event()
{
    if (timecheck_event) {
        timer.cancel_event(timecheck_event);
        timecheck_event = NULL;
    }

    double delay =
            cct->_conf->mon_timecheck_skew_interval * timecheck_rounds_since_clean;

    if (delay <= 0 || delay > cct->_conf->mon_timecheck_interval) {
        delay = cct->_conf->mon_timecheck_interval;
    }

    dout(10) << __func__ << " delay " << delay
             << " rounds_since_clean " << timecheck_rounds_since_clean
             << dendl;

    timecheck_event = timer.add_event_after(
            delay,
            new C_MonContext{this, [this](int) {
                timecheck_start_round();
            }});
}

void PaxosMonitor::timecheck_check_skews()
{
    dout(10) << __func__ << dendl;
    ceph_assert(is_leader());
    ceph_assert((timecheck_round % 2) == 0);
    if (monmap->size() == 1) {
        ceph_abort_msg("We are alone; we shouldn't have gotten here!");
        return;
    }
    ceph_assert(timecheck_latencies.size() == timecheck_skews.size());

    bool found_skew = false;
    for (auto& p : timecheck_skews) {
        double abs_skew;
        if (timecheck_has_skew(p.second, &abs_skew)) {
            dout(10) << __func__
                     << " " << p.first << " skew " << abs_skew << dendl;
            found_skew = true;
        }
    }

    if (found_skew) {
        ++timecheck_rounds_since_clean;
        timecheck_reset_event();
    } else if (timecheck_rounds_since_clean > 0) {
        dout(1) << __func__
                << " no clock skews found after " << timecheck_rounds_since_clean
                << " rounds" << dendl;
        // make sure the skews are really gone and not just a transient success
        // this will run just once if not in the presence of skews again.
        timecheck_rounds_since_clean = 1;
        timecheck_reset_event();
        timecheck_rounds_since_clean = 0;
    }

}

void PaxosMonitor::timecheck_report()
{
    dout(10) << __func__ << dendl;
    ceph_assert(is_leader());
    ceph_assert((timecheck_round % 2) == 0);
    if (monmap->size() == 1) {
        ceph_abort_msg("We are alone; we shouldn't have gotten here!");
        return;
    }

    ceph_assert(timecheck_latencies.size() == timecheck_skews.size());
    bool do_output = true; // only output report once
    for (set<int>::iterator q = quorum.begin(); q != quorum.end(); ++q) {
        if (monmap->get_name(*q) == name)
            continue;

        MTimeCheck2 *m = new MTimeCheck2(MTimeCheck2::OP_REPORT);
        m->epoch = get_epoch();
        m->round = timecheck_round;

        for (auto& it : timecheck_skews) {
            double skew = it.second;
            double latency = timecheck_latencies[it.first];

            m->skews[it.first] = skew;
            m->latencies[it.first] = latency;

            if (do_output) {
                dout(25) << __func__ << " mon." << it.first
                         << " latency " << latency
                         << " skew " << skew << dendl;
            }
        }
        do_output = false;
        dout(10) << __func__ << " send report to mon." << *q << dendl;
        send_mon_message(m, *q);
    }
}

void PaxosMonitor::timecheck()
{
    dout(10) << __func__ << dendl;
    ceph_assert(is_leader());
    if (monmap->size() == 1) {
        ceph_abort_msg("We are alone; we shouldn't have gotten here!");
        return;
    }
    ceph_assert(timecheck_round % 2 != 0);

    timecheck_acks = 1; // we ack ourselves

    dout(10) << __func__ << " start timecheck epoch " << get_epoch()
             << " round " << timecheck_round << dendl;

    // we are at the eye of the storm; the point of reference
    timecheck_skews[rank] = 0.0;
    timecheck_latencies[rank] = 0.0;

    for (set<int>::iterator it = quorum.begin(); it != quorum.end(); ++it) {
        if (monmap->get_name(*it) == name)
            continue;

        utime_t curr_time = ceph_clock_now();
        timecheck_waiting[*it] = curr_time;
        MTimeCheck2 *m = new MTimeCheck2(MTimeCheck2::OP_PING);
        m->epoch = get_epoch();
        m->round = timecheck_round;
        dout(10) << __func__ << " send " << *m << " to mon." << *it << dendl;
        send_mon_message(m, *it);
    }
}

health_status_t PaxosMonitor::timecheck_status(ostringstream &ss,
                                                  const double skew_bound,
                                                  const double latency)
{
    health_status_t status = HEALTH_OK;
    ceph_assert(latency >= 0);

    double abs_skew;
    if (timecheck_has_skew(skew_bound, &abs_skew)) {
        status = HEALTH_WARN;
        ss << "clock skew " << abs_skew << "s"
           << " > max " << g_conf()->mon_clock_drift_allowed << "s";
    }

    return status;
}


// -----------------------------------------------------------
// sync

set<string> PaxosMonitor::get_sync_targets_names()
{
    set<string> targets;
    targets.insert(paxos->get_name());
    for (auto& svc : services) {
        svc->get_store_prefixes(targets);
    }
    return targets;
}


void PaxosMonitor::sync_timeout()
{
    dout(10) << __func__ << dendl;
    ceph_assert(state == STATE_SYNCHRONIZING);
    bootstrap();
}

void PaxosMonitor::sync_obtain_latest_monmap(bufferlist &bl)
{
    dout(1) << __func__ << dendl;

    MonMap latest_monmap;

    // Grab latest monmap from MonmapMonitor
    bufferlist monmon_bl;
    int err = monmon()->get_monmap(monmon_bl);
    if (err < 0) {
        if (err != -ENOENT) {
            derr << __func__
                 << " something wrong happened while reading the store: "
                 << cpp_strerror(err) << dendl;
            ceph_abort_msg("error reading the store");
        }
    } else {
        latest_monmap.decode(monmon_bl);
    }

    // Grab last backed up monmap (if any) and compare epochs
    if (store->exists("mon_sync", "latest_monmap")) {
        bufferlist backup_bl;
        int err = store->get("mon_sync", "latest_monmap", backup_bl);
        if (err < 0) {
            derr << __func__
                 << " something wrong happened while reading the store: "
                 << cpp_strerror(err) << dendl;
            ceph_abort_msg("error reading the store");
        }
        ceph_assert(backup_bl.length() > 0);

        MonMap backup_monmap;
        backup_monmap.decode(backup_bl);

        if (backup_monmap.epoch > latest_monmap.epoch)
            latest_monmap = backup_monmap;
    }

    // Check if our current monmap's epoch is greater than the one we've
    // got so far.
    if (monmap->epoch > latest_monmap.epoch)
        latest_monmap = *monmap;

    dout(1) << __func__ << " obtained monmap e" << latest_monmap.epoch << dendl;

    latest_monmap.encode(bl, CEPH_FEATURES_ALL);
}

void PaxosMonitor::sync_force(Formatter *f)
{
    auto tx(std::make_shared<MonitorDBStore::Transaction>());
    sync_stash_critical_state(tx);
    tx->put("mon_sync", "force_sync", 1);
    store->apply_transaction(tx);

    f->open_object_section("sync_force");
    f->dump_int("ret", 0);
    f->dump_stream("msg") << "forcing store sync the next time the monitor starts";
    f->close_section(); // sync_force
}

void PaxosMonitor::sync_reset_requester()
{
    dout(10) << __func__ << dendl;

    if (sync_timeout_event) {
        timer.cancel_event(sync_timeout_event);
        sync_timeout_event = NULL;
    }

    sync_provider = entity_addrvec_t();
    sync_cookie = 0;
    sync_full = false;
    sync_start_version = 0;
}

void PaxosMonitor::sync_reset_provider()
{
    dout(10) << __func__ << dendl;
    sync_providers.clear();
}

void PaxosMonitor::sync_start(entity_addrvec_t &addrs, bool full)
{
    dout(10) << __func__ << " " << addrs << (full ? " full" : " recent") << dendl;

    ceph_assert(state == STATE_PROBING ||
                state == STATE_SYNCHRONIZING);
    state = STATE_SYNCHRONIZING;

    // make sure are not a provider for anyone!
    sync_reset_provider();

    sync_full = full;

    if (sync_full) {
        // stash key state, and mark that we are syncing
        auto t(std::make_shared<MonitorDBStore::Transaction>());
        sync_stash_critical_state(t);
        t->put("mon_sync", "in_sync", 1);

        sync_last_committed_floor = std::max(sync_last_committed_floor, paxos->get_version());
        dout(10) << __func__ << " marking sync in progress, storing sync_last_committed_floor "
                 << sync_last_committed_floor << dendl;
        t->put("mon_sync", "last_committed_floor", sync_last_committed_floor);

        store->apply_transaction(t);

        ceph_assert(g_conf()->mon_sync_requester_kill_at != 1);

        // clear the underlying store
        set<string> targets = get_sync_targets_names();
        dout(10) << __func__ << " clearing prefixes " << targets << dendl;
        store->clear(targets);

        // make sure paxos knows it has been reset.  this prevents a
        // bootstrap and then different probe reply order from possibly
        // deciding a partial or no sync is needed.
        paxos->init();

        ceph_assert(g_conf()->mon_sync_requester_kill_at != 2);
    }

    // assume 'other' as the leader. We will update the leader once we receive
    // a reply to the sync start.
    sync_provider = addrs;

    sync_reset_timeout();

    MMonSync *m = new MMonSync(sync_full ? MMonSync::OP_GET_COOKIE_FULL : MMonSync::OP_GET_COOKIE_RECENT);
    if (!sync_full)
        m->last_committed = paxos->get_version();
    messenger->send_to_mon(m, sync_provider);
}

void PaxosMonitor::sync_stash_critical_state(MonitorDBStore::TransactionRef t)
{
    dout(10) << __func__ << dendl;
    bufferlist backup_monmap;
    sync_obtain_latest_monmap(backup_monmap);
    ceph_assert(backup_monmap.length() > 0);
    t->put("mon_sync", "latest_monmap", backup_monmap);
}

void PaxosMonitor::sync_reset_timeout()
{
    dout(10) << __func__ << dendl;
    if (sync_timeout_event)
        timer.cancel_event(sync_timeout_event);
    sync_timeout_event = timer.add_event_after(
            g_conf()->mon_sync_timeout,
            new C_MonContext{this, [this](int) {
                sync_timeout();
            }});
}

void PaxosMonitor::sync_finish(version_t last_committed)
{
    dout(10) << __func__ << " lc " << last_committed << " from " << sync_provider << dendl;

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 7);

    if (sync_full) {
        // finalize the paxos commits
        auto tx(std::make_shared<MonitorDBStore::Transaction>());
        paxos->read_and_prepare_transactions(tx, sync_start_version,
                                             last_committed);
        tx->put(paxos->get_name(), "last_committed", last_committed);

        dout(30) << __func__ << " final tx dump:\n";
                JSONFormatter f(true);
                tx->dump(&f);
                f.flush(*_dout);
                *_dout << dendl;

        store->apply_transaction(tx);
    }

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 8);

    auto t(std::make_shared<MonitorDBStore::Transaction>());
    t->erase("mon_sync", "in_sync");
    t->erase("mon_sync", "force_sync");
    t->erase("mon_sync", "last_committed_floor");
    store->apply_transaction(t);

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 9);

    init_paxos();

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 10);

    bootstrap();
}

void PaxosMonitor::handle_sync(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSync>();
    dout(10) << __func__ << " " << *m << dendl;
    switch (m->op) {

        // provider ---------

        case MMonSync::OP_GET_COOKIE_FULL:
        case MMonSync::OP_GET_COOKIE_RECENT:
            handle_sync_get_cookie(op);
            break;
        case MMonSync::OP_GET_CHUNK:
            handle_sync_get_chunk(op);
            break;

            // client -----------

        case MMonSync::OP_COOKIE:
            handle_sync_cookie(op);
            break;

        case MMonSync::OP_CHUNK:
        case MMonSync::OP_LAST_CHUNK:
            handle_sync_chunk(op);
            break;
        case MMonSync::OP_NO_COOKIE:
            handle_sync_no_cookie(op);
            break;

        default:
            dout(0) << __func__ << " unknown op " << m->op << dendl;
            ceph_abort_msg("unknown op");
    }
}

// leader

void PaxosMonitor::_sync_reply_no_cookie(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSync>();
    MMonSync *reply = new MMonSync(MMonSync::OP_NO_COOKIE, m->cookie);
    m->get_connection()->send_message(reply);
}

void PaxosMonitor::handle_sync_get_cookie(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSync>();
    if (is_synchronizing()) {
        _sync_reply_no_cookie(op);
        return;
    }

    ceph_assert(g_conf()->mon_sync_provider_kill_at != 1);

    // make sure they can understand us.
    if ((required_features ^ m->get_connection()->get_features()) &
        required_features) {
        dout(5) << " ignoring peer mon." << m->get_source().num()
                << " has features " << std::hex
                << m->get_connection()->get_features()
                << " but we require " << required_features << std::dec << dendl;
        return;
    }

    // make up a unique cookie.  include election epoch (which persists
    // across restarts for the whole cluster) and a counter for this
    // process instance.  there is no need to be unique *across*
    // monitors, though.
    uint64_t cookie = ((unsigned long long)elector.get_epoch() << 24) + ++sync_provider_count;
    ceph_assert(sync_providers.count(cookie) == 0);

    dout(10) << __func__ << " cookie " << cookie << " for " << m->get_source_inst() << dendl;

    SyncProvider& sp = sync_providers[cookie];
    sp.cookie = cookie;
    sp.addrs = m->get_source_addrs();
    sp.reset_timeout(g_ceph_context, g_conf()->mon_sync_timeout * 2);

    set<string> sync_targets;
    if (m->op == MMonSync::OP_GET_COOKIE_FULL) {
        // full scan
        sync_targets = get_sync_targets_names();
        sp.last_committed = paxos->get_version();
        sp.synchronizer = store->get_synchronizer(sp.last_key, sync_targets);
        sp.full = true;
        dout(10) << __func__ << " will sync prefixes " << sync_targets << dendl;
    } else {
        // just catch up paxos
        sp.last_committed = m->last_committed;
    }
    dout(10) << __func__ << " will sync from version " << sp.last_committed << dendl;

    MMonSync *reply = new MMonSync(MMonSync::OP_COOKIE, sp.cookie);
    reply->last_committed = sp.last_committed;
    m->get_connection()->send_message(reply);
}

void PaxosMonitor::handle_sync_get_chunk(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSync>();
    dout(10) << __func__ << " " << *m << dendl;

    if (sync_providers.count(m->cookie) == 0) {
        dout(10) << __func__ << " no cookie " << m->cookie << dendl;
        _sync_reply_no_cookie(op);
        return;
    }

    ceph_assert(g_conf()->mon_sync_provider_kill_at != 2);

    SyncProvider& sp = sync_providers[m->cookie];
    sp.reset_timeout(g_ceph_context, g_conf()->mon_sync_timeout * 2);

    if (sp.last_committed < paxos->get_first_committed() &&
        paxos->get_first_committed() > 1) {
        dout(10) << __func__ << " sync requester fell behind paxos, their lc " << sp.last_committed
                 << " < our fc " << paxos->get_first_committed() << dendl;
        sync_providers.erase(m->cookie);
        _sync_reply_no_cookie(op);
        return;
    }

    MMonSync *reply = new MMonSync(MMonSync::OP_CHUNK, sp.cookie);
    auto tx(std::make_shared<MonitorDBStore::Transaction>());

    int bytes_left = g_conf()->mon_sync_max_payload_size;
    int keys_left = g_conf()->mon_sync_max_payload_keys;
    while (sp.last_committed < paxos->get_version() &&
           bytes_left > 0 &&
           keys_left > 0) {
        bufferlist bl;
        sp.last_committed++;

        int err = store->get(paxos->get_name(), sp.last_committed, bl);
        ceph_assert(err == 0);

        tx->put(paxos->get_name(), sp.last_committed, bl);
        bytes_left -= bl.length();
        --keys_left;
        dout(20) << __func__ << " including paxos state " << sp.last_committed
                 << dendl;
    }
    reply->last_committed = sp.last_committed;

    if (sp.full && bytes_left > 0 && keys_left > 0) {
        sp.synchronizer->get_chunk_tx(tx, bytes_left, keys_left);
        sp.last_key = sp.synchronizer->get_last_key();
        reply->last_key = sp.last_key;
    }

    if ((sp.full && sp.synchronizer->has_next_chunk()) ||
        sp.last_committed < paxos->get_version()) {
        dout(10) << __func__ << " chunk, through version " << sp.last_committed
                 << " key " << sp.last_key << dendl;
    } else {
        dout(10) << __func__ << " last chunk, through version " << sp.last_committed
                 << " key " << sp.last_key << dendl;
        reply->op = MMonSync::OP_LAST_CHUNK;

        ceph_assert(g_conf()->mon_sync_provider_kill_at != 3);

        // clean up our local state
        sync_providers.erase(sp.cookie);
    }

    encode(*tx, reply->chunk_bl);

    m->get_connection()->send_message(reply);
}

// requester

void PaxosMonitor::handle_sync_cookie(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSync>();
    dout(10) << __func__ << " " << *m << dendl;
    if (sync_cookie) {
        dout(10) << __func__ << " already have a cookie, ignoring" << dendl;
        return;
    }
    if (m->get_source_addrs() != sync_provider) {
        dout(10) << __func__ << " source does not match, discarding" << dendl;
        return;
    }
    sync_cookie = m->cookie;
    sync_start_version = m->last_committed;

    sync_reset_timeout();
    sync_get_next_chunk();

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 3);
}

void PaxosMonitor::sync_get_next_chunk()
{
    dout(20) << __func__ << " cookie " << sync_cookie << " provider " << sync_provider << dendl;
    if (g_conf()->mon_inject_sync_get_chunk_delay > 0) {
        dout(20) << __func__ << " injecting delay of " << g_conf()->mon_inject_sync_get_chunk_delay << dendl;
        usleep((long long)(g_conf()->mon_inject_sync_get_chunk_delay * 1000000.0));
    }
    MMonSync *r = new MMonSync(MMonSync::OP_GET_CHUNK, sync_cookie);
    messenger->send_to_mon(r, sync_provider);

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 4);
}

void PaxosMonitor::handle_sync_chunk(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSync>();
    dout(10) << __func__ << " " << *m << dendl;

    if (m->cookie != sync_cookie) {
        dout(10) << __func__ << " cookie does not match, discarding" << dendl;
        return;
    }
    if (m->get_source_addrs() != sync_provider) {
        dout(10) << __func__ << " source does not match, discarding" << dendl;
        return;
    }

    ceph_assert(state == STATE_SYNCHRONIZING);
    ceph_assert(g_conf()->mon_sync_requester_kill_at != 5);

    auto tx(std::make_shared<MonitorDBStore::Transaction>());
    tx->append_from_encoded(m->chunk_bl);

    dout(30) << __func__ << " tx dump:\n";
            JSONFormatter f(true);
            tx->dump(&f);
            f.flush(*_dout);
            *_dout << dendl;

    store->apply_transaction(tx);

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 6);

    if (!sync_full) {
        dout(10) << __func__ << " applying recent paxos transactions as we go" << dendl;
        auto tx(std::make_shared<MonitorDBStore::Transaction>());
        paxos->read_and_prepare_transactions(tx, paxos->get_version() + 1,
                                             m->last_committed);
        tx->put(paxos->get_name(), "last_committed", m->last_committed);

        dout(30) << __func__ << " tx dump:\n";
                JSONFormatter f(true);
                tx->dump(&f);
                f.flush(*_dout);
                *_dout << dendl;

        store->apply_transaction(tx);
        paxos->init();  // to refresh what we just wrote
    }

    if (m->op == MMonSync::OP_CHUNK) {
        sync_reset_timeout();
        sync_get_next_chunk();
    } else if (m->op == MMonSync::OP_LAST_CHUNK) {
        sync_finish(m->last_committed);
    }
}

void PaxosMonitor::handle_sync_no_cookie(MonOpRequestRef op)
{
    dout(10) << __func__ << dendl;
    bootstrap();
}

void PaxosMonitor::sync_trim_providers()
{
    dout(20) << __func__ << dendl;

    utime_t now = ceph_clock_now();
    map<uint64_t,SyncProvider>::iterator p = sync_providers.begin();
    while (p != sync_providers.end()) {
        if (now > p->second.timeout) {
            dout(10) << __func__ << " expiring cookie " << p->second.cookie
                     << " for " << p->second.addrs << dendl;
            sync_providers.erase(p++);
        } else {
            ++p;
        }
    }
}

// ---------------------------------------------------
// probe

void PaxosMonitor::cancel_probe_timeout()
{
    if (probe_timeout_event) {
        dout(10) << "cancel_probe_timeout " << probe_timeout_event << dendl;
        timer.cancel_event(probe_timeout_event);
        probe_timeout_event = NULL;
    } else {
        dout(10) << "cancel_probe_timeout (none scheduled)" << dendl;
    }
}

void PaxosMonitor::reset_probe_timeout()
{
    cancel_probe_timeout();
    probe_timeout_event = new C_MonContext{this, [this](int r) {
        probe_timeout(r);
    }};
    double t = g_conf()->mon_probe_timeout;
    if (timer.add_event_after(t, probe_timeout_event)) {
        dout(10) << "reset_probe_timeout " << probe_timeout_event
                 << " after " << t << " seconds" << dendl;
    } else {
        probe_timeout_event = nullptr;
    }
}

void PaxosMonitor::probe_timeout(int r)
{
    dout(4) << "probe_timeout " << probe_timeout_event << dendl;
    ceph_assert(is_probing() || is_synchronizing());
    ceph_assert(probe_timeout_event);
    probe_timeout_event = NULL;
    bootstrap();
}

void PaxosMonitor::handle_probe(MonOpRequestRef op)
{
    auto m = op->get_req<MMonProbe>();
    dout(10) << "handle_probe " << *m << dendl;

    if (m->fsid != monmap->fsid) {
        dout(0) << "handle_probe ignoring fsid " << m->fsid << " != " << monmap->fsid << dendl;
        return;
    }

    switch (m->op) {
        case MMonProbe::OP_PROBE:
            handle_probe_probe(op);
            break;

        case MMonProbe::OP_REPLY:
            handle_probe_reply(op);
            break;

        case MMonProbe::OP_MISSING_FEATURES:
            derr << __func__ << " require release " << (int)m->mon_release << " > "
                 << (int)ceph_release()
                 << ", or missing features (have " << CEPH_FEATURES_ALL
                 << ", required " << m->required_features
                 << ", missing " << (m->required_features & ~CEPH_FEATURES_ALL) << ")"
                 << dendl;
            break;
    }
}

void PaxosMonitor::handle_probe_probe(MonOpRequestRef op)
{
    auto m = op->get_req<MMonProbe>();

    dout(10) << "handle_probe_probe " << m->get_source_inst() << " " << *m
             << " features " << m->get_connection()->get_features() << dendl;
    uint64_t missing = required_features & ~m->get_connection()->get_features();
    if ((m->mon_release != ceph_release_t::unknown &&
         m->mon_release < monmap->min_mon_release) ||
        missing) {
        dout(1) << " peer " << m->get_source_addr()
                << " release " << m->mon_release
                << " < min_mon_release " << monmap->min_mon_release
                << ", or missing features " << missing << dendl;
        MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_MISSING_FEATURES,
                                     name, has_ever_joined, monmap->min_mon_release);
        m->required_features = required_features;
        m->get_connection()->send_message(r);
        goto out;
    }

    if (!is_probing() && !is_synchronizing()) {
        // If the probing mon is way ahead of us, we need to re-bootstrap.
        // Normally we capture this case when we initially bootstrap, but
        // it is possible we pass those checks (we overlap with
        // quorum-to-be) but fail to join a quorum before it moves past
        // us.  We need to be kicked back to bootstrap so we can
        // synchonize, not keep calling elections.
        if (paxos->get_version() + 1 < m->paxos_first_version) {
            dout(1) << " peer " << m->get_source_addr() << " has first_committed "
                    << "ahead of us, re-bootstrapping" << dendl;
            bootstrap();
            goto out;

        }
    }

    MMonProbe *r;
    r = new MMonProbe(monmap->fsid, MMonProbe::OP_REPLY, name, has_ever_joined,
                      ceph_release());
    r->name = name;
    r->quorum = quorum;
    r->leader = leader;
    monmap->encode(r->monmap_bl, m->get_connection()->get_features());
    r->paxos_first_version = paxos->get_first_committed();
    r->paxos_last_version = paxos->get_version();
    m->get_connection()->send_message(r);

    // did we discover a peer here?
    if (!monmap->contains(m->get_source_addr())) {
        dout(1) << " adding peer " << m->get_source_addrs()
                << " to list of hints" << dendl;
        extra_probe_peers.insert(m->get_source_addrs());
    } else {
        elector.begin_peer_ping(monmap->get_rank(m->get_source_addr()));
    }

    out:
    return;
}

void PaxosMonitor::handle_probe_reply(MonOpRequestRef op)
{
    auto m = op->get_req<MMonProbe>();
    dout(10) << "handle_probe_reply " << m->get_source_inst()
             << " " << *m << dendl;
    dout(10) << " monmap is " << *monmap << dendl;

    // discover name and addrs during probing or electing states.
    if (!is_probing() && !is_electing()) {
        return;
    }

    // newer map, or they've joined a quorum and we haven't?
    bufferlist mybl;
    monmap->encode(mybl, m->get_connection()->get_features());
    // make sure it's actually different; the checks below err toward
    // taking the other guy's map, which could cause us to loop.
    if (!mybl.contents_equal(m->monmap_bl)) {
        MonMap *newmap = new MonMap;
        newmap->decode(m->monmap_bl);
        if (m->has_ever_joined && (newmap->get_epoch() > monmap->get_epoch() ||
                                   !has_ever_joined)) {
            dout(10) << " got newer/committed monmap epoch " << newmap->get_epoch()
                     << ", mine was " << monmap->get_epoch() << dendl;
            delete newmap;
            monmap->decode(m->monmap_bl);
            notify_new_monmap(false);

            bootstrap();
            return;
        }
        delete newmap;
    }

    // rename peer?
    string peer_name = monmap->get_name(m->get_source_addr());
    if (monmap->get_epoch() == 0 && peer_name.compare(0, 7, "noname-") == 0) {
        dout(10) << " renaming peer " << m->get_source_addr() << " "
                 << peer_name << " -> " << m->name << " in my monmap"
                 << dendl;
        monmap->rename(peer_name, m->name);

        if (is_electing()) {
            bootstrap();
            return;
        }
    } else if (peer_name.size()) {
        dout(10) << " peer name is " << peer_name << dendl;
    } else {
        dout(10) << " peer " << m->get_source_addr() << " not in map" << dendl;
    }

    // new initial peer?
    if (monmap->get_epoch() == 0 &&
        monmap->contains(m->name) &&
        monmap->get_addrs(m->name).front().is_blank_ip()) {
        dout(1) << " learned initial mon " << m->name
                << " addrs " << m->get_source_addrs() << dendl;
        monmap->set_addrvec(m->name, m->get_source_addrs());

        bootstrap();
        return;
    }

    // end discover phase
    if (!is_probing()) {
        return;
    }

    ceph_assert(paxos != NULL);

    if (is_synchronizing()) {
        dout(10) << " currently syncing" << dendl;
        return;
    }

    entity_addrvec_t other = m->get_source_addrs();

    if (m->paxos_last_version < sync_last_committed_floor) {
        dout(10) << " peer paxos versions [" << m->paxos_first_version
                 << "," << m->paxos_last_version << "] < my sync_last_committed_floor "
                 << sync_last_committed_floor << ", ignoring"
                 << dendl;
    } else {
        if (paxos->get_version() < m->paxos_first_version &&
            m->paxos_first_version > 1) {  // no need to sync if we're 0 and they start at 1.
            dout(10) << " peer paxos first versions [" << m->paxos_first_version
                     << "," << m->paxos_last_version << "]"
                     << " vs my version " << paxos->get_version()
                     << " (too far ahead)"
                     << dendl;
            cancel_probe_timeout();
            sync_start(other, true);
            return;
        }
        if (paxos->get_version() + g_conf()->paxos_max_join_drift < m->paxos_last_version) {
            dout(10) << " peer paxos last version " << m->paxos_last_version
                     << " vs my version " << paxos->get_version()
                     << " (too far ahead)"
                     << dendl;
            cancel_probe_timeout();
            sync_start(other, false);
            return;
        }
    }

    // did the existing cluster complete upgrade to luminous?
    if (osdmon()->osdmap.get_epoch()) {
        if (osdmon()->osdmap.require_osd_release < ceph_release_t::luminous) {
            derr << __func__ << " existing cluster has not completed upgrade to"
                 << " luminous; 'ceph osd require_osd_release luminous' before"
                 << " upgrading" << dendl;
            exit(0);
        }
        if (!osdmon()->osdmap.test_flag(CEPH_OSDMAP_PURGED_SNAPDIRS) ||
            !osdmon()->osdmap.test_flag(CEPH_OSDMAP_RECOVERY_DELETES)) {
            derr << __func__ << " existing cluster has not completed a full luminous"
                 << " scrub to purge legacy snapdir objects; please scrub before"
                 << " upgrading beyond luminous." << dendl;
            exit(0);
        }
    }

    // is there an existing quorum?
    if (m->quorum.size()) {
        dout(10) << " existing quorum " << m->quorum << dendl;

        dout(10) << " peer paxos version " << m->paxos_last_version
                 << " vs my version " << paxos->get_version()
                 << " (ok)"
                 << dendl;
        bool in_map = false;
        const auto my_info = monmap->mon_info.find(name);
        const map<string,string> *map_crush_loc{nullptr};
        if (my_info != monmap->mon_info.end()) {
            in_map = true;
            map_crush_loc = &my_info->second.crush_loc;
        }
        if (in_map &&
            !monmap->get_addrs(name).front().is_blank_ip() &&
            (!need_set_crush_loc || (*map_crush_loc == crush_loc))) {
            // i'm part of the cluster; just initiate a new election
            start_election();
        } else {
            dout(10) << " ready to join, but i'm not in the monmap/"
                        "my addr is blank/location is wrong, trying to join" << dendl;
            send_mon_message(new MMonJoin(monmap->fsid, name,
                                          messenger->get_myaddrs(), crush_loc,
                                          need_set_crush_loc),
                             m->leader);
        }
    } else {
        if (monmap->contains(m->name)) {
            dout(10) << " mon." << m->name << " is outside the quorum" << dendl;
            outside_quorum.insert(m->name);
        } else {
            dout(10) << " mostly ignoring mon." << m->name << ", not part of monmap" << dendl;
            return;
        }

        unsigned need = monmap->min_quorum_size();
        dout(10) << " outside_quorum now " << outside_quorum << ", need " << need << dendl;
        if (outside_quorum.size() >= need) {
            if (outside_quorum.count(name)) {
                dout(10) << " that's enough to form a new quorum, calling election" << dendl;
                start_election();
            } else {
                dout(10) << " that's enough to form a new quorum, but it does not include me; waiting" << dendl;
            }
        } else {
            dout(10) << " that's not yet enough for a new quorum, waiting" << dendl;
        }
    }
}


// ----------------------------------------------
// scrub

int PaxosMonitor::scrub_start()
{
    dout(10) << __func__ << dendl;
    ceph_assert(is_leader());

    if (!scrub_result.empty()) {
        clog->info() << "scrub already in progress";
        return -EBUSY;
    }

    scrub_event_cancel();
    scrub_result.clear();
    scrub_state.reset(new ScrubState);

    scrub();
    return 0;
}

int PaxosMonitor::scrub()
{
    ceph_assert(is_leader());
    ceph_assert(scrub_state);

    scrub_cancel_timeout();
    wait_for_paxos_write();
    scrub_version = paxos->get_version();


    // scrub all keys if we're the only monitor in the quorum
    int32_t num_keys =
            (quorum.size() == 1 ? -1 : cct->_conf->mon_scrub_max_keys);

    for (set<int>::iterator p = quorum.begin();
         p != quorum.end();
         ++p) {
        if (*p == rank)
            continue;
        MMonScrub *r = new MMonScrub(MMonScrub::OP_SCRUB, scrub_version,
                                     num_keys);
        r->key = scrub_state->last_key;
        send_mon_message(r, *p);
    }

    // scrub my keys
    bool r = _scrub(&scrub_result[rank],
                    &scrub_state->last_key,
                    &num_keys);

    scrub_state->finished = !r;

    // only after we got our scrub results do we really care whether the
    // other monitors are late on their results.  Also, this way we avoid
    // triggering the timeout if we end up getting stuck in _scrub() for
    // longer than the duration of the timeout.
    scrub_reset_timeout();

    if (quorum.size() == 1) {
        ceph_assert(scrub_state->finished == true);
        scrub_finish();
    }
    return 0;
}

void PaxosMonitor::handle_scrub(MonOpRequestRef op)
{
    auto m = op->get_req<MMonScrub>();
    dout(10) << __func__ << " " << *m << dendl;
    switch (m->op) {
        case MMonScrub::OP_SCRUB:
        {
            if (!is_peon())
                break;

            wait_for_paxos_write();

            if (m->version != paxos->get_version())
                break;

            MMonScrub *reply = new MMonScrub(MMonScrub::OP_RESULT,
                                             m->version,
                                             m->num_keys);

            reply->key = m->key;
            _scrub(&reply->result, &reply->key, &reply->num_keys);
            m->get_connection()->send_message(reply);
        }
            break;

        case MMonScrub::OP_RESULT:
        {
            if (!is_leader())
                break;
            if (m->version != scrub_version)
                break;
            // reset the timeout each time we get a result
            scrub_reset_timeout();

            int from = m->get_source().num();
            ceph_assert(scrub_result.count(from) == 0);
            scrub_result[from] = m->result;

            if (scrub_result.size() == quorum.size()) {
                scrub_check_results();
                scrub_result.clear();
                if (scrub_state->finished)
                    scrub_finish();
                else
                    scrub();
            }
        }
            break;
    }
}

bool PaxosMonitor::_scrub(ScrubResult *r,
                     pair<string,string> *start,
                     int *num_keys)
{
    ceph_assert(r != NULL);
    ceph_assert(start != NULL);
    ceph_assert(num_keys != NULL);

    set<string> prefixes = get_sync_targets_names();
    prefixes.erase("paxos");  // exclude paxos, as this one may have extra states for proposals, etc.

    dout(10) << __func__ << " start (" << *start << ")"
             << " num_keys " << *num_keys << dendl;

    MonitorDBStore::Synchronizer it = store->get_synchronizer(*start, prefixes);

    int scrubbed_keys = 0;
    pair<string,string> last_key;

    while (it->has_next_chunk()) {

        if (*num_keys > 0 && scrubbed_keys == *num_keys)
            break;

        pair<string,string> k = it->get_next_key();
        if (prefixes.count(k.first) == 0)
            continue;

        if (cct->_conf->mon_scrub_inject_missing_keys > 0.0 &&
            (rand() % 10000 < cct->_conf->mon_scrub_inject_missing_keys*10000.0)) {
            dout(10) << __func__ << " inject missing key, skipping (" << k << ")"
                     << dendl;
            continue;
        }

        bufferlist bl;
        int err = store->get(k.first, k.second, bl);
        ceph_assert(err == 0);

        uint32_t key_crc = bl.crc32c(0);
        dout(30) << __func__ << " " << k << " bl " << bl.length() << " bytes"
                 << " crc " << key_crc << dendl;
        r->prefix_keys[k.first]++;
        if (r->prefix_crc.count(k.first) == 0) {
            r->prefix_crc[k.first] = 0;
        }
        r->prefix_crc[k.first] = bl.crc32c(r->prefix_crc[k.first]);

        if (cct->_conf->mon_scrub_inject_crc_mismatch > 0.0 &&
            (rand() % 10000 < cct->_conf->mon_scrub_inject_crc_mismatch*10000.0)) {
            dout(10) << __func__ << " inject failure at (" << k << ")" << dendl;
            r->prefix_crc[k.first] += 1;
        }

        ++scrubbed_keys;
        last_key = k;
    }

    dout(20) << __func__ << " last_key (" << last_key << ")"
             << " scrubbed_keys " << scrubbed_keys
             << " has_next " << it->has_next_chunk() << dendl;

    *start = last_key;
    *num_keys = scrubbed_keys;

    return it->has_next_chunk();
}

void PaxosMonitor::scrub_check_results()
{
    dout(10) << __func__ << dendl;

    // compare
    int errors = 0;
    ScrubResult& mine = scrub_result[rank];
    for (map<int,ScrubResult>::iterator p = scrub_result.begin();
         p != scrub_result.end();
         ++p) {
        if (p->first == rank)
            continue;
        if (p->second != mine) {
            ++errors;
            clog->error() << "scrub mismatch";
            clog->error() << " mon." << rank << " " << mine;
            clog->error() << " mon." << p->first << " " << p->second;
        }
    }
    if (!errors)
        clog->debug() << "scrub ok on " << quorum << ": " << mine;
}

inline void PaxosMonitor::scrub_timeout()
{
    dout(1) << __func__ << " restarting scrub" << dendl;
    scrub_reset();
    scrub_start();
}

void PaxosMonitor::scrub_finish()
{
    dout(10) << __func__ << dendl;
    scrub_reset();
    scrub_event_start();
}

void PaxosMonitor::scrub_reset()
{
    dout(10) << __func__ << dendl;
    scrub_cancel_timeout();
    scrub_version = 0;
    scrub_result.clear();
    scrub_state.reset();
}

inline void PaxosMonitor::scrub_update_interval(ceph::timespan interval)
{
    // we don't care about changes if we are not the leader.
    // changes will be visible if we become the leader.
    if (!is_leader())
        return;

    dout(1) << __func__ << " new interval = " << interval << dendl;

    // if scrub already in progress, all changes will already be visible during
    // the next round.  Nothing to do.
    if (scrub_state != NULL)
        return;

    scrub_event_cancel();
    scrub_event_start();
}

void PaxosMonitor::scrub_event_start()
{
    dout(10) << __func__ << dendl;

    if (scrub_event)
        scrub_event_cancel();

    auto scrub_interval =
            cct->_conf.get_val<std::chrono::seconds>("mon_scrub_interval");
    if (scrub_interval == std::chrono::seconds::zero()) {
        dout(1) << __func__ << " scrub event is disabled"
                << " (mon_scrub_interval = " << scrub_interval
                << ")" << dendl;
        return;
    }

    scrub_event = timer.add_event_after(
            scrub_interval,
            new C_MonContext{this, [this](int) {
                scrub_start();
            }});
}

void PaxosMonitor::scrub_event_cancel()
{
    dout(10) << __func__ << dendl;
    if (scrub_event) {
        timer.cancel_event(scrub_event);
        scrub_event = NULL;
    }
}

inline void PaxosMonitor::scrub_cancel_timeout()
{
    if (scrub_timeout_event) {
        timer.cancel_event(scrub_timeout_event);
        scrub_timeout_event = NULL;
    }
}

void PaxosMonitor::scrub_reset_timeout()
{
    dout(15) << __func__ << " reset timeout event" << dendl;
    scrub_cancel_timeout();
    scrub_timeout_event = timer.add_event_after(
            g_conf()->mon_scrub_timeout,
            new C_MonContext{this, [this](int) {
                scrub_timeout();
            }});
}

int PaxosMonitor::do_admin_command(
        std::string_view command,
        const cmdmap_t& cmdmap,
        Formatter *f,
        std::ostream& err,
        std::ostream& out)
{
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
        (void)op_tracker.dump_ops_in_flight(f);
    } else if (command == "sessions") {
        f->open_array_section("sessions");
        for (auto p : session_map.sessions) {
            f->dump_object("session", *p);
        }
        f->close_section();
    } else if (command == "dump_historic_ops") {
        if (!op_tracker.dump_historic_ops(f)) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "dump_historic_ops_by_duration" ) {
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
        for (auto& devname : devnames) {
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

bool PaxosMonitor::_add_bootstrap_peer_hint(std::string_view cmd,
                                       const cmdmap_t& cmdmap,
                                       ostream& ss)
{
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

void PaxosMonitor::format_command_descriptions(const std::vector<MonCommand> &commands,
                                          Formatter *f,
                                          uint64_t features,
                                          bufferlist *rdata)
{
    int cmdnum = 0;
    f->open_object_section("command_descriptions");
    for (const auto &cmd : commands) {
        unsigned flags = cmd.flags;
        ostringstream secname;
        secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
        dump_cmddesc_to_json(f, features, secname.str(),
                             cmd.cmdstring, cmd.helpstring, cmd.module,
                             cmd.req_perms, flags);
        cmdnum++;
    }
    f->close_section();	// command_descriptions

    f->flush(*rdata);
}

#undef FLAG
#undef COMMAND
#undef COMMAND_WITH_FLAG
#define FLAG(f) (MonCommand::FLAG_##f)
#define COMMAND(parsesig, helptext, modulename, req_perms)    \
  {parsesig, helptext, modulename, req_perms, FLAG(NONE)},
#define COMMAND_WITH_FLAG(parsesig, helptext, modulename, req_perms, flags) \
  {parsesig, helptext, modulename, req_perms, flags},
MonCommand mon_commands[] = {
#include <mon/MonCommands.h>
};
#undef COMMAND
#undef COMMAND_WITH_FLAG

PaxosMonitor::PaxosMonitor(CephContext *cct_, MonitorDBStore *store, std::string nm, Messenger *m, Messenger *mgr_m,
                           MonMap *map)
        : AbstractMonitor(cct_, store, nm, m, mgr_m, map),
          elector(this, map->strategy),
          required_features(0),
          leader(0),
          has_ever_joined(false),
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
          scrub_timeout_event(NULL)
{

    paxos = std::make_unique<PaxosSMR>(*this, "paxos");

    services[PAXOS_MDSMAP].reset(new MDSMonitor(*this, *paxos, "mdsmap"));
    services[PAXOS_MONMAP].reset(new MonmapMonitor(*this, *paxos, "monmap"));
    services[PAXOS_OSDMAP].reset(new OSDMonitor(cct, *this, *paxos, "osdmap"));
    services[PAXOS_LOG].reset(new LogMonitor(*this, *paxos, "logm"));
    services[PAXOS_AUTH].reset(new AuthMonitor(*this, *paxos, "auth"));
    services[PAXOS_MGR].reset(new MgrMonitor(*this, *paxos, "mgr"));
    services[PAXOS_MGRSTAT].reset(new MgrStatMonitor(*this, *paxos, "mgrstat"));
    services[PAXOS_HEALTH].reset(new HealthMonitor(*this, *paxos, "health"));
    services[PAXOS_CONFIG].reset(new ConfigMonitor(*this, *paxos, "config"));
    services[PAXOS_KV].reset(new KVMonitor(*this, *paxos, "kv"));

    exited_quorum = ceph_clock_now();

    // prepare local commands
    local_mon_commands.resize(std::size(mon_commands));
    for (unsigned i = 0; i < std::size(mon_commands); ++i) {
        local_mon_commands[i] = mon_commands[i];
    }

    MonCommand::encode_vector(local_mon_commands, local_mon_commands_bl);

    prenautilus_local_mon_commands = local_mon_commands;
    for (auto& i : prenautilus_local_mon_commands) {
        std::string n = cmddesc_get_prenautilus_compat(i.cmdstring);
        if (n != i.cmdstring) {
            dout(20) << " pre-nautilus cmd " << i.cmdstring << " -> " << n << dendl;
            i.cmdstring = n;
        }
    }

    MonCommand::encode_vector(prenautilus_local_mon_commands, prenautilus_local_mon_commands_bl);

    // assume our commands until we have an election.  this only means
    // we won't reply with EINVAL before the election; any command that
    // actually matters will wait until we have quorum etc and then
    // retry (and revalidate).
    leader_mon_commands = local_mon_commands;
}