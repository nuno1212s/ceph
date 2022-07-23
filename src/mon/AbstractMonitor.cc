#include "AbstractMonitor.h"

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

const string AbstractMonitor::MONITOR_NAME = "monitor";
const string AbstractMonitor::MONITOR_STORE_PREFIX = "monitor_store";

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

AbstractMonitor::AbstractMonitor(CephContext *cct_, MonitorDBStore *store, string nm, Messenger *m, Messenger *mgr_m, MonMap *map)
        : Dispatcher(cct_),
          AuthServer(cct_),
          store(store),
          name(nm),
          rank(-1),
          messenger(m),
          con_self(m ? m->get_loopback_connection() : NULL),
          timer(cct_, lock),
          finisher(cct_, "mon_finisher", "fin"),
          cpu_tp(cct,
                 "Monitor::cpu_tp", "cpu_tp", g_conf()->mon_cpu_threads),
          monmap(map),
          log_client(cct_, messenger, monmap, LogClient::FLAG_MON),
          key_server(cct, &keyring
          ),
          auth_cluster_required(cct,
                                cct->_conf->auth_supported.empty() ?
                                cct->_conf->auth_cluster_required : cct->_conf->auth_supported),
          auth_service_required(cct,
                                cct->_conf->auth_supported.empty() ?
                                cct->_conf->auth_service_required : cct->_conf->auth_supported),
          mgr_messenger(mgr_m),
          mgr_client(cct_, mgr_m, monmap),
          gss_ktfile_client(cct->_conf.get_val<std::string>("gss_ktab_client_file")),
        // scrub
          scrub_version(0),
          scrub_event(NULL),
          scrub_timeout_event(NULL),
          //timecheck stuff
          timecheck_round(0),
          timecheck_acks(0),
          timecheck_rounds_since_clean(0),
          timecheck_event(NULL),
          //sessions
          admin_hook(NULL),
          routed_request_tid(0),
          op_tracker(cct, g_conf().get_val<bool>("mon_enable_op_tracker"), 1)
{
    clog = log_client.create_channel(CLOG_CHANNEL_CLUSTER);
    audit_clog = log_client.create_channel(CLOG_CHANNEL_AUDIT);

    update_log_clients();

    if (!gss_ktfile_client.empty()) {
        // Assert we can export environment variable
        /*
            The default client keytab is used, if it is present and readable,
            to automatically obtain initial credentials for GSSAPI client
            applications. The principal name of the first entry in the client
            keytab is used by default when obtaining initial credentials.
            1. The KRB5_CLIENT_KTNAME environment variable.
            2. The default_client_keytab_name profile variable in [libdefaults].
            3. The hardcoded default, DEFCKTNAME.
        */
        const int32_t set_result(setenv("KRB5_CLIENT_KTNAME",
                                        gss_ktfile_client.c_str(), 1));
        ceph_assert(set_result == 0);
    }

    op_tracker.set_complaint_and_threshold(
            g_conf().get_val<std::chrono::seconds>("mon_op_complaint_time").count(),
            g_conf().get_val<int64_t>("mon_op_log_threshold"));
    op_tracker.set_history_size_and_duration(
            g_conf().get_val<uint64_t>("mon_op_history_size"),
            g_conf().get_val<std::chrono::seconds>("mon_op_history_duration").count());
    op_tracker.set_history_slow_op_size_and_threshold(
            g_conf().get_val<uint64_t>("mon_op_history_slow_op_size"),
            g_conf().get_val<std::chrono::seconds>("mon_op_history_slow_op_threshold").count());

    //TODO: Services

    bool r = mon_caps.parse("allow *", NULL);
    ceph_assert(r);
}


AbstractMonitor::~AbstractMonitor()
{
    op_tracker.on_shutdown();

    delete logger;
    ceph_assert(session_map.sessions.empty());
}

class AdminHook : public AdminSocketHook {
    AbstractMonitor *mon;
public:
    explicit AdminHook(AbstractMonitor *m) : mon(m) {}
    int call(std::string_view command, const cmdmap_t& cmdmap,
             Formatter *f,
             std::ostream& errss,
             bufferlist& out) override {
        stringstream outss;
        int r = mon->do_admin_command(command, cmdmap, f, errss, outss);
        out.append(outss);
        return r;
    }
};

bool AbstractMonitor::is_keyring_required()
{
    return auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX) ||
           auth_service_required.is_supported_auth(CEPH_AUTH_CEPHX) ||
           auth_cluster_required.is_supported_auth(CEPH_AUTH_GSS)   ||
           auth_service_required.is_supported_auth(CEPH_AUTH_GSS);
}

void AbstractMonitor::handle_signal(int signum)
{
    derr << "*** Got Signal " << sig_str(signum) << " ***" << dendl;
    if (signum == SIGHUP) {
        sighup_handler(signum);
        //TODO: Uncomment this
        //logmon()->reopen_logs();
    } else {
        ceph_assert(signum == SIGINT || signum == SIGTERM);
        shutdown();
    }
}

CompatSet AbstractMonitor::get_initial_supported_features()
{
    CompatSet::FeatureSet ceph_mon_feature_compat;
    CompatSet::FeatureSet ceph_mon_feature_ro_compat;
    CompatSet::FeatureSet ceph_mon_feature_incompat;
    ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_BASE);
    ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS);
    return CompatSet(ceph_mon_feature_compat, ceph_mon_feature_ro_compat,
                     ceph_mon_feature_incompat);
}

CompatSet AbstractMonitor::get_supported_features()
{
    CompatSet compat = get_initial_supported_features();
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_KRAKEN);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_LUMINOUS);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_MIMIC);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_NAUTILUS);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OCTOPUS);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_PACIFIC);
    compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_QUINCY);
    return compat;
}

CompatSet AbstractMonitor::get_legacy_features()
{
    CompatSet::FeatureSet ceph_mon_feature_compat;
    CompatSet::FeatureSet ceph_mon_feature_ro_compat;
    CompatSet::FeatureSet ceph_mon_feature_incompat;
    ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_BASE);
    return CompatSet(ceph_mon_feature_compat, ceph_mon_feature_ro_compat,
                     ceph_mon_feature_incompat);
}

int AbstractMonitor::check_features(MonitorDBStore *store)
{
    CompatSet required = get_supported_features();
    CompatSet ondisk;

    read_features_off_disk(store, &ondisk);

    if (!required.writeable(ondisk)) {
        CompatSet diff = required.unsupported(ondisk);
        generic_derr << "ERROR: on disk data includes unsupported features: " << diff << dendl;
        return -EPERM;
    }

    return 0;
}

void AbstractMonitor::read_features_off_disk(MonitorDBStore *store, CompatSet *features)
{
    bufferlist featuresbl;
    store->get(MONITOR_NAME, COMPAT_SET_LOC, featuresbl);
    if (featuresbl.length() == 0) {
        generic_dout(0) << "WARNING: mon fs missing feature list.\n"
                        << "Assuming it is old-style and introducing one." << dendl;
        //we only want the baseline ~v.18 features assumed to be on disk.
        //If new features are introduced this code needs to disappear or
        //be made smarter.
        *features = get_legacy_features();

        features->encode(featuresbl);
        auto t(std::make_shared<MonitorDBStore::Transaction>());
        t->put(MONITOR_NAME, COMPAT_SET_LOC, featuresbl);
        store->apply_transaction(t);
    } else {
        auto it = featuresbl.cbegin();
        features->decode(it);
    }
}

void AbstractMonitor::read_features()
{
    read_features_off_disk(store, &features);
    dout(10) << "features " << features << dendl;

    calc_quorum_requirements();
    dout(10) << "required_features " << required_features << dendl;
}

void AbstractMonitor::write_features(MonitorDBStore::TransactionRef t)
{
    bufferlist bl;
    features.encode(bl);
    t->put(MONITOR_NAME, COMPAT_SET_LOC, bl);
}

const char** AbstractMonitor::get_tracked_conf_keys() const
{
    static const char* KEYS[] = {
            "crushtool", // helpful for testing
            "mon_election_timeout",
            "mon_lease",
            "mon_lease_renew_interval_factor",
            "mon_lease_ack_timeout_factor",
            "mon_accept_timeout_factor",
            // clog & admin clog
            "clog_to_monitors",
            "clog_to_syslog",
            "clog_to_syslog_facility",
            "clog_to_syslog_level",
            "clog_to_graylog",
            "clog_to_graylog_host",
            "clog_to_graylog_port",
            "mon_cluster_log_to_file",
            "host",
            "fsid",
            // periodic health to clog
            "mon_health_to_clog",
            "mon_health_to_clog_interval",
            "mon_health_to_clog_tick_interval",
            // scrub interval
            "mon_scrub_interval",
            "mon_allow_pool_delete",
            // osdmap pruning - observed, not handled.
            "mon_osdmap_full_prune_enabled",
            "mon_osdmap_full_prune_min",
            "mon_osdmap_full_prune_interval",
            "mon_osdmap_full_prune_txsize",
            // debug options - observed, not handled
            "mon_debug_extra_checks",
            "mon_debug_block_osdmap_trim",
            NULL
    };
    return KEYS;
}


int AbstractMonitor::sanitize_options()
{
    int r = 0;

    // mon_lease must be greater than mon_lease_renewal; otherwise we
    // may incur in leases expiring before they are renewed.
    if (g_conf()->mon_lease_renew_interval_factor >= 1.0) {
        clog->error() << "mon_lease_renew_interval_factor ("
                      << g_conf()->mon_lease_renew_interval_factor
                      << ") must be less than 1.0";
        r = -EINVAL;
    }

    // mon_lease_ack_timeout must be greater than mon_lease to make sure we've
    // got time to renew the lease and get an ack for it. Having both options
    // with the same value, for a given small vale, could mean timing out if
    // the monitors happened to be overloaded -- or even under normal load for
    // a small enough value.
    if (g_conf()->mon_lease_ack_timeout_factor <= 1.0) {
        clog->error() << "mon_lease_ack_timeout_factor ("
                      << g_conf()->mon_lease_ack_timeout_factor
                      << ") must be greater than 1.0";
        r = -EINVAL;
    }

    return r;
}

void AbstractMonitor::update_log_clients()
{
    clog->parse_client_options(g_ceph_context);
    audit_clog->parse_client_options(g_ceph_context);
}

void AbstractMonitor::handle_conf_change(const ConfigProxy& conf,
                                 const std::set<std::string> &changed)
{
    sanitize_options();

    dout(10) << __func__ << " " << changed << dendl;

    if (changed.count("clog_to_monitors") ||
        changed.count("clog_to_syslog") ||
        changed.count("clog_to_syslog_level") ||
        changed.count("clog_to_syslog_facility") ||
        changed.count("clog_to_graylog") ||
        changed.count("clog_to_graylog_host") ||
        changed.count("clog_to_graylog_port") ||
        changed.count("host") ||
        changed.count("fsid")) {
        update_log_clients();
    }

    if (changed.count("mon_health_to_clog") ||
        changed.count("mon_health_to_clog_interval") ||
        changed.count("mon_health_to_clog_tick_interval")) {
        finisher.queue(new C_MonContext{this, [this, changed](int) {
            std::lock_guard l{lock};
            health_to_clog_update_conf(changed);
        }});
    }

    if (changed.count("mon_scrub_interval")) {
        auto scrub_interval =
                conf.get_val<std::chrono::seconds>("mon_scrub_interval");
        finisher.queue(new C_MonContext{this, [this, scrub_interval](int) {
            std::lock_guard l{lock};
            scrub_update_interval(scrub_interval);
        }});
    }
}

/**
 * Features
 */
void AbstractMonitor::_apply_compatset_features(CompatSet &new_features)
{
    if (new_features.compare(features) != 0) {
        CompatSet diff = features.unsupported(new_features);
        dout(1) << __func__ << " enabling new quorum features: " << diff << dendl;
        features = new_features;

        auto t = std::make_shared<MonitorDBStore::Transaction>();
        write_features(t);
        store->apply_transaction(t);

        calc_quorum_requirements();
    }
}

void AbstractMonitor::apply_quorum_to_compatset_features()
{
    CompatSet new_features(features);
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES);
    if (quorum_con_features & CEPH_FEATURE_OSDMAP_ENC) {
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC);
    }
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2);
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3);
    dout(5) << __func__ << dendl;
    _apply_compatset_features(new_features);
}

void AbstractMonitor::apply_monmap_to_compatset_features()
{
    CompatSet new_features(features);
    mon_feature_t monmap_features = monmap->get_required_features();

    /* persistent monmap features may go into the compatset.
     * optional monmap features may not - why?
     *   because optional monmap features may be set/unset by the admin,
     *   and possibly by other means that haven't yet been thought out,
     *   so we can't make the monitor enforce them on start - because they
     *   may go away.
     *   this, of course, does not invalidate setting a compatset feature
     *   for an optional feature - as long as you make sure to clean it up
     *   once you unset it.
     */
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_KRAKEN)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_KRAKEN));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_KRAKEN));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_KRAKEN);
    }
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_LUMINOUS)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_LUMINOUS));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_LUMINOUS));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_LUMINOUS);
    }
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_MIMIC)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_MIMIC));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_MIMIC));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_MIMIC);
    }
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_NAUTILUS));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_NAUTILUS));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_NAUTILUS);
    }
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_OCTOPUS)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_OCTOPUS));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_OCTOPUS));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OCTOPUS);
    }
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_PACIFIC)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_PACIFIC));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_PACIFIC));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_PACIFIC);
    }
    if (monmap_features.contains_all(ceph::features::mon::FEATURE_QUINCY)) {
        ceph_assert(ceph::features::mon::get_persistent().contains_all(
                ceph::features::mon::FEATURE_QUINCY));
        // this feature should only ever be set if the quorum supports it.
        ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_QUINCY));
        new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_QUINCY);
    }

    dout(5) << __func__ << dendl;
    _apply_compatset_features(new_features);
}

void AbstractMonitor::calc_quorum_requirements()
{
    required_features = 0;

    // compatset
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC)) {
        required_features |= CEPH_FEATURE_OSDMAP_ENC;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_KRAKEN)) {
        required_features |= CEPH_FEATUREMASK_SERVER_KRAKEN;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_LUMINOUS)) {
        required_features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_MIMIC)) {
        required_features |= CEPH_FEATUREMASK_SERVER_MIMIC;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_NAUTILUS)) {
        required_features |= CEPH_FEATUREMASK_SERVER_NAUTILUS |
                             CEPH_FEATUREMASK_CEPHX_V2;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_OCTOPUS)) {
        required_features |= CEPH_FEATUREMASK_SERVER_OCTOPUS;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_PACIFIC)) {
        required_features |= CEPH_FEATUREMASK_SERVER_PACIFIC;
    }
    if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_QUINCY)) {
        required_features |= CEPH_FEATUREMASK_SERVER_QUINCY;
    }

    // monmap
    if (monmap->get_required_features().contains_all(
            ceph::features::mon::FEATURE_KRAKEN)) {
        required_features |= CEPH_FEATUREMASK_SERVER_KRAKEN;
    }
    if (monmap->get_required_features().contains_all(
            ceph::features::mon::FEATURE_LUMINOUS)) {
        required_features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
    }
    if (monmap->get_required_features().contains_all(
            ceph::features::mon::FEATURE_MIMIC)) {
        required_features |= CEPH_FEATUREMASK_SERVER_MIMIC;
    }
    if (monmap->get_required_features().contains_all(
            ceph::features::mon::FEATURE_NAUTILUS)) {
        required_features |= CEPH_FEATUREMASK_SERVER_NAUTILUS |
                             CEPH_FEATUREMASK_CEPHX_V2;
    }
    dout(10) << __func__ << " required_features " << required_features << dendl;
}

void AbstractMonitor::get_combined_feature_map(FeatureMap *fm)
{
    *fm += session_map.feature_map;
    for (auto id : quorum) {
        if (id != rank) {
            *fm += quorum_feature_map[id];
        }
    }
}

namespace {
    std::string collect_compression_algorithms()
    {
        ostringstream os;
        bool printed = false;
        for (auto [name, key] : Compressor::compression_algorithms) {
            if (printed) {
                os << ", ";
            } else {
                printed = true;
            }
            std::ignore = key;
            os << name;
        }
        return os.str();
    }
}

void AbstractMonitor::collect_metadata(Metadata *m)
{
    collect_sys_info(m, g_ceph_context);
    (*m)["addrs"] = stringify(messenger->get_myaddrs());
    (*m)["compression_algorithms"] = collect_compression_algorithms();

    // infer storage device
    string devname = store->get_devname();
    set<string> devnames;
    get_raw_devices(devname, &devnames);
    map<string,string> errs;
    get_device_metadata(devnames, m, &errs);
    for (auto& i : errs) {
        dout(1) << __func__ << " " << i.first << ": " << i.second << dendl;
    }
}



// health status to clog

void AbstractMonitor::health_tick_start()
{
    if (!cct->_conf->mon_health_to_clog ||
        cct->_conf->mon_health_to_clog_tick_interval <= 0)
        return;

    dout(15) << __func__ << dendl;

    health_tick_stop();
    health_tick_event = timer.add_event_after(
            cct->_conf->mon_health_to_clog_tick_interval,
            new C_MonContext{this, [this](int r) {
                if (r < 0)
                    return;
                health_tick_start();
            }});
}

void AbstractMonitor::health_tick_stop()
{
    dout(15) << __func__ << dendl;

    if (health_tick_event) {
        timer.cancel_event(health_tick_event);
        health_tick_event = NULL;
    }
}

ceph::real_clock::time_point AbstractMonitor::health_interval_calc_next_update()
{
    auto now = ceph::real_clock::now();

    auto secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
    int remainder = secs.count() % cct->_conf->mon_health_to_clog_interval;
    int adjustment = cct->_conf->mon_health_to_clog_interval - remainder;
    auto next = secs + std::chrono::seconds(adjustment);

    dout(20) << __func__
             << " now: " << now << ","
             << " next: " << next << ","
             << " interval: " << cct->_conf->mon_health_to_clog_interval
             << dendl;

    return ceph::real_clock::time_point{next};
}

void AbstractMonitor::health_interval_start()
{
    dout(15) << __func__ << dendl;

    if (!cct->_conf->mon_health_to_clog ||
        cct->_conf->mon_health_to_clog_interval <= 0) {
        return;
    }

    health_interval_stop();
    auto next = health_interval_calc_next_update();
    health_interval_event = new C_MonContext{this, [this](int r) {
        if (r < 0)
            return;
        do_health_to_clog_interval();
    }};
    if (!timer.add_event_at(next, health_interval_event)) {
        health_interval_event = nullptr;
    }
}

void AbstractMonitor::health_interval_stop()
{
    dout(15) << __func__ << dendl;
    if (health_interval_event) {
        timer.cancel_event(health_interval_event);
    }
    health_interval_event = NULL;
}

void AbstractMonitor::health_events_cleanup()
{
    health_tick_stop();
    health_interval_stop();
    health_status_cache.reset();
}

void AbstractMonitor::health_to_clog_update_conf(const std::set<std::string> &changed)
{
    dout(20) << __func__ << dendl;

    if (changed.count("mon_health_to_clog")) {
        if (!cct->_conf->mon_health_to_clog) {
            health_events_cleanup();
            return;
        } else {
            if (!health_tick_event) {
                health_tick_start();
            }
            if (!health_interval_event) {
                health_interval_start();
            }
        }
    }

    if (changed.count("mon_health_to_clog_interval")) {
        if (cct->_conf->mon_health_to_clog_interval <= 0) {
            health_interval_stop();
        } else {
            health_interval_start();
        }
    }

    if (changed.count("mon_health_to_clog_tick_interval")) {
        if (cct->_conf->mon_health_to_clog_tick_interval <= 0) {
            health_tick_stop();
        } else {
            health_tick_start();
        }
    }
}

void AbstractMonitor::do_health_to_clog_interval()
{
    // outputting to clog may have been disabled in the conf
    // since we were scheduled.
    if (!cct->_conf->mon_health_to_clog ||
        cct->_conf->mon_health_to_clog_interval <= 0)
        return;

    dout(10) << __func__ << dendl;

    // do we have a cached value for next_clog_update?  if not,
    // do we know when the last update was?

    do_health_to_clog(true);
    health_interval_start();
}

void AbstractMonitor::do_health_to_clog(bool force)
{
    // outputting to clog may have been disabled in the conf
    // since we were scheduled.
    if (!cct->_conf->mon_health_to_clog ||
        cct->_conf->mon_health_to_clog_interval <= 0)
        return;

    dout(10) << __func__ << (force ? " (force)" : "") << dendl;

    string summary;
    health_status_t level = healthmon()->get_health_status(false, nullptr, &summary);
    if (!force &&
        summary == health_status_cache.summary &&
        level == health_status_cache.overall)
        return;

    if (g_conf()->mon_health_detail_to_clog &&
        summary != health_status_cache.summary &&
        level != HEALTH_OK) {
        string details;
        level = healthmon()->get_health_status(true, nullptr, &details);
        clog->health(level) << "Health detail: " << details;
    } else {
        clog->health(level) << "overall " << summary;
    }
    health_status_cache.summary = summary;
    health_status_cache.overall = level;
}

void AbstractMonitor::log_health(
        const health_check_map_t& updated,
        const health_check_map_t& previous,
        MonitorDBStore::TransactionRef t)
{
    if (!g_conf()->mon_health_to_clog) {
        return;
    }

    const utime_t now = ceph_clock_now();

    // FIXME: log atomically as part of @t instead of using clog.
    dout(10) << __func__ << " updated " << updated.checks.size()
             << " previous " << previous.checks.size()
             << dendl;
    const auto min_log_period = g_conf().get_val<int64_t>(
            "mon_health_log_update_period");
    for (auto& p : updated.checks) {
        auto q = previous.checks.find(p.first);
        bool logged = false;
        if (q == previous.checks.end()) {
            // new
            ostringstream ss;
            ss << "Health check failed: " << p.second.summary << " ("
               << p.first << ")";
            clog->health(p.second.severity) << ss.str();

            logged = true;
        } else {
            if (p.second.summary != q->second.summary ||
                p.second.severity != q->second.severity) {

                auto status_iter = health_check_log_times.find(p.first);
                if (status_iter != health_check_log_times.end()) {
                    if (p.second.severity == q->second.severity &&
                        now - status_iter->second.updated_at < min_log_period) {
                        // We already logged this recently and the severity is unchanged,
                        // so skip emitting an update of the summary string.
                        // We'll get an update out of tick() later if the check
                        // is still failing.
                        continue;
                    }
                }

                // summary or severity changed (ignore detail changes at this level)
                ostringstream ss;
                ss << "Health check update: " << p.second.summary << " (" << p.first << ")";
                clog->health(p.second.severity) << ss.str();

                logged = true;
            }
        }
        // Record the time at which we last logged, so that we can check this
        // when considering whether/when to print update messages.
        if (logged) {
            auto iter = health_check_log_times.find(p.first);
            if (iter == health_check_log_times.end()) {
                health_check_log_times.emplace(p.first, HealthCheckLogStatus(
                        p.second.severity, p.second.summary, now));
            } else {
                iter->second = HealthCheckLogStatus(
                        p.second.severity, p.second.summary, now);
            }
        }
    }
    for (auto& p : previous.checks) {
        if (!updated.checks.count(p.first)) {
            // cleared
            ostringstream ss;
            if (p.first == "DEGRADED_OBJECTS") {
                clog->info() << "All degraded objects recovered";
            } else if (p.first == "OSD_FLAGS") {
                clog->info() << "OSD flags cleared";
            } else {
                clog->info() << "Health check cleared: " << p.first << " (was: "
                             << p.second.summary << ")";
            }

            if (health_check_log_times.count(p.first)) {
                health_check_log_times.erase(p.first);
            }
        }
    }

    if (previous.checks.size() && updated.checks.size() == 0) {
        // We might be going into a fully healthy state, check
        // other subsystems
        bool any_checks = false;
        //TODO: Update this to not use paxos service, but instead just regular service
        for (auto& svc : paxos_service) {
            if (&(svc->get_health_checks()) == &(previous)) {
                // Ignore the ones we're clearing right now
                continue;
            }

            if (svc->get_health_checks().checks.size() > 0) {
                any_checks = true;
                break;
            }
        }
        if (!any_checks) {
            clog->info() << "Cluster is now healthy";
        }
    }
}

void AbstractMonitor::_generate_command_map(cmdmap_t& cmdmap,
                                    map<string,string> &param_str_map)
{
    for (auto p = cmdmap.begin(); p != cmdmap.end(); ++p) {
        if (p->first == "prefix")
            continue;
        if (p->first == "caps") {
            vector<string> cv;
            if (cmd_getval(cmdmap, "caps", cv) &&
                cv.size() % 2 == 0) {
                for (unsigned i = 0; i < cv.size(); i += 2) {
                    string k = string("caps_") + cv[i];
                    param_str_map[k] = cv[i + 1];
                }
                continue;
            }
        }
        param_str_map[p->first] = cmd_vartype_stringify(p->second);
    }
}

const MonCommand *AbstractMonitor::_get_moncommand(
        const string &cmd_prefix,
        const vector<MonCommand>& cmds)
{
    for (auto& c : cmds) {
        if (c.cmdstring.compare(0, cmd_prefix.size(), cmd_prefix) == 0) {
            return &c;
        }
    }
    return nullptr;
}

bool AbstractMonitor::_allowed_command(MonSession *s, const string &module,
                               const string &prefix, const cmdmap_t& cmdmap,
                               const map<string,string>& param_str_map,
                               const MonCommand *this_cmd) {

    bool cmd_r = this_cmd->requires_perm('r');
    bool cmd_w = this_cmd->requires_perm('w');
    bool cmd_x = this_cmd->requires_perm('x');

    bool capable = s->caps.is_capable(
            g_ceph_context,
            s->entity_name,
            module, prefix, param_str_map,
            cmd_r, cmd_w, cmd_x,
            s->get_peer_socket_addr());

    dout(10) << __func__ << " " << (capable ? "" : "not ") << "capable" << dendl;
    return capable;
}


struct C_MgrProxyCommand : public Context {
    AbstractMonitor *mon;
    MonOpRequestRef op;
    uint64_t size;
    bufferlist outbl;
    string outs;
    C_MgrProxyCommand(AbstractMonitor *mon, MonOpRequestRef op, uint64_t s)
            : mon(mon), op(op), size(s) { }
    void finish(int r) {
        std::lock_guard l(mon->lock);
        mon->mgr_proxy_bytes -= size;
        mon->reply_command(op, r, outs, outbl, 0);
    }
};

void AbstractMonitor::handle_tell_command(MonOpRequestRef op)
{
    ceph_assert(op->is_type_command());
    MCommand *m = static_cast<MCommand*>(op->get_req());
    if (m->fsid != monmap->fsid) {
        dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
        return reply_tell_command(op, -EACCES, "wrong fsid");
    }
    MonSession *session = op->get_session();
    if (!session) {
        dout(5) << __func__ << " dropping stray message " << *m << dendl;
        return;
    }
    cmdmap_t cmdmap;
    if (stringstream ss; !cmdmap_from_json(m->cmd, &cmdmap, ss)) {
        return reply_tell_command(op, -EINVAL, ss.str());
    }
    map<string,string> param_str_map;
    _generate_command_map(cmdmap, param_str_map);
    string prefix;
    if (!cmd_getval(cmdmap, "prefix", prefix)) {
        return reply_tell_command(op, -EINVAL, "no prefix");
    }
    if (auto cmd = _get_moncommand(prefix,
                                   get_local_commands(quorum_mon_features));
            cmd) {
        if (cmd->is_obsolete() ||
            (cct->_conf->mon_debug_deprecated_as_obsolete &&
             cmd->is_deprecated())) {
            return reply_tell_command(op, -ENOTSUP,
                                      "command is obsolete; "
                                      "please check usage and/or man page");
        }
    }
    // see if command is allowed
    if (!session->caps.is_capable(
            g_ceph_context,
            session->entity_name,
            "mon", prefix, param_str_map,
            true, true, true,
            session->get_peer_socket_addr())) {
        return reply_tell_command(op, -EACCES, "insufficient caps");
    }
    // pass it to asok
    cct->get_admin_socket()->queue_tell_command(m);
}

void AbstractMonitor::reply_command(MonOpRequestRef op, int rc, const string &rs, version_t version)
{
    bufferlist rdata;
    reply_command(op, rc, rs, rdata, version);
}

void AbstractMonitor::reply_command(MonOpRequestRef op, int rc, const string &rs,
                            bufferlist& rdata, version_t version)
{
    auto m = op->get_req<MMonCommand>();
    ceph_assert(m->get_type() == MSG_MON_COMMAND);
    MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
    reply->set_tid(m->get_tid());
    reply->set_data(rdata);
    send_reply(op, reply);
}

void AbstractMonitor::reply_tell_command(
        MonOpRequestRef op, int rc, const string &rs)
{
    MCommand *m = static_cast<MCommand*>(op->get_req());
    ceph_assert(m->get_type() == MSG_COMMAND);
    MCommandReply *reply = new MCommandReply(rc, rs);
    reply->set_tid(m->get_tid());
    m->get_connection()->send_message(reply);
}

vector<DaemonHealthMetric> AbstractMonitor::get_health_metrics()
{
    vector<DaemonHealthMetric> metrics;

    utime_t oldest_secs;
    const utime_t now = ceph_clock_now();
    auto too_old = now;
    too_old -= g_conf().get_val<std::chrono::seconds>("mon_op_complaint_time").count();
    int slow = 0;
    TrackedOpRef oldest_op;
    auto count_slow_ops = [&](TrackedOp& op) {
        if (op.get_initiated() < too_old) {
            slow++;
            if (!oldest_op || op.get_initiated() < oldest_op->get_initiated()) {
                oldest_op = &op;
            }
            return true;
        } else {
            return false;
        }
    };
    if (op_tracker.visit_ops_in_flight(&oldest_secs, count_slow_ops)) {
        if (slow) {
            derr << __func__ << " reporting " << slow << " slow ops, oldest is "
                 << oldest_op->get_desc() << dendl;
        }
        metrics.emplace_back(daemon_metric::SLOW_OPS, slow, oldest_secs);
    } else {
        metrics.emplace_back(daemon_metric::SLOW_OPS, 0, 0);
    }
    return metrics;
}


int AbstractMonitor::check_fsid()
{
    bufferlist ebl;
    int r = store->get(MONITOR_NAME, "cluster_uuid", ebl);
    if (r == -ENOENT)
        return r;
    ceph_assert(r == 0);

    string es(ebl.c_str(), ebl.length());

    // only keep the first line
    size_t pos = es.find_first_of('\n');
    if (pos != string::npos)
        es.resize(pos);

    dout(10) << "check_fsid cluster_uuid contains '" << es << "'" << dendl;
    uuid_d ondisk;
    if (!ondisk.parse(es.c_str())) {
        derr << "error: unable to parse uuid" << dendl;
        return -EINVAL;
    }

    if (monmap->get_fsid() != ondisk) {
        derr << "error: cluster_uuid file exists with value " << ondisk
             << ", != our uuid " << monmap->get_fsid() << dendl;
        return -EEXIST;
    }

    return 0;
}

int AbstractMonitor::write_fsid()
{
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    write_fsid(t);
    int r = store->apply_transaction(t);
    return r;
}

int AbstractMonitor::write_fsid(MonitorDBStore::TransactionRef t)
{
    ostringstream ss;
    ss << monmap->get_fsid() << "\n";
    string us = ss.str();

    bufferlist b;
    b.append(us);

    t->put(MONITOR_NAME, "cluster_uuid", b);
    return 0;
}

/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int AbstractMonitor::mkfs(bufferlist& osdmapbl)
{
    auto t(std::make_shared<MonitorDBStore::Transaction>());

    // verify cluster fsid
    int r = check_fsid();
    if (r < 0 && r != -ENOENT)
        return r;

    bufferlist magicbl;
    magicbl.append(CEPH_MON_ONDISK_MAGIC);
    magicbl.append("\n");
    t->put(MONITOR_NAME, "magic", magicbl);


    features = get_initial_supported_features();
    write_features(t);

    // save monmap, osdmap, keyring.
    bufferlist monmapbl;
    monmap->encode(monmapbl, CEPH_FEATURES_ALL);
    monmap->set_epoch(0);     // must be 0 to avoid confusing first MonmapMonitor::update_from_paxos()
    t->put("mkfs", "monmap", monmapbl);

    if (osdmapbl.length()) {
        // make sure it's a valid osdmap
        try {
            OSDMap om;
            om.decode(osdmapbl);
        }
        catch (ceph::buffer::error& e) {
            derr << "error decoding provided osdmap: " << e.what() << dendl;
            return -EINVAL;
        }
        t->put("mkfs", "osdmap", osdmapbl);
    }

    if (is_keyring_required()) {
        KeyRing keyring;
        string keyring_filename;

        r = ceph_resolve_file_search(g_conf()->keyring, keyring_filename);
        if (r) {
            if (g_conf()->key != "") {
                string keyring_plaintext = "[mon.]\n\tkey = " + g_conf()->key +
                                           "\n\tcaps mon = \"allow *\"\n";
                bufferlist bl;
                bl.append(keyring_plaintext);
                try {
                    auto i = bl.cbegin();
                    keyring.decode(i);
                }
                catch (const ceph::buffer::error& e) {
                    derr << "error decoding keyring " << keyring_plaintext
                         << ": " << e.what() << dendl;
                    return -EINVAL;
                }
            } else {
                derr << "unable to find a keyring on " << g_conf()->keyring
                     << ": " << cpp_strerror(r) << dendl;
                return r;
            }
        } else {
            r = keyring.load(g_ceph_context, keyring_filename);
            if (r < 0) {
                derr << "unable to load initial keyring " << g_conf()->keyring << dendl;
                return r;
            }
        }

        // put mon. key in external keyring; seed with everything else.
        extract_save_mon_key(keyring);

        bufferlist keyringbl;
        keyring.encode_plaintext(keyringbl);
        t->put("mkfs", "keyring", keyringbl);
    }
    write_fsid(t);
    store->apply_transaction(t);

    return 0;
}

int AbstractMonitor::write_default_keyring(bufferlist& bl)
{
    ostringstream os;
    os << g_conf()->mon_data << "/keyring";

    int err = 0;
    int fd = ::open(os.str().c_str(), O_WRONLY|O_CREAT|O_CLOEXEC, 0600);
    if (fd < 0) {
        err = -errno;
        dout(0) << __func__ << " failed to open " << os.str()
                << ": " << cpp_strerror(err) << dendl;
        return err;
    }

    err = bl.write_fd(fd);
    if (!err)
        ::fsync(fd);
    VOID_TEMP_FAILURE_RETRY(::close(fd));

    return err;
}

void AbstractMonitor::extract_save_mon_key(KeyRing& keyring)
{
    EntityName mon_name;
    mon_name.set_type(CEPH_ENTITY_TYPE_MON);
    EntityAuth mon_key;
    if (keyring.get_auth(mon_name, mon_key)) {
        dout(10) << "extract_save_mon_key moving mon. key to separate keyring" << dendl;
        KeyRing pkey;
        pkey.add(mon_name, mon_key);
        bufferlist bl;
        pkey.encode_plaintext(bl);
        write_default_keyring(bl);
        keyring.remove(mon_name);
    }
}

// AuthClient methods -- for mon <-> mon communication
int AbstractMonitor::get_auth_request(
        Connection *con,
        AuthConnectionMeta *auth_meta,
        uint32_t *method,
        vector<uint32_t> *preferred_modes,
        bufferlist *out)
{
    std::scoped_lock l(auth_lock);
    if (con->get_peer_type() != CEPH_ENTITY_TYPE_MON &&
        con->get_peer_type() != CEPH_ENTITY_TYPE_MGR) {
        return -EACCES;
    }
    AuthAuthorizer *auth;
    if (!get_authorizer(con->get_peer_type(), &auth)) {
        return -EACCES;
    }
    auth_meta->authorizer.reset(auth);
    auth_registry.get_supported_modes(con->get_peer_type(),
                                      auth->protocol,
                                      preferred_modes);
    *method = auth->protocol;
    *out = auth->bl;
    return 0;
}

int AbstractMonitor::handle_auth_reply_more(
        Connection *con,
        AuthConnectionMeta *auth_meta,
        const bufferlist& bl,
        bufferlist *reply)
{
    std::scoped_lock l(auth_lock);
    if (!auth_meta->authorizer) {
        derr << __func__ << " no authorizer?" << dendl;
        return -EACCES;
    }
    auth_meta->authorizer->add_challenge(cct, bl);
    *reply = auth_meta->authorizer->bl;
    return 0;
}

int AbstractMonitor::handle_auth_done(
        Connection *con,
        AuthConnectionMeta *auth_meta,
        uint64_t global_id,
        uint32_t con_mode,
        const bufferlist& bl,
        CryptoKey *session_key,
        std::string *connection_secret)
{
    std::scoped_lock l(auth_lock);
    // verify authorizer reply
    auto p = bl.begin();
    if (!auth_meta->authorizer->verify_reply(p, connection_secret)) {
        dout(0) << __func__ << " failed verifying authorizer reply" << dendl;
        return -EACCES;
    }
    auth_meta->session_key = auth_meta->authorizer->session_key;
    return 0;
}

int AbstractMonitor::handle_auth_bad_method(
        Connection *con,
        AuthConnectionMeta *auth_meta,
        uint32_t old_auth_method,
        int result,
        const std::vector<uint32_t>& allowed_methods,
        const std::vector<uint32_t>& allowed_modes)
{
    derr << __func__ << " hmm, they didn't like " << old_auth_method
         << " result " << cpp_strerror(result) << dendl;
    return -EACCES;
}

bool AbstractMonitor::get_authorizer(int service_id, AuthAuthorizer **authorizer)
{
    dout(10) << "get_authorizer for " << ceph_entity_type_name(service_id)
             << dendl;

    if (is_shutdown())
        return false;

    // we only connect to other monitors and mgr; every else connects to us.
    if (service_id != CEPH_ENTITY_TYPE_MON &&
        service_id != CEPH_ENTITY_TYPE_MGR)
        return false;

    if (!auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX)) {
        // auth_none
        dout(20) << __func__ << " building auth_none authorizer" << dendl;
        AuthNoneClientHandler handler{g_ceph_context};
        handler.set_global_id(0);
        *authorizer = handler.build_authorizer(service_id);
        return true;
    }

    CephXServiceTicketInfo auth_ticket_info;
    CephXSessionAuthInfo info;
    int ret;

    EntityName name;
    name.set_type(CEPH_ENTITY_TYPE_MON);
    auth_ticket_info.ticket.name = name;
    auth_ticket_info.ticket.global_id = 0;

    if (service_id == CEPH_ENTITY_TYPE_MON) {
        // mon to mon authentication uses the private monitor shared key and not the
        // rotating key
        CryptoKey secret;
        if (!keyring.get_secret(name, secret) &&
            !key_server.get_secret(name, secret)) {
            dout(0) << " couldn't get secret for mon service from keyring or keyserver"
                    << dendl;
            stringstream ss, ds;
            int err = key_server.list_secrets(ds);
            if (err < 0)
                ss << "no installed auth entries!";
            else
                ss << "installed auth entries:";
            dout(0) << ss.str() << "\n" << ds.str() << dendl;
            return false;
        }

        ret = key_server.build_session_auth_info(
                service_id, auth_ticket_info.ticket, secret, (uint64_t)-1, info);
        if (ret < 0) {
            dout(0) << __func__ << " failed to build mon session_auth_info "
                    << cpp_strerror(ret) << dendl;
            return false;
        }
    } else if (service_id == CEPH_ENTITY_TYPE_MGR) {
        // mgr
        ret = key_server.build_session_auth_info(
                service_id, auth_ticket_info.ticket, info);
        if (ret < 0) {
            derr << __func__ << " failed to build mgr service session_auth_info "
                 << cpp_strerror(ret) << dendl;
            return false;
        }
    } else {
        ceph_abort();  // see check at top of fn
    }

    CephXTicketBlob blob;
    if (!cephx_build_service_ticket_blob(cct, info, blob)) {
        dout(0) << "get_authorizer failed to build service ticket" << dendl;
        return false;
    }
    bufferlist ticket_data;
    encode(blob, ticket_data);

    auto iter = ticket_data.cbegin();
    CephXTicketHandler handler(g_ceph_context, service_id);
    decode(handler.ticket, iter);

    handler.session_key = info.session_key;

    *authorizer = handler.build_authorizer(0);

    return true;
}

int AbstractMonitor::handle_auth_request(
        Connection *con,
        AuthConnectionMeta *auth_meta,
        bool more,
        uint32_t auth_method,
        const bufferlist &payload,
        bufferlist *reply)
{
    std::scoped_lock l(auth_lock);

    // NOTE: be careful, the Connection hasn't fully negotiated yet, so
    // e.g., peer_features, peer_addrs, and others are still unknown.

    dout(10) << __func__ << " con " << con << (more ? " (more)":" (start)")
             << " method " << auth_method
             << " payload " << payload.length()
             << dendl;
    if (!payload.length()) {
        if (!con->is_msgr2() &&
            con->get_peer_type() != CEPH_ENTITY_TYPE_MON) {
            // for v1 connections, we tolerate no authorizer (from
            // non-monitors), because authentication happens via MAuth
            // messages.
            return 1;
        }
        return -EACCES;
    }
    if (!more) {
        auth_meta->auth_mode = payload[0];
    }

    if (auth_meta->auth_mode >= AUTH_MODE_AUTHORIZER &&
        auth_meta->auth_mode <= AUTH_MODE_AUTHORIZER_MAX) {
        AuthAuthorizeHandler *ah = get_auth_authorize_handler(con->get_peer_type(),
                                                              auth_method);
        if (!ah) {
            lderr(cct) << __func__ << " no AuthAuthorizeHandler found for auth method "
                       << auth_method << dendl;
            return -EOPNOTSUPP;
        }
        bool was_challenge = (bool)auth_meta->authorizer_challenge;
        bool isvalid = ah->verify_authorizer(
                cct,
                keyring,
                payload,
                auth_meta->get_connection_secret_length(),
                reply,
                &con->peer_name,
                &con->peer_global_id,
                &con->peer_caps_info,
                &auth_meta->session_key,
                &auth_meta->connection_secret,
                &auth_meta->authorizer_challenge);
        if (isvalid) {
            ms_handle_authentication(con);
            return 1;
        }
        if (!more && !was_challenge && auth_meta->authorizer_challenge) {
            return 0;
        }
        dout(10) << __func__ << " bad authorizer on " << con << dendl;
        return -EACCES;
    } else if (auth_meta->auth_mode < AUTH_MODE_MON ||
               auth_meta->auth_mode > AUTH_MODE_MON_MAX) {
        derr << __func__ << " unrecognized auth mode " << auth_meta->auth_mode
             << dendl;
        return -EACCES;
    }

    // wait until we've formed an initial quorum on mkfs so that we have
    // the initial keys (e.g., client.admin).
    if (authmon()->get_last_committed() == 0) {
        dout(10) << __func__ << " haven't formed initial quorum, EBUSY" << dendl;
        return -EBUSY;
    }

    RefCountedPtr priv;
    MonSession *s;
    int32_t r = 0;
    auto p = payload.begin();
    if (!more) {
        if (con->get_priv()) {
            return -EACCES; // wtf
        }

        // handler?
        unique_ptr<AuthServiceHandler> auth_handler{get_auth_service_handler(
                auth_method, g_ceph_context, &key_server)};
        if (!auth_handler) {
            dout(1) << __func__ << " auth_method " << auth_method << " not supported"
                    << dendl;
            return -EOPNOTSUPP;
        }

        uint8_t mode;
        EntityName entity_name;

        try {
            decode(mode, p);
            if (mode < AUTH_MODE_MON ||
                mode > AUTH_MODE_MON_MAX) {
                dout(1) << __func__ << " invalid mode " << (int)mode << dendl;
                return -EACCES;
            }
            assert(mode >= AUTH_MODE_MON && mode <= AUTH_MODE_MON_MAX);
            decode(entity_name, p);
            decode(con->peer_global_id, p);
        } catch (ceph::buffer::error& e) {
            dout(1) << __func__ << " failed to decode, " << e.what() << dendl;
            return -EACCES;
        }

        // supported method?
        if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
            entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
            entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
            entity_name.get_type() == CEPH_ENTITY_TYPE_MGR) {
            if (!auth_cluster_required.is_supported_auth(auth_method)) {
                dout(10) << __func__ << " entity " << entity_name << " method "
                         << auth_method << " not among supported "
                         << auth_cluster_required.get_supported_set() << dendl;
                return -EOPNOTSUPP;
            }
        } else {
            if (!auth_service_required.is_supported_auth(auth_method)) {
                dout(10) << __func__ << " entity " << entity_name << " method "
                         << auth_method << " not among supported "
                         << auth_cluster_required.get_supported_set() << dendl;
                return -EOPNOTSUPP;
            }
        }

        // for msgr1 we would do some weirdness here to ensure signatures
        // are supported by the client if we require it.  for msgr2 that
        // is not necessary.

        bool is_new_global_id = false;
        if (!con->peer_global_id) {
            con->peer_global_id = authmon()->_assign_global_id();
            if (!con->peer_global_id) {
                dout(1) << __func__ << " failed to assign global_id" << dendl;
                return -EBUSY;
            }
            is_new_global_id = true;
        }

        // set up partial session
        s = new MonSession(con);
        s->auth_handler = auth_handler.release();
        con->set_priv(RefCountedPtr{s, false});

        r = s->auth_handler->start_session(
                entity_name,
                con->peer_global_id,
                is_new_global_id,
                reply,
                &con->peer_caps_info);
    } else {
        priv = con->get_priv();
        if (!priv) {
            // this can happen if the async ms_handle_reset event races with
            // the unlocked call into handle_auth_request
            return -EACCES;
        }
        s = static_cast<MonSession*>(priv.get());
        r = s->auth_handler->handle_request(
                p,
                auth_meta->get_connection_secret_length(),
                reply,
                &con->peer_caps_info,
                &auth_meta->session_key,
                &auth_meta->connection_secret);
    }
    if (r > 0 &&
        !s->authenticated) {
        ms_handle_authentication(con);
    }

    dout(30) << " r " << r << " reply:\n";
            reply->hexdump(*_dout);
            *_dout << dendl;
    return r;
}

void AbstractMonitor::ms_handle_accept(Connection *con)
{
    auto priv = con->get_priv();
    MonSession *s = static_cast<MonSession*>(priv.get());
    if (!s) {
        // legacy protocol v1?
        dout(10) << __func__ << " con " << con << " no session" << dendl;
        return;
    }

    if (s->item.is_on_list()) {
        dout(10) << __func__ << " con " << con << " session " << s
                 << " already on list" << dendl;
    } else {
        std::lock_guard l(session_map_lock);
        if (state == STATE_SHUTDOWN) {
            dout(10) << __func__ << " ignoring new con " << con << " (shutdown)" << dendl;
            con->mark_down();
            return;
        }
        dout(10) << __func__ << " con " << con << " session " << s
                 << " registering session for "
                 << con->get_peer_addrs() << dendl;
        s->_ident(entity_name_t(con->get_peer_type(), con->get_peer_id()),
                  con->get_peer_addrs());
        session_map.add_session(s);
    }
}

int AbstractMonitor::ms_handle_authentication(Connection *con)
{
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
        // mon <-> mon connections need no Session, and setting one up
        // creates an awkward ref cycle between Session and Connection.
        return 1;
    }

    auto priv = con->get_priv();
    MonSession *s = static_cast<MonSession*>(priv.get());
    if (!s) {
        // must be msgr2, otherwise dispatch would have set up the session.
        s = session_map.new_session(
                entity_name_t(con->get_peer_type(), -1),  // we don't know yet
                con->get_peer_addrs(),
                con);
        assert(s);
        dout(10) << __func__ << " adding session " << s << " to con " << con
                 << dendl;
        con->set_priv(s);
        logger->set(l_mon_num_sessions, session_map.get_size());
        logger->inc(l_mon_session_add);
    }
    dout(10) << __func__ << " session " << s << " con " << con
             << " addr " << s->con->get_peer_addr()
             << " " << *s << dendl;

    AuthCapsInfo &caps_info = con->get_peer_caps_info();
    int ret = 0;
    if (caps_info.allow_all) {
        s->caps.set_allow_all();
        s->authenticated = true;
        ret = 1;
    } else if (caps_info.caps.length()) {
        bufferlist::const_iterator p = caps_info.caps.cbegin();
        string str;
        try {
            decode(str, p);
        } catch (const ceph::buffer::error &err) {
            derr << __func__ << " corrupt cap data for " << con->get_peer_entity_name()
                 << " in auth db" << dendl;
            str.clear();
            ret = -EACCES;
        }
        if (ret >= 0) {
            if (s->caps.parse(str, NULL)) {
                s->authenticated = true;
                ret = 1;
            } else {
                derr << __func__ << " unparseable caps '" << str << "' for "
                     << con->get_peer_entity_name() << dendl;
                ret = -EACCES;
            }
        }
    }

    return ret;
}

bool AbstractMonitor::ms_handle_reset(Connection *con)
{
    dout(10) << "ms_handle_reset " << con << " " << con->get_peer_addr() << dendl;

    // ignore lossless monitor sessions
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
        return false;

    auto priv = con->get_priv();
    auto s = static_cast<MonSession*>(priv.get());
    if (!s)
        return false;

    // break any con <-> session ref cycle
    s->con->set_priv(nullptr);

    if (is_shutdown())
        return false;

    std::lock_guard l(lock);

    dout(10) << "reset/close on session " << s->name << " " << s->addrs << dendl;
    if (!s->closed && s->item.is_on_list()) {
        std::lock_guard l(session_map_lock);
        remove_session(s);
    }
    return true;
}

bool AbstractMonitor::ms_handle_refused(Connection *con)
{
    // just log for now...
    dout(10) << "ms_handle_refused " << con << " " << con->get_peer_addr() << dendl;
    return false;
}

int AbstractMonitor::load_metadata()
{
    bufferlist bl;
    int r = store->get(MONITOR_STORE_PREFIX, "last_metadata", bl);
    if (r)
        return r;
    auto it = bl.cbegin();
    decode(mon_metadata, it);

    pending_metadata = mon_metadata;
    return 0;
}

void AbstractMonitor::count_metadata(const string& field, map<string,int> *out)
{
    for (auto& p : mon_metadata) {
        auto q = p.second.find(field);
        if (q == p.second.end()) {
            (*out)["unknown"]++;
        } else {
            (*out)[q->second]++;
        }
    }
}

void AbstractMonitor::count_metadata(const string& field, Formatter *f)
{
    map<string,int> by_val;
    count_metadata(field, &by_val);
    f->open_object_section(field.c_str());
    for (auto& p : by_val) {
        f->dump_int(p.first.c_str(), p.second);
    }
    f->close_section();
}

void AbstractMonitor::get_all_versions(std::map<string, list<string> > &versions)
{
    // mon
    get_versions(versions);
    // osd
    osdmon()->get_versions(versions);
    // mgr
    mgrmon()->get_versions(versions);
    // mds
    mdsmon()->get_versions(versions);
    dout(20) << __func__ << " all versions=" << versions << dendl;
}

void AbstractMonitor::get_versions(std::map<string, list<string> > &versions)
{
    for (auto& [rank, metadata] : mon_metadata) {
        auto q = metadata.find("ceph_version_short");
        if (q == metadata.end()) {
            // not likely
            continue;
        }
        versions[q->second].push_back(string("mon.") + monmap->get_name(rank));
    }
}

void AbstractMonitor::register_cluster_logger()
{
    if (!cluster_logger_registered) {
        dout(10) << "register_cluster_logger" << dendl;
        cluster_logger_registered = true;
        cct->get_perfcounters_collection()->add(cluster_logger);
    } else {
        dout(10) << "register_cluster_logger - already registered" << dendl;
    }
}

void AbstractMonitor::unregister_cluster_logger()
{
    if (cluster_logger_registered) {
        dout(10) << "unregister_cluster_logger" << dendl;
        cluster_logger_registered = false;
        cct->get_perfcounters_collection()->remove(cluster_logger);
    } else {
        dout(10) << "unregister_cluster_logger - not registered" << dendl;
    }
}


void AbstractMonitor::timecheck_start()
{
    dout(10) << __func__ << dendl;
    timecheck_cleanup();
    if (get_quorum_mon_features().contains_all(
            ceph::features::mon::FEATURE_NAUTILUS)) {
        timecheck_start_round();
    }
}

void AbstractMonitor::timecheck_finish()
{
    dout(10) << __func__ << dendl;
    timecheck_cleanup();
}

void AbstractMonitor::timecheck_start_round()
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

void AbstractMonitor::timecheck_finish_round(bool success)
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

void AbstractMonitor::timecheck_cancel_round()
{
    timecheck_finish_round(false);
}

void AbstractMonitor::timecheck_cleanup()
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

void AbstractMonitor::timecheck_reset_event()
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

void AbstractMonitor::timecheck_check_skews()
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

void AbstractMonitor::timecheck_report()
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

void AbstractMonitor::timecheck()
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

health_status_t AbstractMonitor::timecheck_status(ostringstream &ss,
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

void AbstractMonitor::handle_subscribe(MonOpRequestRef op)
{
    auto m = op->get_req<MMonSubscribe>();
    dout(10) << "handle_subscribe " << *m << dendl;

    bool reply = false;

    MonSession *s = op->get_session();
    ceph_assert(s);

    if (m->hostname.size()) {
        s->remote_host = m->hostname;
    }

    for (map<string,ceph_mon_subscribe_item>::iterator p = m->what.begin();
         p != m->what.end();
         ++p) {
        if (p->first == "monmap" || p->first == "config") {
            // these require no caps
        } else if (!s->is_capable("mon", MON_CAP_R)) {
            dout(5) << __func__ << " " << op->get_req()->get_source_inst()
                    << " not enough caps for " << *(op->get_req()) << " -- dropping"
                    << dendl;
            continue;
        }

        // if there are any non-onetime subscriptions, we need to reply to start the resubscribe timer
        if ((p->second.flags & CEPH_SUBSCRIBE_ONETIME) == 0)
            reply = true;

        // remove conflicting subscribes
        if (logmon()->sub_name_to_id(p->first) >= 0) {
            for (map<string, Subscription*>::iterator it = s->sub_map.begin();
                 it != s->sub_map.end(); ) {
                if (it->first != p->first && logmon()->sub_name_to_id(it->first) >= 0) {
                    std::lock_guard l(session_map_lock);
                    session_map.remove_sub((it++)->second);
                } else {
                    ++it;
                }
            }
        }

        {
            std::lock_guard l(session_map_lock);
            session_map.add_update_sub(s, p->first, p->second.start,
                                       p->second.flags & CEPH_SUBSCRIBE_ONETIME,
                                       m->get_connection()->has_feature(CEPH_FEATURE_INCSUBOSDMAP));
        }

        if (p->first.compare(0, 6, "mdsmap") == 0 || p->first.compare(0, 5, "fsmap") == 0) {
            dout(10) << __func__ << ": MDS sub '" << p->first << "'" << dendl;
            if ((int)s->is_capable("mds", MON_CAP_R)) {
                Subscription *sub = s->sub_map[p->first];
                ceph_assert(sub != nullptr);
                mdsmon()->check_sub(sub);
            }
        } else if (p->first == "osdmap") {
            if ((int)s->is_capable("osd", MON_CAP_R)) {
                if (s->osd_epoch > p->second.start) {
                    // client needs earlier osdmaps on purpose, so reset the sent epoch
                    s->osd_epoch = 0;
                }
                osdmon()->check_osdmap_sub(s->sub_map["osdmap"]);
            }
        } else if (p->first == "osd_pg_creates") {
            if ((int)s->is_capable("osd", MON_CAP_W)) {
                osdmon()->check_pg_creates_sub(s->sub_map["osd_pg_creates"]);
            }
        } else if (p->first == "monmap") {
            monmon()->check_sub(s->sub_map[p->first]);
        } else if (logmon()->sub_name_to_id(p->first) >= 0) {
            logmon()->check_sub(s->sub_map[p->first]);
        } else if (p->first == "mgrmap" || p->first == "mgrdigest") {
            mgrmon()->check_sub(s->sub_map[p->first]);
        } else if (p->first == "servicemap") {
            mgrstatmon()->check_sub(s->sub_map[p->first]);
        } else if (p->first == "config") {
            configmon()->check_sub(s);
        } else if (p->first.find("kv:") == 0) {
            kvmon()->check_sub(s->sub_map[p->first]);
        }
    }

    if (reply) {
        // we only need to reply if the client is old enough to think it
        // has to send renewals.
        ConnectionRef con = m->get_connection();
        if (!con->has_feature(CEPH_FEATURE_MON_STATEFUL_SUB))
            m->get_connection()->send_message(new MMonSubscribeAck(
                    monmap->get_fsid(), (int)g_conf()->mon_subscribe_interval));
    }
}

void AbstractMonitor::handle_ping(MonOpRequestRef op)
{
    auto m = op->get_req<MPing>();
    dout(10) << __func__ << " " << *m << dendl;
    MPing *reply = new MPing;
    bufferlist payload;
    boost::scoped_ptr<Formatter> f(new JSONFormatter(true));
    f->open_object_section("pong");

    healthmon()->get_health_status(false, f.get(), nullptr);
    get_mon_status(f.get());

    f->close_section();
    stringstream ss;
    f->flush(ss);
    encode(ss.str(), payload);
    reply->set_payload(payload);
    dout(10) << __func__ << " reply payload len " << reply->get_payload().length() << dendl;
    m->get_connection()->send_message(reply);
}

void AbstractMonitor::send_latest_monmap(Connection *con)
{
    bufferlist bl;
    monmap->encode(bl, con->get_features());
    con->send_message(new MMonMap(bl));
}

void AbstractMonitor::handle_mon_get_map(MonOpRequestRef op)
{
    auto m = op->get_req<MMonGetMap>();
    dout(10) << "handle_mon_get_map" << dendl;
    send_latest_monmap(m->get_connection().get());
}

void AbstractMonitor::handle_route(MonOpRequestRef op)
{
    auto m = op->get_req<MRoute>();
    MonSession *session = op->get_session();
    //check privileges
    if (!session->is_capable("mon", MON_CAP_X)) {
        dout(0) << "MRoute received from entity without appropriate perms! "
                << dendl;
        return;
    }
    if (m->msg)
        dout(10) << "handle_route tid " << m->session_mon_tid << " " << *m->msg
                 << dendl;
    else
        dout(10) << "handle_route tid " << m->session_mon_tid << " null" << dendl;

    // look it up
    if (!m->session_mon_tid) {
        dout(10) << " not a routed request, ignoring" << dendl;
        return;
    }
    auto found = routed_requests.find(m->session_mon_tid);
    if (found == routed_requests.end()) {
        dout(10) << " don't have routed request tid " << m->session_mon_tid << dendl;
        return;
    }
    std::unique_ptr<RoutedRequest> rr{found->second};
    // reset payload, in case encoding is dependent on target features
    if (m->msg) {
        m->msg->clear_payload();
        rr->con->send_message(m->msg);
        m->msg = NULL;
    }
    if (m->send_osdmap_first) {
        dout(10) << " sending osdmaps from " << m->send_osdmap_first << dendl;
        osdmon()->send_incremental(m->send_osdmap_first, rr->session,
                                   true, MonOpRequestRef());
    }
    ceph_assert(rr->tid == m->session_mon_tid && rr->session->routed_request_tids.count(m->session_mon_tid));
    routed_requests.erase(found);
    rr->session->routed_request_tids.erase(m->session_mon_tid);
}

int AbstractMonitor::get_mon_metadata(int mon, Formatter *f, ostream& err)
{
    ceph_assert(f);
    if (!mon_metadata.count(mon)) {
        err << "mon." << mon << " not found";
        return -EINVAL;
    }
    const Metadata& m = mon_metadata[mon];
    for (Metadata::const_iterator p = m.begin(); p != m.end(); ++p) {
        f->dump_string(p->first.c_str(), p->second);
    }
    return 0;
}

int AbstractMonitor::print_nodes(Formatter *f, ostream& err)
{
    map<string, list<string> > mons;	// hostname => mon
    for (map<int, Metadata>::iterator it = mon_metadata.begin();
         it != mon_metadata.end(); ++it) {
        const Metadata& m = it->second;
        Metadata::const_iterator hostname = m.find("hostname");
        if (hostname == m.end()) {
            // not likely though
            continue;
        }
        mons[hostname->second].push_back(monmap->get_name(it->first));
    }

    dump_services(f, mons, "mon");
    return 0;
}

void AbstractMonitor::update_pending_metadata()
{
    Metadata metadata;
    collect_metadata(&metadata);
    size_t version_size = mon_metadata[rank]["ceph_version_short"].size();
    const std::string current_version = mon_metadata[rank]["ceph_version_short"];
    const std::string pending_version = metadata["ceph_version_short"];

    if (current_version.compare(0, version_size, pending_version) < 0) {
        mgr_client.update_daemon_metadata("mon", name, metadata);
    }
}

void AbstractMonitor::get_cluster_status(stringstream &ss, Formatter *f,
                                 MonSession *session)
{
    if (f)
        f->open_object_section("status");

    const auto&& fs_names = session->get_allowed_fs_names();

    if (f) {
        f->dump_stream("fsid") << monmap->get_fsid();
        healthmon()->get_health_status(false, f, nullptr);
        f->dump_unsigned("election_epoch", get_epoch());
        {
            f->open_array_section("quorum");
            for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
                f->dump_int("rank", *p);
            f->close_section();
            f->open_array_section("quorum_names");
            for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
                f->dump_string("id", monmap->get_name(*p));
            f->close_section();
            f->dump_int(
                    "quorum_age",
                    quorum_age());
        }
        f->open_object_section("monmap");
        monmap->dump_summary(f);
        f->close_section();
        f->open_object_section("osdmap");
        osdmon()->osdmap.print_summary(f, cout, string(12, ' '));
        f->close_section();
        f->open_object_section("pgmap");
        mgrstatmon()->print_summary(f, NULL);
        f->close_section();
        f->open_object_section("fsmap");

        FSMap fsmap_copy = mdsmon()->get_fsmap();
        if (!fs_names.empty()) {
            fsmap_copy.filter(fs_names);
        }
        const FSMap *fsmapp = &fsmap_copy;

        fsmapp->print_summary(f, NULL);
        f->close_section();
        f->open_object_section("mgrmap");
        mgrmon()->get_map().print_summary(f, nullptr);
        f->close_section();

        f->dump_object("servicemap", mgrstatmon()->get_service_map());

        f->open_object_section("progress_events");
        for (auto& i : mgrstatmon()->get_progress_events()) {
            f->dump_object(i.first.c_str(), i.second);
        }
        f->close_section();

        f->close_section();
    } else {
        ss << "  cluster:\n";
        ss << "    id:     " << monmap->get_fsid() << "\n";

        string health;
        healthmon()->get_health_status(false, nullptr, &health,
                                       "\n            ", "\n            ");
        ss << "    health: " << health << "\n";

        ss << "\n \n  services:\n";
        {
            size_t maxlen = 3;
            auto& service_map = mgrstatmon()->get_service_map();
            for (auto& p : service_map.services) {
                maxlen = std::max(maxlen, p.first.size());
            }
            string spacing(maxlen - 3, ' ');
            const auto quorum_names = get_quorum_names();
            const auto mon_count = monmap->mon_info.size();
            auto mnow = ceph::mono_clock::now();
            ss << "    mon: " << spacing << mon_count << " daemons, quorum "
               << quorum_names << " (age " << timespan_str(mnow - quorum_since) << ")";
            if (quorum_names.size() != mon_count) {
                std::list<std::string> out_of_q;
                for (size_t i = 0; i < monmap->ranks.size(); ++i) {
                    if (quorum.count(i) == 0) {
                        out_of_q.push_back(monmap->ranks[i]);
                    }
                }
                ss << ", out of quorum: " << joinify(out_of_q.begin(),
                                                     out_of_q.end(), std::string(", "));
            }
            ss << "\n";
            if (mgrmon()->in_use()) {
                ss << "    mgr: " << spacing;
                mgrmon()->get_map().print_summary(nullptr, &ss);
                ss << "\n";
            }

            FSMap fsmap_copy = mdsmon()->get_fsmap();
            if (!fs_names.empty()) {
                fsmap_copy.filter(fs_names);
            }
            const FSMap *fsmapp = &fsmap_copy;

            if (fsmapp->filesystem_count() > 0 and mdsmon()->should_print_status()){
                ss << "    mds: " << spacing;
                fsmapp->print_daemon_summary(ss);
                ss << "\n";
            }

            ss << "    osd: " << spacing;
            osdmon()->osdmap.print_summary(NULL, ss, string(maxlen + 6, ' '));
            ss << "\n";
            for (auto& p : service_map.services) {
                const std::string &service = p.first;
                // filter out normal ceph entity types
                if (ServiceMap::is_normal_ceph_entity(service)) {
                    continue;
                }
                ss << "    " << p.first << ": " << string(maxlen - p.first.size(), ' ')
                   << p.second.get_summary() << "\n";
            }
        }

        if (auto& service_map = mgrstatmon()->get_service_map();
                std::any_of(service_map.services.begin(),
                            service_map.services.end(),
                            [](auto& service) {
                                return service.second.has_running_tasks();
                            })) {
            ss << "\n \n  task status:\n";
            for (auto& [name, service] : service_map.services) {
                ss << service.get_task_summary(name);
            }
        }

        ss << "\n \n  data:\n";
        mdsmon()->print_fs_summary(ss);
        mgrstatmon()->print_summary(NULL, &ss);

        auto& pem = mgrstatmon()->get_progress_events();
        if (!pem.empty()) {
            ss << "\n \n  progress:\n";
            for (auto& i : pem) {
                if (i.second.add_to_ceph_s){
                    ss << "    " << i.second.message << "\n";
                }
            }
        }
        ss << "\n ";
    }
}

