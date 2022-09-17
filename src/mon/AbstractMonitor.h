#ifndef CEPH_ABSTRACTMONITOR_H
#define CEPH_ABSTRACTMONITOR_H

#include <cmath>
#include <string>
#include <array>

#include "include/types.h"
#include "include/health.h"
#include "msg/Messenger.h"

#include "common/Timer.h"

#include "health_check.h"
#include "MonMap.h"
#include "Session.h"
#include "MonCommand.h"

#include "common/config_obs.h"
#include "common/LogClient.h"
#include "auth/AuthClient.h"
#include "auth/AuthServer.h"
#include "auth/cephx/CephxKeyServer.h"
#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"
#include "include/common_fwd.h"
#include "messages/MMonCommand.h"
#include "mon/MonitorDBStore.h"
#include "mgr/MgrClient.h"

#include "mon/MonOpRequest.h"
#include "common/WorkQueue.h"
#include "common/admin_socket.h"

enum {
    l_cluster_first = 555000,
    l_cluster_num_mon,
    l_cluster_num_mon_quorum,
    l_cluster_num_osd,
    l_cluster_num_osd_up,
    l_cluster_num_osd_in,
    l_cluster_osd_epoch,
    l_cluster_osd_bytes,
    l_cluster_osd_bytes_used,
    l_cluster_osd_bytes_avail,
    l_cluster_num_pool,
    l_cluster_num_pg,
    l_cluster_num_pg_active_clean,
    l_cluster_num_pg_active,
    l_cluster_num_pg_peering,
    l_cluster_num_object,
    l_cluster_num_object_degraded,
    l_cluster_num_object_misplaced,
    l_cluster_num_object_unfound,
    l_cluster_num_bytes,
    l_cluster_last,
};

enum {
    l_mon_first = 456000,
    l_mon_num_sessions,
    l_mon_session_add,
    l_mon_session_rm,
    l_mon_session_trim,
    l_mon_num_elections,
    l_mon_election_call,
    l_mon_election_win,
    l_mon_election_lose,
    l_mon_last,
};

#define COMPAT_SET_LOC "feature_set"
#define CEPH_MON_PROTOCOL     13 /* cluster internal */

class Service;

class AbstractMonitor : public Dispatcher,
                        public AuthClient,
                        public AuthServer,
                        public md_config_obs_t {

public:

    static const std::string MONITOR_NAME;
    static const std::string MONITOR_STORE_PREFIX;

    int orig_argc = 0;
    const char **orig_argv = nullptr;

    // me
    std::string name;
    int rank;
    Messenger *messenger;
    ConnectionRef con_self;
    ceph::mutex lock = ceph::make_mutex("Monitor::lock");
    SafeTimer timer;
    Finisher finisher;
    ThreadPool cpu_tp;  ///< threadpool for CPU intensive work

    /// true if we have ever joined a quorum.  if false, we are either a
    /// new cluster, a newly joining monitor, or a just-upgraded
    /// monitor.
    bool has_ever_joined;

    ceph::mutex auth_lock = ceph::make_mutex("Monitor::auth_lock");

public:
    MonMap *monmap;
    uuid_d fingerprint;

    std::set<entity_addrvec_t> extra_probe_peers;

    PerfCounters *logger, *cluster_logger;
    bool cluster_logger_registered;

    void register_cluster_logger();

    void update_logger();

    void unregister_cluster_logger();

    LogClient log_client;
    LogChannelRef clog;
    LogChannelRef audit_clog;
    KeyRing keyring;
    KeyServer key_server;

    AuthMethodList auth_cluster_required;
    AuthMethodList auth_service_required;

    CompatSet features;

    Messenger *mgr_messenger;
    MgrClient mgr_client;
    uint64_t mgr_proxy_bytes = 0;  // in-flight proxied mgr command message bytes
    std::string gss_ktfile_client{};

    /**
     * Handle ping messages from others.
     */
    void handle_ping(MonOpRequestRef op);

    void _apply_compatset_features(CompatSet &new_features);

public:

    /*
     * We still use a monitor DB store, but only for some of the needed
     * Features of a monitor.
     * In the case of Paxos, this will still be used as before, but with febft
     * This store will not be used for the SMR protocol, as febft has it's own way of storing things
     */
    MonitorDBStore *store;

protected:
    /// features we require of peers (based on on-disk compatset)
    uint64_t required_features;

    std::set<int> quorum;       // current active set of monitors (if !starting)

public:
    // map of counts of connected clients, by type and features, for
    // each quorum mon
    std::map<int, FeatureMap> quorum_feature_map;

protected:
    /**
     * Intersection of quorum member's connection feature bits.
     */
    uint64_t quorum_con_features;
    /**
     * Intersection of quorum members mon-specific feature bits
     */
    mon_feature_t quorum_mon_features;

    ceph_release_t quorum_min_mon_release{ceph_release_t::unknown};

    std::set<std::string> outside_quorum;

    std::vector<MonCommand> leader_mon_commands; // quorum leader's commands
    std::vector<MonCommand> local_mon_commands;  // commands i support
    ceph::buffer::list local_mon_commands_bl;       // encoded version of above

    std::vector<MonCommand> prenautilus_local_mon_commands;
    ceph::buffer::list prenautilus_local_mon_commands_bl;

public:
    const std::vector<MonCommand> &get_local_commands(mon_feature_t f) {
        if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
            return local_mon_commands;
        } else {
            return prenautilus_local_mon_commands;
        }
    }
    const ceph::buffer::list& get_local_commands_bl(mon_feature_t f) {
        if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
            return local_mon_commands_bl;
        } else {
            return prenautilus_local_mon_commands_bl;
        }
    }
    void set_leader_commands(const std::vector<MonCommand>& cmds) {
        leader_mon_commands = cmds;
    }

public:

    virtual const char * get_state_name() const = 0;

    void prepare_new_fingerprint(MonitorDBStore::TransactionRef t);

    virtual epoch_t get_epoch() = 0;

    virtual int get_leader() const = 0;

    bool is_leader() {
        return get_leader() == rank;
    }

    bool is_peon () {
        return get_leader() != rank;
    }

    virtual void forward_request_leader(MonOpRequestRef op) = 0;

    virtual std::string get_leader_name() = 0;

    virtual utime_t get_leader_since() = 0;

    const std::set<int> &get_quorum() const { return quorum; }

    virtual int quorum_age() const = 0;

    std::list<std::string> get_quorum_names() {
        std::list<std::string> q;
        for (auto p = quorum.begin(); p != quorum.end(); ++p)
            q.push_back(monmap->get_name(*p));
        return q;
    }

    uint64_t get_quorum_con_features() const {
        return quorum_con_features;
    }

    mon_feature_t get_quorum_mon_features() const {
        return quorum_mon_features;
    }

    uint64_t get_required_features() const {
        return required_features;
    }

    mon_feature_t get_required_mon_features() const {
        return monmap->get_required_features();
    }

    bool is_mon_down() const {
        int max = monmap->size();
        int actual = get_quorum().size();
        auto now = ceph::real_clock::now();
        return actual < max && now > monmap->created.to_real_time();
    }

    void apply_quorum_to_compatset_features();

    void apply_monmap_to_compatset_features();

    void calc_quorum_requirements();

    void get_combined_feature_map(FeatureMap *fm);

    /**
     * @defgroup Sessions
     *
     * {@
     */

    MonSessionMap session_map;
    ceph::mutex session_map_lock = ceph::make_mutex("Monitor::session_map_lock");
    AdminSocketHook *admin_hook;

    template<typename Func, typename...Args>
    void with_session_map(Func &&func) {
        std::lock_guard l(session_map_lock);
        std::forward<Func>(func)(session_map);
    }

    void send_latest_monmap(Connection *con);

    // messages
    virtual void handle_get_version(MonOpRequestRef op) = 0;

    void handle_subscribe(MonOpRequestRef op);

    void handle_mon_get_map(MonOpRequestRef op);

    /**
     * @}
     */

    static void _generate_command_map(cmdmap_t &cmdmap,
                                      std::map<std::string, std::string> &param_str_map);

    static const MonCommand *_get_moncommand(
            const std::string &cmd_prefix,
            const std::vector<MonCommand> &cmds);

    bool _allowed_command(MonSession *s, const std::string &module,
                          const std::string &prefix,
                          const cmdmap_t &cmdmap,
                          const std::map<std::string, std::string> &param_str_map,
                          const MonCommand *this_cmd);


    virtual void get_mon_status(ceph::Formatter *f) = 0;

    virtual void _quorum_status(ceph::Formatter *f, std::ostream &ss) = 0;

    void handle_tell_command(MonOpRequestRef op);

    void handle_route(MonOpRequestRef op);

    //We want each monitor type to handle its commands separately as
    //Paxos needs to forward its requests to the leader while febft
    //can execute the commands on any given monitor
    virtual void handle_command(MonOpRequestRef op) = 0;

    int get_mon_metadata(int mon, ceph::Formatter *f, std::ostream &err);

    int print_nodes(ceph::Formatter *f, std::ostream &err);

    // track metadata reported by win_election()
    std::map<int, Metadata> mon_metadata;
    std::map<int, Metadata> pending_metadata;

    /**
     *
     */
    struct health_cache_t {
        health_status_t overall;
        std::string summary;

        void reset() {
            // health_status_t doesn't really have a NONE value and we're not
            // okay with setting something else (say, HEALTH_ERR).  so just
            // leave it be.
            summary.clear();
        }
    } health_status_cache;

    Context *health_tick_event = nullptr;
    Context *health_interval_event = nullptr;

    void health_tick_start();

    void health_tick_stop();

    ceph::real_clock::time_point health_interval_calc_next_update();

    void health_interval_start();

    void health_interval_stop();

    void health_events_cleanup();

    void health_to_clog_update_conf(const std::set<std::string> &changed);

    void do_health_to_clog_interval();

    void do_health_to_clog(bool force = false);

    std::vector<DaemonHealthMetric> get_health_metrics();

    void log_health(
            const health_check_map_t &updated,
            const health_check_map_t &previous,
            MonitorDBStore::TransactionRef t);

    void update_pending_metadata();

protected:

    class HealthCheckLogStatus {
    public:
        health_status_t severity;
        std::string last_message;
        utime_t updated_at = 0;

        HealthCheckLogStatus(health_status_t severity_,
                             const std::string &last_message_,
                             utime_t updated_at_)
                : severity(severity_),
                  last_message(last_message_),
                  updated_at(updated_at_) {}
    };

    std::map<std::string, HealthCheckLogStatus> health_check_log_times;

public:

    void get_cluster_status(std::stringstream &ss, ceph::Formatter *f,
                            MonSession *session);

    void reply_command(MonOpRequestRef op, int rc, const std::string &rs, version_t version);

    void reply_command(MonOpRequestRef op, int rc, const std::string &rs, ceph::buffer::list &rdata, version_t version);

    void reply_tell_command(MonOpRequestRef op, int rc, const std::string &rs);

    // request routing
    struct RoutedRequest {
        uint64_t tid;
        ceph::buffer::list request_bl;
        MonSession *session;
        ConnectionRef con;
        uint64_t con_features;
        MonOpRequestRef op;

        RoutedRequest() : tid(0), session(NULL), con_features(0) {}

        ~RoutedRequest() {
            if (session)
                session->put();
        }
    };

    uint64_t routed_request_tid;
    std::map<uint64_t, RoutedRequest *> routed_requests;

    void send_reply(MonOpRequestRef op, Message *reply);

    void no_reply(MonOpRequestRef op);

    void remove_session(MonSession *s);

    void remove_all_sessions();

    void send_mon_message(Message *m, int rank);

    /**
     * can_change_external_state if we can do things like
     *  call elections as a result of the new map.
     */
    virtual void notify_new_monmap(bool can_change_external_state = false) = 0;

public:
    struct C_Command : public C_MonOp {
        AbstractMonitor &mon;
        int rc;
        std::string rs;
        ceph::buffer::list rdata;
        version_t version;

        C_Command(AbstractMonitor &_mm, MonOpRequestRef _op, int r, std::string s, version_t v) :
                C_MonOp(_op), mon(_mm), rc(r), rs(s), version(v) {}

        C_Command(AbstractMonitor &_mm, MonOpRequestRef _op, int r, std::string s, ceph::buffer::list rd, version_t v) :
                C_MonOp(_op), mon(_mm), rc(r), rs(s), rdata(rd), version(v) {}

        void _finish(int r) override {
            auto m = op->get_req<MMonCommand>();
            if (r >= 0) {
                std::ostringstream ss;
                if (!op->get_req()->get_connection()) {
                    ss << "connection dropped for command ";
                } else {
                    MonSession *s = op->get_session();

                    // if client drops we may not have a session to draw information from.
                    if (s) {
                        ss << "from='" << s->name << " " << s->addrs << "' "
                           << "entity='" << s->entity_name << "' ";
                    } else {
                        ss << "session dropped for command ";
                    }
                }
                cmdmap_t cmdmap;
                std::ostringstream ds;
                std::string prefix;
                cmdmap_from_json(m->cmd, &cmdmap, ds);
                cmd_getval(cmdmap, "prefix", prefix);
                if (prefix != "config set" && prefix != "config-key set")
                    ss << "cmd='" << m->cmd << "': finished";

                mon.audit_clog->info() << ss.str();
                mon.reply_command(op, rc, rs, rdata, version);
            } else if (r == -ECANCELED)
                return;
            else if (r == -EAGAIN)
                mon.dispatch_op(op);
            else
                ceph_abort_msg("bad C_Command return value");
        }
    };

public:
    class C_RetryMessage : public C_MonOp {
        AbstractMonitor *mon;
    public:
        C_RetryMessage(AbstractMonitor *m, MonOpRequestRef op) :
                C_MonOp(op), mon(m) {}

        void _finish(int r) override {
            if (r == -EAGAIN || r >= 0)
                mon->dispatch_op(op);
            else if (r == -ECANCELED)
                return;
            else
                ceph_abort_msg("bad C_RetryMessage return value");
        }
    };

protected:

    //ms_dispatch handles a lot of logic so we want to reuse it
    //on forwarded messages, so we create a non-locking version for this class
    virtual void _ms_dispatch(Message *m) = 0;

    bool ms_dispatch(Message *m) override {
        std::lock_guard l{lock};
        _ms_dispatch(m);
        return true;
    }

    void dispatch_op(MonOpRequestRef op);

    virtual void _dispatch_op(MonOpRequestRef op) = 0;

    //mon_caps is used for un-connected messages from monitors
    MonCap mon_caps;

    bool get_authorizer(int dest_type, AuthAuthorizer **authorizer);

public: // for AuthMonitor msgr1:
    int ms_handle_authentication(Connection *con) override;

protected:
    void ms_handle_accept(Connection *con) override;

    bool ms_handle_reset(Connection *con) override;

    void ms_handle_remote_reset(Connection *con) override {}

    bool ms_handle_refused(Connection *con) override;

    /**
     * @defgroup AuthClient
     *
     * {@
     */
    int get_auth_request(
            Connection *con,
            AuthConnectionMeta *auth_meta,
            uint32_t *method,
            std::vector<uint32_t> *preferred_modes,
            ceph::buffer::list *out) override;

    int handle_auth_reply_more(
            Connection *con,
            AuthConnectionMeta *auth_meta,
            const ceph::buffer::list &bl,
            ceph::buffer::list *reply) override;

    int handle_auth_done(
            Connection *con,
            AuthConnectionMeta *auth_meta,
            uint64_t global_id,
            uint32_t con_mode,
            const ceph::buffer::list &bl,
            CryptoKey *session_key,
            std::string *connection_secret) override;

    int handle_auth_bad_method(
            Connection *con,
            AuthConnectionMeta *auth_meta,
            uint32_t old_auth_method,
            int result,
            const std::vector<uint32_t> &allowed_methods,
            const std::vector<uint32_t> &allowed_modes) override;

    /**
     * @} Auth Client
     */

    // AuthServer
    int handle_auth_request(
            Connection *con,
            AuthConnectionMeta *auth_meta,
            bool more,
            uint32_t auth_method,
            const ceph::buffer::list &bl,
            ceph::buffer::list *reply) override;
    // /AuthServer

    int write_default_keyring(ceph::buffer::list &bl);

    void extract_save_mon_key(KeyRing &keyring);

    void collect_metadata(Metadata *m);

    int load_metadata();

    void count_metadata(const std::string &field, ceph::Formatter *f);

    void count_metadata(const std::string &field, std::map<std::string, int> *out);

    // get_all_versions() gathers version information from daemons for health check
    void get_all_versions(std::map<std::string, std::list<std::string>> &versions);

    void get_versions(std::map<std::string, std::list<std::string>> &versions);

    /// read the ondisk features into the CompatSet pointed to by read_features
    static void read_features_off_disk(MonitorDBStore *store, CompatSet *read_features);

    void read_features();

    void write_features(MonitorDBStore::TransactionRef t);

    OpTracker op_tracker;

public:
    // features
    static CompatSet get_initial_supported_features();

    static CompatSet get_supported_features();

    static CompatSet get_legacy_features();

public:
    AbstractMonitor(CephContext *cct_, MonitorDBStore *store, std::string nm, Messenger *m, Messenger *mgr_m,
                    MonMap *map);

    ~AbstractMonitor() override;

    void update_log_clients();

    int sanitize_options();

    static int check_features(MonitorDBStore *store);

    // config observer
    const char **get_tracked_conf_keys() const override;

    void handle_conf_change(const ConfigProxy &conf,
                            const std::set<std::string> &changed) override;

    virtual int preinit() = 0;

    virtual int init() = 0;

    virtual void refresh_from_smr(bool *need_bootstrap) = 0;

protected:
    //Schedule the ticks
    void new_tick();

public:
    virtual void shutdown() = 0;

    virtual bool is_init() const = 0;

    virtual bool is_shutdown() const = 0;

    virtual void tick() = 0;

    void handle_signal(int sig);

    int mkfs(ceph::buffer::list &osdmapbl);

    /**
     * check cluster_fsid file
     *
     * @return EEXIST if file exists and doesn't match, 0 on match, or negative error code
     */
    int check_fsid();

    /**
     * write cluster_fsid file
     *
     * @return 0 on success, or negative error code
     */
    int write_fsid();

    int write_fsid(MonitorDBStore::TransactionRef t);

    virtual int do_admin_command(std::string_view command, const cmdmap_t &cmdmap,
                                 ceph::Formatter *f,
                                 std::ostream &err,
                                 std::ostream &out) = 0;

    static void format_command_descriptions(const std::vector<MonCommand> &commands,
                                            ceph::Formatter *f,
                                            uint64_t features,
                                            ceph::buffer::list *rdata);

    bool is_keyring_required();

    /**
     * @defgroup Services
     *
     * {@
     */

    /**
     * Vector holding the Services serviced by this Monitor.
     */
    std::array<std::unique_ptr<Service>, PAXOS_NUM> services;

    class MDSMonitor *mdsmon() {
        return (class MDSMonitor *) services[PAXOS_MDSMAP].get();
    }

    class MonmapMonitor *monmon() {
        return (class MonmapMonitor *) services[PAXOS_MONMAP].get();
    }

    class OSDMonitor *osdmon() {
        return (class OSDMonitor *) services[PAXOS_OSDMAP].get();
    }

    class AuthMonitor *authmon() {
        return (class AuthMonitor *) services[PAXOS_AUTH].get();
    }

    class LogMonitor *logmon() {
        return (class LogMonitor *) services[PAXOS_LOG].get();
    }

    class MgrMonitor *mgrmon() {
        return (class MgrMonitor *) services[PAXOS_MGR].get();
    }

    class MgrStatMonitor *mgrstatmon() {
        return (class MgrStatMonitor *) services[PAXOS_MGRSTAT].get();
    }

    class HealthMonitor *healthmon() {
        return (class HealthMonitor *) services[PAXOS_HEALTH].get();
    }

    class ConfigMonitor *configmon() {
        return (class ConfigMonitor *) services[PAXOS_CONFIG].get();
    }

    class KVMonitor *kvmon() {
        return (class KVMonitor *) services[PAXOS_KV].get();
    }

    /**
     * @}
     */

    friend class OSDMonitor;

    friend class MDSMonitor;

    friend class MonmapMonitor;

    friend class LogMonitor;

    friend class KVMonitor;

public:
    /**
     * @defgroup Monitor_h_TimeCheck Monitor Clock Drift Early Warning System
     * @{
     *
     * We use time checks to keep track of any clock drifting going on in the
     * cluster. This is accomplished by periodically ping each monitor in the
     * quorum and register its response time on a map, assessing how much its
     * clock has drifted. We also take this opportunity to assess the latency
     * on response.
     *
     * This mechanism works as follows:
     *
     *  - Leader sends out a 'PING' message to each other monitor in the quorum.
     *    The message is timestamped with the leader's current time. The leader's
     *    current time is recorded in a map, associated with each peon's
     *    instance.
     *  - The peon replies to the leader with a timestamped 'PONG' message.
     *  - The leader calculates a delta between the peon's timestamp and its
     *    current time and stashes it.
     *  - The leader also calculates the time it took to receive the 'PONG'
     *    since the 'PING' was sent, and stashes an approximate latency estimate.
     *  - Once all the quorum members have pong'ed, the leader will share the
     *    clock skew and latency maps with all the monitors in the quorum.
     */
    std::map<int, utime_t> timecheck_waiting;
    std::map<int, double> timecheck_skews;
    std::map<int, double> timecheck_latencies;
    // odd value means we are mid-round; even value means the round has
    // finished.
    version_t timecheck_round{};
    unsigned int timecheck_acks{};
    utime_t timecheck_round_start;
    friend class HealthMonitor;
    /* When we hit a skew we will start a new round based off of
     * 'mon_timecheck_skew_interval'. Each new round will be backed off
     * until we hit 'mon_timecheck_interval' -- which is the typical
     * interval when not in the presence of a skew.
     *
     * This variable tracks the number of rounds with skews since last clean
     * so that we can report to the user and properly adjust the backoff.
     */
    uint64_t timecheck_rounds_since_clean{};
    /**
     * Time Check event.
     */
    Context *timecheck_event{};

    void timecheck_start();
    void timecheck_finish();
    void timecheck_start_round();
    void timecheck_finish_round(bool success = true);
    void timecheck_cancel_round();
    void timecheck_cleanup();
    void timecheck_reset_event();
    void timecheck_check_skews();
    void timecheck_report();
    void timecheck();

    health_status_t timecheck_status(std::ostringstream &ss,
                                     const double skew_bound,
                                     const double latency);

    void handle_timecheck(MonOpRequestRef op);

private:

    void handle_timecheck_leader(MonOpRequestRef op);
    void handle_timecheck_peon(MonOpRequestRef op);

public:
    /**
     * Returns 'true' if this is considered to be a skew; 'false' otherwise.
     */
    bool timecheck_has_skew(const double skew_bound, double *abs) const {
        double abs_skew = std::fabs(skew_bound);
        if (abs)
            *abs = abs_skew;
        return (abs_skew > g_conf()->mon_clock_drift_allowed);
    }

    /**
     * @} Timecheck end
     */

    /**
     * @defgroup Paxos Strech mode
     *
     * @{
     */


    virtual bool is_stretch_mode() = 0;
    virtual bool is_degraded_stretch_mode() = 0;
    virtual bool is_recovering_stretch_mode() = 0;

    /**
     * This set of functions maintains the in-memory stretch state
     * and sets up transitions of the map states by calling in to
     * MonmapMonitor and OSDMonitor.
     *
     * The [maybe_]go_* functions are called on the leader to
     * decide if transitions should happen; the trigger_* functions
     * set up the map transitions; and the set_* functions actually
     * change the memory state -- but these are only called
     * via OSDMonitor::update_from_paxos, to guarantee consistent
     * updates across the entire cluster.
     */
    virtual void try_engage_stretch_mode() = 0;
    virtual void maybe_go_degraded_stretch_mode() = 0;
    virtual void trigger_degraded_stretch_mode(const std::set<std::string>& dead_mons,
                                       const std::set<int>& dead_buckets) = 0;
    virtual void set_degraded_stretch_mode() = 0;
    virtual void go_recovery_stretch_mode() = 0;
    virtual void set_recovery_stretch_mode() = 0;
    virtual void trigger_healthy_stretch_mode() = 0;
    virtual void set_healthy_stretch_mode() = 0;
    //This is also not implemented in regular ceph
    virtual void enable_stretch_mode() = 0;
    virtual void set_mon_crush_location(const std::string& loc) = 0;

    /**
     * }@
     */
protected:
    // don't allow copying
    AbstractMonitor(const AbstractMonitor &rhs);

    AbstractMonitor &operator=(const AbstractMonitor &rhs);

};

#define CEPH_MON_FEATURE_INCOMPAT_BASE CompatSet::Feature (1, "initial feature set (~v.18)")
#define CEPH_MON_FEATURE_INCOMPAT_GV CompatSet::Feature (2, "global version sequencing (v0.52)")
#define CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS CompatSet::Feature (3, "single paxos with k/v store (v0.\?)")
#define CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES CompatSet::Feature(4, "support erasure code pools")
#define CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC CompatSet::Feature(5, "new-style osdmap encoding")
#define CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2 CompatSet::Feature(6, "support isa/lrc erasure code")
#define CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3 CompatSet::Feature(7, "support shec erasure code")
#define CEPH_MON_FEATURE_INCOMPAT_KRAKEN CompatSet::Feature(8, "support monmap features")
#define CEPH_MON_FEATURE_INCOMPAT_LUMINOUS CompatSet::Feature(9, "luminous ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_MIMIC CompatSet::Feature(10, "mimic ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_NAUTILUS CompatSet::Feature(11, "nautilus ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_OCTOPUS CompatSet::Feature(12, "octopus ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_PACIFIC CompatSet::Feature(13, "pacific ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_QUINCY CompatSet::Feature(14, "quincy ondisk layout")
// make sure you add your feature to Monitor::get_supported_features

/* Callers use:
 *
 *      new C_MonContext{...}
 *
 * instead of
 *
 *      new C_MonContext(...)
 *
 * because of gcc bug [1].
 *
 * [1] https://gcc.gnu.org/bugzilla/show_bug.cgi?id=85883
 */
template<typename T>
class C_MonContext : public LambdaContext<T> {
public:
    C_MonContext(const AbstractMonitor *m, T &&f) :
            LambdaContext<T>(std::forward<T>(f)),
            mon(m) {}

    void finish(int r) override {
        if (mon->is_shutdown())
            return;
        LambdaContext<T>::finish(r);
    }

protected:
    const AbstractMonitor *mon;
};

struct C_MgrProxyCommand : public Context {
    AbstractMonitor *mon;
    MonOpRequestRef op;
    uint64_t size;
    bufferlist outbl;
    std::string outs;
    C_MgrProxyCommand(AbstractMonitor *mon, MonOpRequestRef op, uint64_t s)
            : mon(mon), op(op), size(s) { }
    void finish(int r) {
        std::lock_guard l(mon->lock);
        mon->mgr_proxy_bytes -= size;
        mon->reply_command(op, r, outs, outbl, 0);
    }
};

class AdminHook : public AdminSocketHook {
    AbstractMonitor *mon;
public:
    explicit AdminHook(AbstractMonitor *m) : mon(m) {}

    int call(std::string_view command, const cmdmap_t &cmdmap,
             Formatter *f,
             std::ostream &errss,
             bufferlist &out) override {
        std::stringstream outss;
        int r = mon->do_admin_command(command, cmdmap, f, errss, outss);
        out.append(outss);
        return r;
    }
};

#endif //CEPH_ABSTRACTMONITOR_H