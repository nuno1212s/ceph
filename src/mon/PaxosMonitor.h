#ifndef CEPH_PAXOSMONITOR_H
#define CEPH_PAXOSMONITOR_H

#include "AbstractMonitor.h"
#include "Elector.h"
#include "PaxosSmr.h"

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

class PaxosMonitor : public AbstractMonitor {
    // -- monitor state --
private:
    enum {
        STATE_INIT = 1,
        STATE_PROBING,
        STATE_SYNCHRONIZING,
        STATE_ELECTING,
        STATE_LEADER,
        STATE_PEON,
        STATE_SHUTDOWN
    };
    int state = STATE_INIT;

public:
    static const char *get_state_name(int s) {
        switch (s) {
            case STATE_PROBING: return "probing";
            case STATE_SYNCHRONIZING: return "synchronizing";
            case STATE_ELECTING: return "electing";
            case STATE_LEADER: return "leader";
            case STATE_PEON: return "peon";
            case STATE_SHUTDOWN: return "shutdown";
            default: return "???";
        }
    }
    const char *get_state_name() const {
        return get_state_name(state);
    }

    bool is_init() const override { return state == STATE_INIT; }
    bool is_shutdown() const { return state == STATE_SHUTDOWN; }
    bool is_probing() const { return state == STATE_PROBING; }
    bool is_synchronizing() const { return state == STATE_SYNCHRONIZING; }
    bool is_electing() const { return state == STATE_ELECTING; }
    bool is_leader() const { return state == STATE_LEADER; }
    bool is_peon() const { return state == STATE_PEON; }

    const utime_t &get_leader_since() const;


    int quorum_age() const {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
                ceph::mono_clock::now() - quorum_since);
        return age.count();
    }

    bool is_mon_down() const {
        int max = monmap->size();
        int actual = get_quorum().size();
        auto now = ceph::real_clock::now();
        return actual < max && now > monmap->created.to_real_time();
    }

    // -- elector --
private:
    std::unique_ptr<PaxosSMR> paxos;
    Elector elector;
    friend class Elector;

    /// features we require of peers (based on on-disk compatset)
    uint64_t required_features{};

    int leader{};            // current leader (to best of knowledge)
    std::set<int> quorum;       // current active set of monitors (if !starting)
    ceph::mono_clock::time_point quorum_since;  // when quorum formed
    utime_t leader_since;  // when this monitor became the leader, if it is the leader
    utime_t exited_quorum; // time detected as not in quorum; 0 if in
    std::set<std::string> outside_quorum;

    std::vector<MonCommand> leader_mon_commands; // quorum leader's commands
    std::vector<MonCommand> local_mon_commands;  // commands i support
    ceph::buffer::list local_mon_commands_bl;       // encoded version of above

    std::vector<MonCommand> prenautilus_local_mon_commands;
    ceph::buffer::list prenautilus_local_mon_commands_bl;

    bool stretch_mode_engaged{false};
    bool degraded_stretch_mode{false};
    bool recovering_stretch_mode{false};
    std::string stretch_bucket_divider;
    std::map<std::string, std::set<std::string>> dead_mon_buckets; // bucket->mon ranks, locations with no live mons
    std::set<std::string> up_mon_buckets; // locations with a live mon
    void do_stretch_mode_election_work();

    bool session_stretch_allowed(MonSession *s, MonOpRequestRef& op);
    void disconnect_disallowed_stretch_sessions();
    void set_elector_disallowed_leaders(bool allow_election);

public:

    int preinit() override;

    int init() override;

    void shutdown() override;

    const bool is_shutdown() override;

    epoch_t get_epoch() override {
        return elector.get_epoch();
    }

    int get_leader() const override { return leader; }

    std::string get_leader_name() override {
        return quorum.empty() ? std::string() : monmap->get_name(leader);
    }

    std::map<std::string,std::string> crush_loc;
    bool need_set_crush_loc{false};

    void notify_new_monmap(bool can_change_external_state = false) override;

private:
    void _reset();   ///< called from bootstrap, start_, or join_election
    void wait_for_paxos_write();
    void _finish_svc_election(); ///< called by {win,lose}_election
    void respawn();
public:
    void bootstrap();
    void join_election();
    void start_election();
    void win_standalone_election();
    // end election (called by Elector)
    void win_election(epoch_t epoch, const std::set<int>& q,
                      uint64_t features,
                      const mon_feature_t& mon_features,
                      ceph_release_t min_mon_release,
                      const std::map<int,Metadata>& metadata);
    void lose_election(epoch_t epoch, std::set<int>& q, int l,
                       uint64_t features,
                       const mon_feature_t& mon_features,
                       ceph_release_t min_mon_release);
    // end election (called by Elector)
    void finish_election();
public:
    bool is_stretch_mode() { return stretch_mode_engaged; }
    bool is_degraded_stretch_mode() { return degraded_stretch_mode; }
    bool is_recovering_stretch_mode() { return recovering_stretch_mode; }

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
    void try_engage_stretch_mode();
    void maybe_go_degraded_stretch_mode();
    void trigger_degraded_stretch_mode(const std::set<std::string>& dead_mons,
                                       const std::set<int>& dead_buckets);
    void set_degraded_stretch_mode();
    void go_recovery_stretch_mode();
    void set_recovery_stretch_mode();
    void trigger_healthy_stretch_mode();
    void set_healthy_stretch_mode();
    //This is also not implemented in regular ceph
    void enable_stretch_mode();
    void set_mon_crush_location(const std::string& loc);

    /**
     * @defgroup Monitor_h_sync Synchronization
     * @{
     */
    /**
     * @} // provider state
     */
    struct SyncProvider {
        entity_addrvec_t addrs;
        uint64_t cookie;       ///< unique cookie for this sync attempt
        utime_t timeout;       ///< when we give up and expire this attempt
        version_t last_committed; ///< last paxos version on peer
        std::pair<std::string,std::string> last_key; ///< last key sent to (or on) peer
        bool full;             ///< full scan?
        MonitorDBStore::Synchronizer synchronizer;   ///< iterator

        SyncProvider() : cookie(0), last_committed(0), full(false) {}

        void reset_timeout(CephContext *cct, int grace) {
            timeout = ceph_clock_now();
            timeout += grace;
        }
    };

    std::map<std::uint64_t, SyncProvider> sync_providers;  ///< cookie -> SyncProvider for those syncing from us
    uint64_t sync_provider_count{};   ///< counter for issued cookies to keep them unique

    /**
     * @} // requester state
     */
    entity_addrvec_t sync_provider;  ///< who we are syncing from
    uint64_t sync_cookie{};          ///< 0 if we are starting, non-zero otherwise
    bool sync_full{};                ///< true if we are a full sync, false for recent catch-up
    version_t sync_start_version{};  ///< last_committed at sync start
    Context *sync_timeout_event{};   ///< timeout event

    /**
     * floor for sync source
     *
     * When we sync we forget about our old last_committed value which
     * can be dangerous.  For example, if we have a cluster of:
     *
     *   mon.a: lc 100
     *   mon.b: lc 80
     *   mon.c: lc 100 (us)
     *
     * If something forces us to sync (say, corruption, or manual
     * intervention, or bug), we forget last_committed, and might abort.
     * If mon.a happens to be down when we come back, we will see:
     *
     *   mon.b: lc 80
     *   mon.c: lc 0 (us)
     *
     * and sync from mon.b, at which point a+b will both have lc 80 and
     * come online with a majority holding out of date commits.
     *
     * Avoid this by preserving our old last_committed value prior to
     * sync and never going backwards.
     */
    version_t sync_last_committed_floor{};

    /**
     * Obtain the synchronization target prefixes in set form.
     *
     * We consider a target prefix all those that are relevant when
     * synchronizing two stores. That is, all those that hold paxos service's
     * versions, as well as paxos versions, or any control keys such as the
     * first or last committed version.
     *
     * Given the current design, this function should return the name of all and
     * any available paxos service, plus the paxos name.
     *
     * @returns a set of strings referring to the prefixes being synchronized
     */
    std::set<std::string> get_sync_targets_names();

    /**
     * Reset the monitor's sync-related data structures for syncing *from* a peer
     */
    void sync_reset_requester();

    /**
     * Reset sync state related to allowing others to sync from us
     */
    void sync_reset_provider();

    /**
     * Caled when a sync attempt times out (requester-side)
     */
    void sync_timeout();

    /**
     * Get the latest monmap for backup purposes during sync
     */
    void sync_obtain_latest_monmap(ceph::buffer::list &bl);

    /**
     * Start sync process
     *
     * Start pulling committed state from another monitor.
     *
     * @param entity where to pull committed state from
     * @param full whether to do a full sync or just catch up on recent paxos
     */
    void sync_start(entity_addrvec_t &addrs, bool full);

    /**
     * force a sync on next mon restart
     */
    void sync_force(ceph::Formatter *f);

private:
    /**
     * store critical state for safekeeping during sync
     *
     * We store a few things on the side that we don't want to get clobbered by sync.  This
     * includes the latest monmap and a lower bound on last_committed.
     */
    void sync_stash_critical_state(MonitorDBStore::TransactionRef tx);

    /**
     * reset the sync timeout
     *
     * This is used on the client to restart if things aren't progressing
     */
    void sync_reset_timeout();

    /**
     * trim stale sync provider state
     *
     * If someone is syncing from us and hasn't talked to us recently, expire their state.
     */
    void sync_trim_providers();

    /**
     * Complete a sync
     *
     * Finish up a sync after we've gotten all of the chunks.
     *
     * @param last_committed final last_committed value from provider
     */
    void sync_finish(version_t last_committed);

    /**
     * request the next chunk from the provider
     */
    void sync_get_next_chunk();

    /**
     * handle sync message
     *
     * @param m Sync message with operation type MMonSync::OP_START_CHUNKS
     */
    void handle_sync(MonOpRequestRef op);

    void _sync_reply_no_cookie(MonOpRequestRef op);

    void handle_sync_get_cookie(MonOpRequestRef op);
    void handle_sync_get_chunk(MonOpRequestRef op);

    void handle_sync_cookie(MonOpRequestRef op);
    void handle_sync_chunk(MonOpRequestRef op);
    void handle_sync_no_cookie(MonOpRequestRef op);

    //These are not implemented in the original ceph version ?
    void handle_sync_finish(MonOpRequestRef op);
    void handle_sync_forward(MonOpRequestRef op);

    /**
     * @} // Synchronization
     */

    std::list<Context*> waitfor_quorum;
    std::list<Context*> maybe_wait_for_quorum;

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
     * @defgroup Monitor_h_scrub
     * @{
     */
    version_t scrub_version{};            ///< paxos version we are scrubbing
    std::map<int,ScrubResult> scrub_result;  ///< results so far

    /**
     * trigger a cross-mon scrub
     *
     * Verify all mons are storing identical content
     */
    int scrub_start();
    int scrub();
    void handle_scrub(MonOpRequestRef op);
    bool _scrub(ScrubResult *r,
                std::pair<std::string,std::string> *start,
                int *num_keys);
    void scrub_check_results();
    void scrub_timeout();
    void scrub_finish();
    void scrub_reset();
    void scrub_update_interval(ceph::timespan interval);

    Context *scrub_event{};       ///< periodic event to trigger scrub (leader)
    Context *scrub_timeout_event{};  ///< scrub round timeout (leader)
    void scrub_event_start();
    void scrub_event_cancel();
    void scrub_reset_timeout();
    void scrub_cancel_timeout();

    struct ScrubState {
        std::pair<std::string,std::string> last_key; ///< last scrubbed key
        bool finished;

        ScrubState() : finished(false) { }
        virtual ~ScrubState() { }
    };
    std::shared_ptr<ScrubState> scrub_state; ///< keeps track of current scrub

    /**
     * @} Scrub end
     */

    /**
     * @defgroup Probe
     *
     * @{
     */
    Context *probe_timeout_event = nullptr;  // for probing

    void reset_probe_timeout();
    void cancel_probe_timeout();
    void probe_timeout(int r);

    void handle_probe(MonOpRequestRef op);
    /**
     * Handle a Probe Operation, replying with our name, quorum and known versions.
     *
     * We use the MMonProbe message class for anything and everything related with
     * Monitor probing. One of the operations relates directly with the probing
     * itself, in which we receive a probe request and to which we reply with
     * our name, our quorum and the known versions for each Paxos service. Thus the
     * redundant function name. This reply will obviously be sent to the one
     * probing/requesting these infos.
     *
     * @todo Add @pre and @post
     *
     * @param m A Probe message, with an operation of type Probe.
     */
    void handle_probe_probe(MonOpRequestRef op);
    void handle_probe_reply(MonOpRequestRef op);

    /**
     * @} probing
     */

    void get_mon_status(ceph::Formatter *f) override;
    void _quorum_status(ceph::Formatter *f, std::ostream& ss) override;

    //Forward requests to the leader
    void forward_request_leader(MonOpRequestRef op) override;

    //handle requests that have been forwarded to us
    void handle_forward(MonOpRequestRef op);

    void resend_routed_requests();

    void waitlist_or_zap_client(MonOpRequestRef op);
    void _ms_dispatch(Message *m) override;

    int do_admin_command(std::string_view command, const cmdmap_t& cmdmap,
                         ceph::Formatter *f,
                         std::ostream& err,
                         std::ostream& out) override;

    void handle_get_version(MonOpRequestRef op) override;

    bool _add_bootstrap_peer_hint(std::string_view cmd, const cmdmap_t& cmdmap,
                                  std::ostream& ss);
    /**
     * @defgroup Time check things
     */
    void handle_timecheck_leader(MonOpRequestRef op);
    void handle_timecheck_peon(MonOpRequestRef op);

    /**
     * @} Time check end
     */

    /** can_change_external_state if we can do things like
     *  call elections as a result of the new map.
     */
    void notify_new_monmap(bool can_change_external_state=false);


public:

    /**
     * Tick this monitor
     */
    void tick() override;

    void handle_command(MonOpRequestRef op) override;

    void _dispatch_op(MonOpRequestRef op) override;

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
    PaxosMonitor(CephContext* cct_, std::string nm,, MonitorDBStore *store,  Messenger *m, Messenger *mgr_m, MonMap *map);
};


#endif //CEPH_PAXOSMONITOR_H
