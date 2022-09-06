#ifndef CEPH_FEBFTMONITOR_H
#define CEPH_FEBFTMONITOR_H

#include "mon/AbstractMonitor.h"
#include "FebftSmr.h"

class FebftMonitor : public AbstractMonitor {

private:
    std::unique_ptr<FebftSMR> febft;

public:

    const char * get_state_name() const override;

    int get_leader() const override;

    std::string get_leader_name() override;

    utime_t get_leader_since() override;

    int quorum_age() const override;

    epoch_t get_epoch() override;

    int do_admin_command(std::string_view command, const cmdmap_t& cmdmap,
                         ceph::Formatter *f,
                         std::ostream& err,
                         std::ostream& out) override;

    void handle_get_version(MonOpRequestRef op) override;

    void get_mon_status(ceph::Formatter *f) override;

    void _quorum_status(ceph::Formatter *f, std::ostream &ss) override;

    void handle_command(MonOpRequestRef op) override;

    void notify_new_monmap(bool can_change_external_state) override;

    void _dispatch_op(MonOpRequestRef op) override;

    int preinit() override;

    int init() override;

    void refresh_from_smr(bool *need_bootstrap) override;

    void shutdown() override;

    bool is_init() const override;

    bool is_shutdown() const override;

    void tick() override;

    void forward_request_leader(MonOpRequestRef op) override {
      //In FeBFT, because we use the clients instead of acting directly like a replica, we
      //never need to forward any requests, we only need to give them to the febft client, which
      //Will then broadcast the request into the necessary replicas
    };


public:
    /**
     * None of these methods are implemented in the FeBFT version of ceph as there
     * Was not enough time to investigate and translate the features in febft
     */
    bool is_stretch_mode() override;
    bool is_degraded_stretch_mode() override;
    bool is_recovering_stretch_mode() override;

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
    void try_engage_stretch_mode() override;
    void maybe_go_degraded_stretch_mode() override;
    void trigger_degraded_stretch_mode(const std::set<std::string>& dead_mons,
                                       const std::set<int>& dead_buckets) override;
    void set_degraded_stretch_mode() override;
    void go_recovery_stretch_mode() override;
    void set_recovery_stretch_mode() override;
    void trigger_healthy_stretch_mode() override;
    void set_healthy_stretch_mode() override;
    //This is also not implemented in regular ceph
    void enable_stretch_mode() override;
    void set_mon_crush_location(const std::string& loc) override;


    FebftMonitor(CephContext *cct_, std::string nm, MonitorDBStore *store, Messenger *m, Messenger *mgr_m,
                               MonMap *map);
};


#endif //CEPH_FEBFTMONITOR_H
