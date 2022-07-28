#ifndef CEPH_SERVICE_H
#define CEPH_SERVICE_H

#include "AbstractMonitor.h"
#include "SMRProtocol.h"

class Service {

public:

    /**
     * Reference to the monitor class that is associated with this service
     */
    AbstractMonitor &mon;

    /**
     * The SMR protocol instance to which this class is associated with
     */
    SMRProtocol &smr_protocol;

    /**
     * The name of this service
     */
    std::string service_name;

    /**
     * Are we currently proposing a value?
     *
     * This is true if our client instance is currently awaiting 2f + 1 responses from replicas
     */
    bool proposing;

    /**
     *
     */
    bool need_immediate_propose = false;

    /**
     * @defgroup Service_h_store_keys Set of keys that are usually used on
     *					 all the services implementing this
     *					 class, and, being almost the only keys
     *					 used, should be standardized to avoid
     *					 mistakes.
     * @{
     */
    const std::string last_committed_name;
    const std::string first_committed_name;
    const std::string full_prefix_name;
    const std::string full_latest_name;
    /**
     * @}
     */

/**
 * Retry dispatching a given service message
 *
 * This callback class is used when we had to wait for some condition to
 * become true while we were dispatching it.
 *
 * For instance, if the message's version isn't readable, according to Paxos,
 * then we must wait for it to become readable. So, we just queue an
 * instance of this class onto the Paxos::wait_for_readable function, and
 * we will retry the whole dispatch again once the callback is fired.
 */
class C_RetryMessage : public C_MonOp {
    Service *svc;
public:
    C_RetryMessage(Service *s, MonOpRequestRef op_) :
            C_MonOp(op_), svc(s) {}

    void _finish(int r) override {
        if (r == -EAGAIN || r >= 0)
            svc->dispatch(op);
        else if (r == -ECANCELED)
            return;
        else
            ceph_abort_msg("bad C_RetryMessage return value");
    }
};

protected:

    /**
     * Services implementing us used to depend on the Paxos version, back when
     * each service would have a Paxos instance for itself. However, now we only
     * have a single Paxos instance, shared by all the services. Each service now
     * must keep its own version, if so they wish. This variable should be used
     * for that purpose.
     */
    version_t service_version;

private:

    /**
     * Callback for proposing our pending value once a timer runs out
     */
    Context *proposal_timer;

    /**
     *
     */
    bool have_pending;

    health_check_map_t health_checks;

private:
    /**
     * @defgroup PaxosService_h_version_cache Variables holding cached values
     *                                        for the most used versions (first
     *                                        and last committed); we only have
     *                                        to read them when the store is
     *                                        updated, so in-between updates we
     *                                        may very well use cached versions
     *                                        and avoid the overhead.
     * @{
     */
    version_t cached_first_committed;
    version_t cached_last_committed;
    /**
     * @}
     */

    /**
     * Callback list to be used whenever we are running a proposal through
     * Paxos. These callbacks will be awaken whenever the said proposal
     * finishes.
     */
    std::list<Context *> waiting_for_finished_proposal;


protected:
    /**
     * format of our state in the leveldb, 0 for default
     */
    version_t format_version;

    /**
     * @defgroup PaxosService_h_callbacks Callback classes
     * @{
     */

    class C_ReplyOp : public C_MonOp {
        AbstractMonitor &mon;
        MonOpRequestRef op;
        MessageRef reply;
    public:
        C_ReplyOp(Service *s, MonOpRequestRef o, MessageRef r) :
                C_MonOp(o), mon(s->mon), op(o), reply(r) {}

        void _finish(int r) override {
            if (r >= 0) {
                mon.send_reply(op, reply.detach());
            }
        }
    };
    /**
     * @}
     */

public:
    /**
     *
     * @param mon
     * @param name
     */
    Service(AbstractMonitor &mon, SMRProtocol &smr_protocol, std::string name) : mon(mon),
                                                                                 smr_protocol(smr_protocol),
                                                                                 service_name(name), proposing(false),
                                                                                 last_committed_name("last_committed"),
                                                                                 first_committed_name(
                                                                                         "first_committed"),
                                                                                 full_prefix_name("full"),
                                                                                 full_latest_name("latest"),
                                                                                 service_version(0),
                                                                                 have_pending(false),
                                                                                 cached_first_committed(0),
                                                                                 cached_last_committed(0) {}

    virtual ~Service() {}

private:

    /**
     * Update our state from the SMR protocol and then create a new pending state if we are the leader of the view.
     *
     * @remarks Only create the pending state if we this machine is the leader
     *
     * @pre SMR is active
     * @post have_pending is true if our Monitor is this view's leader
     */
    void _active();

public:

    /**
     * Get the service's name.
     *
     * @returns The service's name.
     */
    const std::string &get_service_name() const { return service_name; }

    /**
     * Get the store prefixes we utilize
     */
    virtual void get_store_prefixes(std::set<std::string> &s) const {
        s.insert(service_name);
    }

    // i implement and you ignore
    /**
     * Informs this instance that it should consider itself restarted.
     *
     * This means that we will cancel our proposal_timer event, if any exists.
     */
    void restart();

    /**
     * Informs this instance that an election has finished.
     *
     * This means that we will invoke a PaxosService::discard_pending while
     * setting have_pending to false (basically, ignore our pending state) and
     * we will then make sure we obtain a new state.
     *
     * Our state shall be updated by PaxosService::_active if the Paxos is
     * active; otherwise, we will wait for it to become active by adding a
     * PaxosService::C_Active callback to it.
     */
    void election_finished();

    /**
     * Informs this instance that it is supposed to shutdown.
     *
     * Basically, it will instruct SMR to cancel all events/callbacks and then
     * will cancel the proposal_timer event if any exists.
     */
    void shutdown();

    /**
     * Propose a new value through the SMR.
     *
     * This function should be called by the classes implementing
     * Service, in order to propose a new value through the SMR algorithm.
     *
     * @pre The implementation class implements the encode_pending function.
     * @pre have_pending is true
     * - If this is paxos:
     * @pre Our monitor is the Leader
     * @pre Paxos is active
     * - If not those two clauses are not necessary
     * @post Cancel the proposal timer, if any
     * @post have_pending is false
     * @post propose pending value through SMR
     *
     * @note This function depends on the implementation of encode_pending on
     *	   the class that is implementing PaxosService
     */
    void propose_pending();

    /**
     * Let others request us to propose.
     *
     * At the moment, this is just a wrapper to propose_pending() with an
     * extra check for is_writeable(), but it's a good practice to dissociate
     * requests for proposals from direct usage of propose_pending() for
     * future use -- we might want to perform additional checks or put a
     * request on hold, for instance..
     */
    void request_proposal() {
        ceph_assert(is_writeable());

        propose_pending();
    }

    /**
     * Request service @p other to perform a proposal.
     *
     * We could simply use the function above, requesting @p other directly,
     * but we might eventually want to do something to the request -- say,
     * set a flag stating we're waiting on a cross-proposal to be finished.
     */
    void request_proposal(Service *other) {
        ceph_assert(other != NULL);
        ceph_assert(other->is_writeable());

        other->request_proposal();
    }

    /**
     * Dispatch a message by passing it to several different functions that are
     * either implemented directly by this service, or that should be implemented
     * by the class implementing this service.
     *
     * @param m A message
     * @returns 'true' on successful dispatch; 'false' otherwise.
     */
    bool dispatch(MonOpRequestRef op);

    void refresh(bool *need_bootstrap);

    void post_refresh();

    /**
     * @defgroup PaxosService_h_override_funcs Functions that should be
     *					     overridden.
     *
     * These functions should be overridden at will by the class implementing
     * this service.
     * @{
     */
    /**
     * Create the initial state for your system.
     *
     * In some of ours the state is actually set up elsewhere so this does
     * nothing.
     */
    virtual void create_initial() = 0;

    /**
     * Query the SMR system for the latest state and apply it if it's newer
     * than the current Monitor state.
     */
    virtual void update_from_smr(bool *need_bootstrap) = 0;

    /**
     * Hook called after all services have refreshed their state from smr service
     *
     * This is useful for doing any update work that depends on other
     * service's having up-to-date state.
     */
    virtual void post_smr_update() {}

    /**
     * Init on startup
     *
     * This is called on mon startup, after all of the PaxosService instances'
     * update_from_paxos() methods have been called
     */
    virtual void init() {}

    /**
     * Create the pending state.
     *
     * @invariant This function is only called on a Leader.
     * @remarks This created state is then modified by incoming messages.
     * @remarks Called at startup and after every Paxos ratification round.
     */
    virtual void create_pending() = 0;

    /**
     * Encode the pending state into a ceph::buffer::list for ratification and
     * transmission as the next state.
     *
     * @invariant This function is only called on a Leader.
     *
     * @param t The transaction to hold all changes.
     */
    virtual void encode_pending(MonitorDBStore::TransactionRef t) = 0;

    /**
     * Discard the pending state
     *
     * @invariant This function is only called on a Leader.
     *
     * @remarks This function is NOT overridden in any of our code, but it is
     *	      called in PaxosService::election_finished if have_pending is
     *	      true.
     */
    virtual void discard_pending() { }

    /**
     * Look at the query; if the query can be handled without changing state,
     * do so.
     *
     * @param m A query message
     * @returns 'true' if the query was handled (e.g., was a read that got
     *	      answered, was a state change that has no effect); 'false'
     *	      otherwise.
     */
    virtual bool preprocess_query(MonOpRequestRef op) = 0;

    /**
     * Apply the message to the pending state.
     *
     * @invariant This function is only called on a Leader.
     *
     * @param m An update message
     * @returns 'true' if the update message was handled (e.g., a command that
     *	      went through); 'false' otherwise.
     */
    virtual bool prepare_update(MonOpRequestRef op) = 0;
    /**
     * @}
     */

    /**
     * Determine if the Paxos system should vote on pending, and if so how long
     * it should wait to vote.
     *
     * @param[out] delay The wait time, used so we can limit the update traffic
     *		       spamming.
     * @returns 'true' if the Paxos system should propose; 'false' otherwise.
     */
    virtual bool should_propose(double &delay);

    /**
     * force an immediate propose.
     *
     * This is meant to be called from prepare_update(op).
     */
    void force_immediate_propose() {
        need_immediate_propose = true;
    }

    /**
     * @defgroup Service_h_courtesy Courtesy functions
     *
     * Courtesy functions, in case the class implementing this service has
     * anything it wants/needs to do at these times.
     * @{
     */
    /**
     * This is called when the SMR state goes to active.
     *
     * On the peon, this is after each election.
     * On the leader, this is after each election, *and* after each completed
     * proposal.
     *
     * @note This function may get called twice in certain recovery cases.
     */
    virtual void on_active() { }

    /**
     * This is called when we are shutting down
     */
    virtual void on_shutdown() {}

    /**
     * this is called when activating on the leader
     *
     * it should conditionally upgrade the on-disk format by proposing a transaction
     */
    virtual void upgrade_format() { }

    /**
     * this is called when we detect the store has just upgraded underneath us
     */
    virtual void on_upgrade() {}

    /**
     * Called when the Paxos system enters a Leader election.
     *
     * @remarks It's a courtesy method, in case the class implementing this
     *	      service has anything it wants/needs to do at that time.
     */
    virtual void on_restart() { }
    /**
     * @}
     */

    /**
     * Tick.
     */
    virtual void tick() {}

    const health_check_map_t& get_health_checks() const {
        return health_checks;
    }

    void encode_health(const health_check_map_t& next,
                       MonitorDBStore::TransactionRef t) {
        using ceph::encode;
        ceph::buffer::list bl;
        encode(next, bl);
        t->put("health", service_name, bl);
        mon.log_health(next, health_checks, t);
    }

    void load_health();

    /**
     * @defgroup Service_h_store_funcs Back storage interface functions
     * @{
     */
    /**
     * @defgroup Service_h_store_modify Wrapper function interface to access
     *					   the back store for modification
     *					   purposes
     * @{
     */
    void put_first_committed(MonitorDBStore::TransactionRef t, version_t ver) {
        t->put(get_service_name(), first_committed_name, ver);
    }

    /**
     * Set the last committed version to @p ver
     *
     * @param t A transaction to which we add this put operation
     * @param ver The last committed version number being put
     */
    void put_last_committed(MonitorDBStore::TransactionRef t, version_t ver) {
        t->put(get_service_name(), last_committed_name, ver);

        /* We only need to do this once, and that is when we are about to make our
         * first proposal. There are some services that rely on first_committed
         * being set -- and it should! -- so we need to guarantee that it is,
         * specially because the services itself do not do it themselves. They do
         * rely on it, but they expect us to deal with it, and so we shall.
         */
        if (!get_first_committed())
            put_first_committed(t, ver);
    }

    /**
     * Put the contents of @p bl into version @p ver
     *
     * @param t A transaction to which we will add this put operation
     * @param ver The version to which we will add the value
     * @param bl A ceph::buffer::list containing the version's value
     */
    void put_version(MonitorDBStore::TransactionRef t, version_t ver,
                     ceph::buffer::list &bl) {
        t->put(get_service_name(), ver, bl);
    }

    /**
     * Put the contents of @p bl into a full version key for this service, that
     * will be created with @p ver in mind.
     *
     * @param t The transaction to which we will add this put operation
     * @param ver A version number
     * @param bl A ceph::buffer::list containing the version's value
     */
    void put_version_full(MonitorDBStore::TransactionRef t,
                          version_t ver, ceph::buffer::list &bl) {
        std::string key = mon.store->combine_strings(full_prefix_name, ver);
        t->put(get_service_name(), key, bl);
    }

    /**
     * Put the version number in @p ver into the key pointing to the latest full
     * version of this service.
     *
     * @param t The transaction to which we will add this put operation
     * @param ver A version number
     */
    void put_version_latest_full(MonitorDBStore::TransactionRef t, version_t ver) {
        std::string key = mon.store->combine_strings(full_prefix_name, full_latest_name);
        t->put(get_service_name(), key, ver);
    }

    /**
     * Put the contents of @p bl into the key @p key.
     *
     * @param t A transaction to which we will add this put operation
     * @param key The key to which we will add the value
     * @param bl A ceph::buffer::list containing the value
     */
    void put_value(MonitorDBStore::TransactionRef t,
                   const std::string &key, ceph::buffer::list &bl) {
        t->put(get_service_name(), key, bl);
    }

    /**
     * Put integer value @v into the key @p key.
     *
     * @param t A transaction to which we will add this put operation
     * @param key The key to which we will add the value
     * @param v An integer
     */
    void put_value(MonitorDBStore::TransactionRef t,
                   const std::string &key, version_t v) {
        t->put(get_service_name(), key, v);
    }

    /**
     * @}
     */

    /**
     * Get the first committed version
     *
     * @returns Our first committed version (that is available)
     */
    version_t get_first_committed() const {
        return cached_first_committed;
    }

    /**
     * Get the last committed version
     *
     * @returns Our last committed version
     */
    version_t get_last_committed() const {
        return cached_last_committed;
    }

    /**
     * @}
     */

    /**
    * @defgroup PaxosService_h_store_get Wrapper function interface to access
    *					the back store for reading purposes
    * @{
    */

    /**
     * @defgroup PaxosService_h_version_cache Obtain cached versions for this
     *                                        service.
     * @{
     */

    /**
     * Get the contents of a given version @p ver
     *
     * @param ver The version being obtained
     * @param bl The ceph::buffer::list to be populated
     * @return 0 on success; <0 otherwise
     */
    virtual int get_version(version_t ver, ceph::buffer::list &bl) {
        return smr_protocol.read_version_from_service(get_service_name(), ver, bl);
    }

    /**
     * Get the contents of a given full version of this service.
     *
     * @param ver A version number
     * @param bl The ceph::buffer::list to be populated
     * @returns 0 on success; <0 otherwise
     */
    virtual int get_version_full(version_t ver, ceph::buffer::list &bl) {
        std::string key = smr_protocol.combine_strings(full_prefix_name, ver);

        return smr_protocol.read_version_from_service(get_service_name(), key, bl);
    }

    /**
     * Get the latest full version number
     *
     * @returns A version number
     */
    version_t get_version_latest_full() {
        std::string key = smr_protocol.combine_strings(full_prefix_name, full_latest_name);

        return smr_protocol.read_current_from_service(get_service_name(), key);
    }

    /**
     * Get a value from a given key.
     *
     * @param[in] key The key
     * @param[out] bl The ceph::buffer::list to be populated with the value
     */
    int get_value(const std::string &key, ceph::buffer::list &bl) {
        return smr_protocol.read_version_from_service(get_service_name(), key, bl);
    }

    /**
     * Get an integer value from a given key.
     *
     * @param[in] key The key
     */
    version_t get_value(const std::string &key) {
        return smr_protocol.read_current_from_service(get_service_name(), key);
    }

    /**
     * @}
     */

    /**
     * @}
     */
public:

    /**
     * Check if we are proposing a value through the SMR algorithm
     *
     * @returns true if we are proposing; false otherwise.
     */
    virtual bool is_proposing() const {
        return proposing;
    }

    /**
     * Check if we are in the Paxos ACTIVE state.
     *
     * @note This function is a wrapper for Paxos::is_active
     *
     * @returns true if in state ACTIVE; false otherwise.
     */
    bool is_active() const {
        return
                !is_proposing() &&
                (smr_protocol.is_active() || smr_protocol.is_updating() || smr_protocol.is_writing());
    }

    /**
     * Check if we are readable.
     *
     * This mirrors on the paxos check, except that we also verify that
     *
     *  - the client hasn't seen the future relative to this PaxosService
     *  - this service isn't proposing.
     *  - we have committed our initial state (last_committed > 0)
     *
     * @param ver The version we want to check if is readable
     * @returns true if it is readable; false otherwise
     */
    bool is_readable(version_t ver = 0) const {
        if (ver > get_last_committed() ||
            !smr_protocol.is_readable(0) ||
            get_last_committed() == 0)
            return false;
        return true;
    }

    /**
     * Check if we are writeable.
     *
     * We consider to be writeable iff:
     *
     *  - we are not proposing a new version;
     *  - we are ready to be written to -- i.e., we have a pending value.
     *  - paxos is (active or updating or writing or refresh)
     *
     * @returns true if writeable; false otherwise
     */
    bool is_writeable() const {
        return is_active() && have_pending;
    }

    /**
     * Wait for a proposal to finish.
     *
     * Add a callback to be awaken whenever our current proposal finishes being
     * proposed through Paxos.
     *
     * @param c The callback to be awaken once the proposal is finished.
     */
    void wait_for_finished_proposal(MonOpRequestRef op, Context *c) {
        if (op)
            op->mark_event(service_name + ":wait_for_finished_proposal");
        waiting_for_finished_proposal.push_back(c);
    }
    void wait_for_finished_proposal_ctx(Context *c) {
        MonOpRequestRef o;
        wait_for_finished_proposal(o, c);
    }

    /**
     * Wait for us to become active
     *
     * @param c The callback to be awaken once we become active.
     */
    void wait_for_active(MonOpRequestRef op, Context *c) {
        if (op)
            op->mark_event(service_name + ":wait_for_active");

        if (!is_proposing()) {
            smr_protocol.wait_for_active(op, c);
            return;
        }

        wait_for_finished_proposal(op, c);
    }
    void wait_for_active_ctx(Context *c) {
        MonOpRequestRef o;
        wait_for_active(o, c);
    }

    /**
     * Wait for us to become readable
     *
     * @param c The callback to be awaken once we become active.
     * @param ver The version we want to wait on.
     */
    void wait_for_readable(MonOpRequestRef op, Context *c, version_t ver = 0) {
        /* This is somewhat of a hack. We only do check if a version is readable on
         * PaxosService::dispatch(), but, nonetheless, we must make sure that if that
         * is why we are not readable, then we must wait on PaxosService and not on
         * Paxos; otherwise, we may assert on Paxos::wait_for_readable() if it
         * happens to be readable at that specific point in time.
         */
        if (op)
            op->mark_event(service_name + ":wait_for_readable");

        if (is_proposing() ||
            ver > get_last_committed() ||
            get_last_committed() == 0)
            wait_for_finished_proposal(op, c);
        else {
            if (op)
                op->mark_event(service_name + ":wait_for_readable/paxos");

            smr_protocol.wait_for_readable(op, c, ver);
        }
    }

    void wait_for_readable_ctx(Context *c, version_t ver = 0) {
        MonOpRequestRef o; // will initialize the shared_ptr to NULL
        wait_for_readable(o, c, ver);
    }

    /**
     * Wait for us to become writeable
     *
     * @param c The callback to be awaken once we become writeable.
     */
    void wait_for_writeable(MonOpRequestRef op, Context *c) {
        if (op)
            op->mark_event(service_name + ":wait_for_writeable");

        if (is_proposing())
            wait_for_finished_proposal(op, c);
        else if (!is_writeable())
            wait_for_active(op, c);
        else
            smr_protocol.wait_for_writeable(op, c);
    }
    void wait_for_writeable_ctx(Context *c) {
        MonOpRequestRef o;
        wait_for_writeable(o, c);
    }


    /**
     * @defgroup Service_h_Trim Functions for trimming states
     * @{
     */
    /**
     * trim service states if appropriate
     *
     * Called at same interval as tick()
     */
    void maybe_trim();

    /**
     * Auxiliary function to trim our state from version @p from to version
     * @p to, not including; i.e., the interval [from, to[
     *
     * @param t The transaction to which we will add the trim operations.
     * @param from the lower limit of the interval to be trimmed
     * @param to the upper limit of the interval to be trimmed (not including)
     */
    void trim(MonitorDBStore::TransactionRef t, version_t from, version_t to);

    /**
     * encode service-specific extra bits into trim transaction
     *
     * @param tx transaction
     * @param first new first_committed value
     */
    virtual void encode_trim_extra(MonitorDBStore::TransactionRef tx,
                                   version_t first) {}

    /**
     * Get the version we should trim to.
     *
     * Should be overloaded by service if it wants to trim states.
     *
     * @returns the version we should trim to; if we return zero, it should be
     *	      assumed that there's no version to trim to.
     */
    virtual version_t get_trim_to() const {
        return 0;
    }

    /**
     * @}
     */

    /**
 * @defgroup Service_h_Stash_Full
 * @{
 */
    virtual bool should_stash_full();
    /**
     * Encode a full version on @p t
     *
     * @note We force every service to implement this function, since we strongly
     *	   desire the encoding of full versions.
     * @note Services that do not trim their state, will be bound to only create
     *	   one full version. Full version stashing is determined/controlled by
     *	   trimming: we stash a version each time a trim is bound to erase the
     *	   latest full version.
     *
     * @param t Transaction on which the full version shall be encoded.
     */
    virtual void encode_full(MonitorDBStore::TransactionRef t) = 0;

    /**
     * @}
     */

    /**
     * Cancel events.
     *
     * @note This function is a wrapper for Paxos::cancel_events
     */
    void cancel_events() {
        smr_protocol.cancel_events();
    }

};

#endif //CEPH_SERVICE_H
