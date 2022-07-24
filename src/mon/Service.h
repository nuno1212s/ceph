#ifndef CEPH_SERVICE_H
#define CEPH_SERVICE_H

#include "Monitor.h"
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
     * @defgroup PaxosService_h_store_keys Set of keys that are usually used on
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

    class C_ReplyOp : public C_MonOp {
        Monitor &mon;
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

    /**
     * Dispatch a message by passing it to several different functions that are
     * either implemented directly by this service, or that should be implemented
     * by the class implementing this service.
     *
     * @param m A message
     * @returns 'true' on successful dispatch; 'false' otherwise.
     */
    bool dispatch(MonOpRequestRef op);


    /**
     * @defgroup PaxosService_h_store_funcs Back storage interface functions
     * @{
     */
    /**
     * @defgroup PaxosService_h_store_modify Wrapper function interface to access
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
        return mon.store->get(get_service_name(), ver, bl);
    }

    /**
     * Get the contents of a given full version of this service.
     *
     * @param ver A version number
     * @param bl The ceph::buffer::list to be populated
     * @returns 0 on success; <0 otherwise
     */
    virtual int get_version_full(version_t ver, ceph::buffer::list &bl) {
        std::string key = mon.store->combine_strings(full_prefix_name, ver);
        return mon.store->get(get_service_name(), key, bl);
    }

    /**
     * Get the latest full version number
     *
     * @returns A version number
     */
    version_t get_version_latest_full() {
        std::string key = mon.store->combine_strings(full_prefix_name, full_latest_name);
        return mon.store->get(get_service_name(), key);
    }

    /**
     * Get a value from a given key.
     *
     * @param[in] key The key
     * @param[out] bl The ceph::buffer::list to be populated with the value
     */
    int get_value(const std::string &key, ceph::buffer::list &bl) {
        return mon.store->get(get_service_name(), key, bl);
    }

    /**
     * Get an integer value from a given key.
     *
     * @param[in] key The key
     */
    version_t get_value(const std::string &key) {
        return mon.store->get(get_service_name(), key);
    }

    /**
     * @}
     */

    /**
     * @}
     */

};

#endif //CEPH_SERVICE_H
