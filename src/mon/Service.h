#ifndef CEPH_SERVICE_H
#define CEPH_SERVICE_H

#include "Monitor.h"

class Service {

public:

    /**
     * Reference to the monitor class that is associated with this service
     */
    Monitor &mon;

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
    Service(Monitor &mon, std::string name) : mon(mon), service_name(name), proposing(false),
                                              last_committed_name("last_committed"),
                                              first_committed_name("first_committed"),
                                              full_prefix_name("full"),
                                              full_latest_name("latest"),
                                              service_version(0), have_pending(false),
                                              cached_first_committed(0), cached_last_committed(0) {}

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
};

#endif //CEPH_SERVICE_H
