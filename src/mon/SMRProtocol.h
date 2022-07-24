#ifndef CEPH_SMRPROTOCOL_H
#define CEPH_SMRPROTOCOL_H

#include <stdlib.h>
#include "MonOpRequest.h"
#include "MonitorDBStore.h"
#include "AbstractMonitor.h"

class SMRProtocol {

    AbstractMonitor &mon;

    PerfCounters *logger;


    /**
     * @defgroup Active vars Common active-related member variables
     * @{
     */
    /**
     * When does our read lease expires.
     *
     * Instead of performing a full commit each time a read is requested, we
     * keep leases. Each lease will have an expiration date, which may or may
     * not be extended.
     */
    ceph::real_clock::time_point lease_expire;
    /**
     * List of callbacks waiting for our state to change into STATE_ACTIVE.
     */
    std::list<Context *> waiting_for_active;
    /**
     * List of callbacks waiting for the chance to read a version from us.
     *
     * Each entry on the list may result from an attempt to read a version that
     * wasn't available at the time, or an attempt made during a period during
     * which we could not satisfy the read request. The first case happens if
     * the requested version is greater than our last committed version. The
     * second scenario may happen if we are recovering, or if we don't have a
     * valid lease.
     *
     * The list will be woken up once we change to STATE_ACTIVE with an extended
     * lease -- which can be achieved if we have everyone on the quorum on board
     * with the latest proposal, or if we don't really care about the remaining
     * uncommitted values --, or if we're on a quorum of one.
     */
    std::list<Context *> waiting_for_readable;

    /**
     * @}
     */

    virtual std::string get_name() const = 0;


    virtual void init_logger() = 0;

    virtual void init() = 0;

    virtual bool is_active() = 0;

    virtual bool is_updating() = 0;

    virtual bool is_readable(version_t v) = 0;

    virtual bool is_writing() = 0;

    virtual void wait_for_active(MonOpRequestRef o, Context *c) = 0;

    virtual void wait_for_readable(MonOpRequestRef o, Context *c, version_t ver) = 0;

    virtual void wait_for_writeable(MonOpRequestRef o, Context *c) = 0;

    virtual bool is_writing_previous() = 0;

    virtual void read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
                                               version_t first, version_t last) = 0;

    virtual void dispatch(MonOpRequestRef op) = 0;

    /**
     * Get first committed version
     *
     * @return the first committed version
     */
    virtual version_t get_first_committed() = 0;

    /**
     * Get latest committed version
     *
     * @return latest committed version
     */
    virtual version_t get_version() = 0;

    virtual void leader_init() = 0;

    virtual void peon_init() = 0;

    virtual void dump_info(Formatter *f) = 0;

    virtual void restart() = 0;

    virtual void shutdown() = 0;

    /**
    * Read version @e v and store its value in @e bl
    *
    * @param[in] v The version we want to read
    * @param[out] bl The version's value
    * @return 'true' if we successfully read the value; 'false' otherwise
    */
    virtual bool read(version_t v, ceph::buffer::list &bl) = 0;

    /**
     * Read the latest committed version
     *
     * @param[out] bl The version's value
     * @return the latest committed version if we successfully read the value;
     *	     or 0 (zero) otherwise.
     */
    virtual version_t read_current(ceph::buffer::list &bl) = 0;


    /**
     * Get a transaction to submit operations to propose against
     *
     * Apply operations to this transaction.
     *
     * They will be proposed to the SMR eventually
     */
    virtual MonitorDBStore::TransactionRef get_pending_transaction() = 0;

};


#endif //CEPH_SMRPROTOCOL_H
