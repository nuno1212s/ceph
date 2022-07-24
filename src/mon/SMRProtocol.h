#ifndef CEPH_SMRPROTOCOL_H
#define CEPH_SMRPROTOCOL_H

#include <stdlib.h>
#include "MonOpRequest.h"
#include "MonitorDBStore.h"
#include "AbstractMonitor.h"

class SMRProtocol {
    /**
     * @}
     */

    virtual std::string get_name() const = 0;

public:
    virtual void init_logger() = 0;

    virtual void init() = 0;

    virtual bool is_active() const = 0;

    virtual bool is_updating() const = 0;

    /**
     * Check if a given version is readable.
     *
     * A version may not be readable for a myriad of reasons:
     *  @li the version @e v is higher that the last committed version
     *  @li we are not the Leader nor a Peon (election may be on-going)
     *  @li we do not have a committed value yet
     *  @li we do not have a valid lease
     *
     * @param seen The version we want to check if it is readable.
     * @return 'true' if the version is readable; 'false' otherwise.
     */
    virtual bool is_readable(version_t v) const = 0;

    /**
     * Check if we are writeable.
     *
     * We are writeable if we are alone (i.e., a quorum of one), or if we match
     * all the following conditions:
     *  @li We are the Leader
     *  @li We are on STATE_ACTIVE
     *  @li We have a valid lease
     *
     * @return 'true' if we are writeable; 'false' otherwise.
     */
    virtual bool is_writeable() = 0;

    virtual bool is_writing() const = 0;

    /**
     * Add c to the list of callbacks waiting for us to become active.
     *
     * @param c A callback
     */
    virtual void wait_for_active(MonOpRequestRef o, Context *c) = 0;

    void wait_for_active(Context *c) {
        MonOpRequestRef o;
        wait_for_active(o, c);
    }

    /**
     * Add onreadable to the list of callbacks waiting for us to become readable.
     *
     * @param onreadable A callback
     */
    virtual void wait_for_readable(MonOpRequestRef o, Context *c, version_t ver) = 0;

    void wait_for_readable(Context *onreadable, version_t ver) {
        MonOpRequestRef o;
        wait_for_readable(o, onreadable, ver);
    }

    /**
    * Add c to the list of callbacks waiting for us to become writeable.
    *
    * @param c A callback
    */
    virtual void wait_for_writeable(MonOpRequestRef o, Context *c) = 0;

    void wait_for_writeable(Context *c) {
        MonOpRequestRef o;
        wait_for_writeable(o, c);
    }

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

    /**
     * dump state info to a formatter
     */
    virtual void dump_info(Formatter *f) = 0;

    virtual void restart() = 0;

    virtual void shutdown() = 0;

    /**
    * Read key @e key and store its value in @e bl
    *
    * @param[in] key The key we want to read
    * @param[out] bl The version's value
    * @return 'true' if we successfully read the value; 'false' otherwise
    */
    virtual bool read(const std::string &key, ceph::buffer::list &bl) = 0;

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

    /**
     * Cancel all of Paxos' timeout/renew events.
     */
    virtual void cancel_events() = 0;
};


#endif //CEPH_SMRPROTOCOL_H
