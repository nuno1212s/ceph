#ifndef CEPH_SMRPROTOCOL_H
#define CEPH_SMRPROTOCOL_H

#include <stdlib.h>
#include "MonOpRequest.h"
#include "MonitorDBStore.h"
#include "AbstractMonitor.h"

class SMRProtocol {
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

    /**
     * Check if we are currently in the Writing phase of the SMR
     *
     * We are writing if:
     *
     * @li We are currently in the middle of a consensus instance of the SMR protocol
     *
     * @return 'true' if we are writing; 'false' otherwise
     */
    virtual bool is_writing() const = 0;

    /**
     * Check if we are currently Writing to disk a previous phase of the SMR protocol
     *
     * This is in particular important for the Paxos protocol.
     *
     * @return
     */
    virtual bool is_writing_previous() const = 0;

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

    /**
     * Read and prepare the given transaction
     */
    virtual void read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
                                               version_t first, version_t last) = 0;

    /**
     * Execute a given request in this SMR protocol
     */
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
     * Read a value for a service / key combination
     *
     * @param[in] service_name The name of the service
     * @param[in] key The key that we want to retrieve
     * @param[out] bl Where to put the result
     * @return 'true' if we successfully read the value; 'false' otherwise
     */
    virtual int
    read_version_from_service(const std::string &service_name, const std::string &key, buffer::list &bl) = 0;

    /**
     * Read a value for a service / version combination
     *
     * @param[in] service_name The name of the service
     * @param[in] v The key that we want to retrieve
     * @param[out] bl Where to put the result
     * @return 'true' if we successfully read the value; 'false' otherwise
     */
    virtual int read_version_from_service(const std::string &service_name, version_t v, buffer::list &bl) = 0;

    /**
     * Read the latest committed version
     *
     * @param[in] service_name The name of the service
     * @param[in] key The key to get the latest version for
     * @return The latest version present for the given service/key combination. Returns 0 if no value was read
     */
    virtual version_t read_current_from_service(const std::string &service_name,
                                                const std::string &key) = 0;

    /**
     * Get a transaction to submit operations to propose against
     *
     * Apply operations to this transaction.
     *
     * They will be proposed to the SMR eventually
     */
    virtual MonitorDBStore::TransactionRef get_pending_transaction() = 0;

    /**
    * (try to) trigger a proposal
    *
    * Tell the SMR Protocol that it should submit the pending proposal.  Note that if it
    * is not active (e.g., because it is already in the midst of committing
    * something) that will be deferred (e.g., until the current round finishes).
    */
    virtual bool trigger_propose() = 0;

    /**
     * Cancel all of Paxos' timeout/renew events.
     */
    virtual void cancel_events() = 0;

    /**
 * Combine two keys to generate a new key that can be searched in the service store
 * @param prefix
 * @param suffix
 * @return
 */
    std::string combine_strings(const std::string &prefix, const std::string &suffix) {
        return get_store()->combine_strings(prefix, suffix);
    }

    std::string combine_strings(const std::string &prefix, const version_t ver) {
        std::ostringstream os;
        os << ver;
        return combine_strings(prefix, os.str());
    }

protected:
    virtual MonitorDBStore *get_store() = 0;
};


#endif //CEPH_SMRPROTOCOL_H
