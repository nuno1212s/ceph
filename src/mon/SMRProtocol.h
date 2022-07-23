#ifndef CEPH_SMRPROTOCOL_H
#define CEPH_SMRPROTOCOL_H



class SMRProtocol {

    virtual std::string get_name() const = 0;

    virtual version_t get_version() = 0;

    virtual void init_logger() = 0;

    virtual void init() = 0;

    virtual bool is_active() = 0;

    virtual bool is_writing() = 0;

    virtual bool is_writing_previous() = 0;

    virtual void read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
                                              version_t first, version_t last) = 0;

    virtual void dispatch(MonOpRequestRef op) = 0;

    virtual void trigger_propose() = 0;

    virtual version_t get_first_committed() = 0;

    virtual void leader_init() = 0;

    virtual void peon_init() = 0;

    virtual MonitorDBStore::TransactionRef get_pending_transaction() = 0;

    virtual void dump_info(Formatter *f) = 0;

    virtual void restart() = 0;

    virtual void shutdown() = 0;

};


#endif //CEPH_SMRPROTOCOL_H
