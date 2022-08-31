#ifndef CEPH_FEBFTSMR_H
#define CEPH_FEBFTSMR_H

#include "mon/SMRProtocol.h"

class FebftSMR : public SMRProtocol {

protected:
    std::string name;

    MonitorDBStore::TransactionRef pending_operation;

public:
    void init_logger() override;

    void init() override;

    bool is_active() const override;

    bool is_updating() const override;

    bool is_readable(version_t v) const override;

    bool is_writeable() override;

    bool is_writing() const override;

    void wait_for_active(MonOpRequestRef o, Context *c) override;

    void wait_for_readable(MonOpRequestRef o, Context *c, version_t ver) override;

    void wait_for_writeable(MonOpRequestRef o, Context *c) override;

    bool is_plugged() const override;

    void plug() override;

    void unplug() override;

    void queue_pending_finisher(Context *onfinished) override;

    void read_and_prepare_transactions(MonitorDBStore::TransactionRef tx, version_t first, version_t last) override;

    void dispatch(MonOpRequestRef op) override;

    utime_t get_last_commit_time() override;

    version_t get_first_committed() override;

    version_t get_version() override;

    void dump_info(Formatter *f) override;

    void restart() override;

    void shutdown() override;

    bool read(const std::string &key, buffer::list &bl) override;

    bool read(version_t v, buffer::list &bl) override;

    version_t read_current(buffer::list &bl) override;

    int read_version_from_service(const std::string &service_name, const std::string &key, buffer::list &bl) override;

    int read_version_from_service(const std::string &service_name, version_t v, buffer::list &bl) override;

    version_t read_current_from_service(const std::string &service_name, const std::string &key) override;

    bool exists_in_service(const std::string &service_name, const std::string &key) override;

    MonitorDBStore::TransactionRef get_pending_transaction() override;

    bool trigger_propose() override;

    void cancel_events() override;

protected:
    MonitorDBStore *get_store() override;

    ~FebftSMR() override = default;

private:
    std::string get_name() const override;

};


#endif //CEPH_FEBFTSMR_H
