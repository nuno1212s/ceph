#include "FebftSmr.h"
#include "FebftMonitor.h"
#include "febft_rust_interface.h"

using std::string;

using ceph::bufferlist;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::to_timespan;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mon.name, mon.rank, name)

static std::ostream &_prefix(std::ostream *_dout, FebftMonitor &mon, const string &name,
                             int rank, const string &febft_name) {
    return *_dout << "mon." << name << "@" << rank
                  << "(" << mon.get_state_name() << ")"
                  << ".febft(" << febft_name << " "
                  << ") ";
}


Transaction *translate_transaction(MonitorDBStore::TransactionRef t);

Transaction *init_read_transaction(const std::string &svc_name, const std::string &key);

FebftSMR::FebftSMR(FebftMonitor &mon, const std::string &name, const std::string &mon_name) : name(name), mon(mon) {

    if (mon_name.find('a') != std::string::npos) {
        this->replica_id = 0;
    } else if (mon_name.find('b') != std::string::npos) {
        this->replica_id = 1;
    } else if (mon_name.find('c') != std::string::npos) {
        this->replica_id = 2;
    } else if (mon_name.find('d') != std::string::npos) {
        this->replica_id = 3;
    } else {
        this->replica_id = 0;
    }

    dout(10) << __func__ << " replica id " << this->replica_id << " for the name " << name << dendl;
}

bool FebftSMR::is_init() const {
    return smr_client != nullptr;
}

bool FebftSMR::is_shutdown() const {
    return smr_client == nullptr;
}

void FebftSMR::init_logger() {}

void FebftSMR::init() {

    this->guard = ::init(4, 4, this->replica_id);

    dout(10) << __func__ << " initializing febft thread" << dendl;

//    std::thread init_febft_thread([this]() {
    std::lock_guard lock(this->smr_lock);

    dout(10) << __func__ << " initializing febft replica with id " << this->replica_id << dendl;

    auto replica_result = ::init_replica(this->replica_id);

    if (replica_result.error != 0) {

        dout(10) << __func__ << " failed to initialize replica with error " << replica_result.error << " and message "
                 << replica_result.str << dendl;

    } else {

        this->replica = replica_result.replica;

        dout(10) << __func__ << " initializing febft client " << dendl;
        this->smr_client = ::init_client(this->replica_id, 4, 1, ::ctx_callback);

    // Define a lambda expression
        auto f = [](Replica<CephExecutor, StrictPersistentLog> *replica) {
            block_on_replica(replica);
        };

        dout(10) << __func__ << " running febft replica " << dendl;
        std::thread replica_thread(f, this->replica);
//    });
    }

}

epoch_t FebftSMR::get_epoch() {
    std::lock_guard lock(this->smr_lock);
    return ::get_view_seq(this->smr_client);
}

int FebftSMR::quorum_age() {
    std::lock_guard lock(this->smr_lock);
    return ::get_quorum_age(this->smr_client);
}

int FebftSMR::get_leader() {
    std::lock_guard lock(this->smr_lock);
    return ::get_leader(this->smr_client);
}

utime_t FebftSMR::get_leader_since() {
    std::lock_guard lock(this->smr_lock);
    return translate_time(::get_leader_since(this->smr_client));
}

bool FebftSMR::is_active() const {
    ceph_assert(ceph_mutex_is_locked(this->smr_lock));

    return ::is_active(this->smr_client);
}

bool FebftSMR::is_updating() const {
    return false;
}

bool FebftSMR::is_readable(version_t v) const {
    bool ret;
    if (v > get_version())
        ret = false;
    else
        ret =
                (mon.is_peon() || mon.is_leader()) &&
                (is_active() || is_updating() || is_writing()) &&
                get_version() > 0; // must have a value alone, or have lease

    dout(5) << __func__ << " = " << (int) ret
            << " - now=" << ceph_clock_now()
            << dendl;
    return ret;
}

bool FebftSMR::is_writeable() {
    std::lock_guard lock(this->smr_lock);
    return ::is_writeable(this->smr_client);
}

bool FebftSMR::is_writing() const {
    ceph_assert(ceph_mutex_is_locked(this->smr_lock));

    return ::is_writing(this->smr_client);
}

void FebftSMR::wait_for_active(MonOpRequestRef o, Context *c) {
    if (o)
        o->mark_event("febft:wait_for_active");

    std::lock_guard lock(this->smr_lock);
    ::wait_for_active(this->smr_client, c);
}

void FebftSMR::wait_for_readable(MonOpRequestRef o, Context *c, version_t ver) {

    if (o)
        o->mark_event("febft:wait_for_readable");

    std::lock_guard lock(this->smr_lock);
    ::wait_for_readable(this->smr_client, c);
}

void FebftSMR::wait_for_writeable(MonOpRequestRef o, Context *c) {
    if (o)
        o->mark_event("febft:wait_for_writeable");

    std::lock_guard lock(this->smr_lock);
    ::wait_for_writeable(this->smr_client, c);
}

bool FebftSMR::is_plugged() const {
    return this->plugged;
}

void FebftSMR::plug() {

    this->plugged = true;
}

void FebftSMR::unplug() {
    this->plugged = false;
}

void FebftSMR::queue_pending_finisher(Context *onfinished) {
    std::lock_guard lock(this->smr_lock);
    ::queue_finisher(this->smr_client, onfinished);
}

void FebftSMR::read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
                                             version_t first, version_t last) {
    dout(10) << __func__ << " first " << first << " last " << last << dendl;
    for (version_t v = first; v <= last; ++v) {
        dout(30) << __func__ << " apply version " << v << dendl;
        bufferlist bl;
        int err = read(v, bl);
        ceph_assert(err == 0);
        ceph_assert(bl.length());
        decode_append_transaction(tx, bl);
    }
    dout(15) << __func__ << " total versions " << (last - first) << dendl;
}

void FebftSMR::dispatch(MonOpRequestRef op) {

    dout(0) << "Got served a message? Ignoring it" << dendl;

}

utime_t FebftSMR::get_last_commit_time() const {

    ceph_assert(ceph_mutex_is_locked(this->smr_lock));

    auto time = ::get_last_committed_time(this->smr_client);

    return translate_time(time);
}

version_t FebftSMR::get_first_committed() const {
    ceph_assert(ceph_mutex_is_locked(this->smr_lock));

    return ::get_first_committed(this->smr_client);
}

version_t FebftSMR::get_version() const {
    ceph_assert(ceph_mutex_is_locked(this->smr_lock));

    return ::get_last_committed(this->smr_client);
}

void FebftSMR::dump_info(Formatter *f) {

}

void FebftSMR::restart() {

}

void FebftSMR::shutdown() {

}

bool FebftSMR::read(const std::string &key, buffer::list &bl) {

    std::lock_guard lock(this->smr_lock);

    auto *transaction = init_read_transaction(get_name(), key);

    auto *reply = do_blocking_request(this->smr_client, transaction);

    if (!is_valid_read_response(reply)) {

        dispose_of_transaction(transaction);
        dispose_of_replies(reply);

        return false;
    }

    SizedData sized_data = read_read_response_from(reply);

    append_rust_buffer_to_ceph_buffer(sized_data, bl, true);

    dispose_of_transaction(transaction);
    dispose_of_replies(reply);

    return true;
}

bool FebftSMR::read(version_t v, buffer::list &bl) {
    std::ostringstream key;
    key << v;

    return read(key.str(), bl);
}

version_t FebftSMR::read_current(buffer::list &bl) {
    if (read(get_version(), bl))
        return get_version();

    return 0;
}

int FebftSMR::read_version_from_service(const std::string &service_name, const std::string &key, buffer::list &bl) {

    std::lock_guard lock(this->smr_lock);
    auto *transaction = init_read_transaction(service_name, key);

    auto *reply = do_blocking_request(this->smr_client, transaction);

    if (!is_valid_read_response(reply)) {

        dispose_of_transaction(transaction);
        dispose_of_replies(reply);

        return -ENOENT;
    }

    SizedData sized_data = read_read_response_from(reply);

    append_rust_buffer_to_ceph_buffer(sized_data, bl, true);

    dispose_of_transaction(transaction);
    dispose_of_replies(reply);

    return 0;
}

int FebftSMR::read_version_from_service(const std::string &service_name, version_t v, buffer::list &bl) {
    std::ostringstream key;

    key << v;

    return read_version_from_service(service_name, key.str(), bl);
}

version_t FebftSMR::read_current_from_service(const std::string &service_name, const std::string &key) {
    using ceph::decode;
    std::lock_guard lock(this->smr_lock);

    auto *transaction = init_read_transaction(service_name, key);

    auto *reply = do_blocking_request(this->smr_client, transaction);

    if (!is_valid_read_response(reply)) {

        dispose_of_transaction(transaction);
        dispose_of_replies(reply);

        return false;
    }

    SizedData sized_data = read_read_response_from(reply);

    ceph::buffer::list bl;

    append_rust_buffer_to_ceph_buffer(sized_data, bl, true);

    dispose_of_transaction(transaction);
    dispose_of_replies(reply);

    ceph_assert(bl.length());
    version_t ver;
    auto p = bl.cbegin();
    decode(ver, p);
    return ver;
}

bool FebftSMR::exists_in_service(const std::string &service_name, const std::string &key) {

    ceph::buffer::list bl;

    return read_version_from_service(service_name, key, bl) == 0;
}

MonitorDBStore::TransactionRef FebftSMR::get_pending_transaction() {
    //We do not need to check if we are the leader, since febft works with clients
    //And therefore all monitors can propose operations to the SMR
    if (!pending_operation) {
        pending_operation.reset(new MonitorDBStore::Transaction);
    }

    return pending_operation;
}

void FebftSMR::propose_pending() {
    std::lock_guard lock(this->smr_lock);

    Transaction *result = translate_transaction(this->pending_operation);

    TransactionReply *reply = do_blocking_request(this->smr_client, result);

    if (!is_valid_read_response(reply)) {
        dout(10) << __func__ << "Failed to propose " << dendl;
    }

}

bool FebftSMR::trigger_propose() {

    if (is_plugged()) {
        return false;
    } else if (is_active()) {
        propose_pending();
        return true;
    } else {
        return false;
    }
}

void FebftSMR::cancel_events() {

}

MonitorDBStore *FebftSMR::get_store() {
    return nullptr;
}

std::string FebftSMR::get_name() const {
    return std::string();
}

Transaction *init_read_transaction(const std::string &svc_name, const std::string &key) {

    CephRequest *req = init_read_req(svc_name.c_str(), key.c_str());

    auto **reqs = (CephRequest **) malloc(sizeof(CephRequest *) * 1);

    reqs[0] = req;

    return init_transaction(reqs, 1);
}

Transaction *translate_transaction(MonitorDBStore::TransactionRef t) {

    //TODO: Maybe don't use mallocs?
    auto **requests = (CephRequest **) malloc(sizeof(CephRequest *) * t->ops.size());

    int i = 0;

    for (auto it = t->ops.begin(); it != t->ops.end(); ++it) {

        const MonitorDBStore::Op &op = *it;

        switch (op.type) {
            case MonitorDBStore::Transaction::OP_PUT: {

                auto prefix = op.prefix;
                auto key = op.key;

                std::unique_ptr<SizedData> data = transform_ceph_buffer_to_rust(op.bl);

                auto req = init_write_put_req(prefix.c_str(), key.c_str(), &*data);

                requests[i] = req;

                break;
            }
            case MonitorDBStore::Transaction::OP_ERASE: {
                auto prefix = op.prefix;
                auto key = op.key;

                auto req = init_write_erase(prefix.c_str(), key.c_str());

                requests[i] = req;

                break;
            }
            case MonitorDBStore::Transaction::OP_ERASE_RANGE: {
                auto prefix = op.prefix;
                auto key = op.key;
                auto end = op.endkey;

                auto req = init_write_erase_range(prefix.c_str(), key.c_str(), end.c_str());

                requests[i] = req;

                break;
            }
            case MonitorDBStore::Transaction::OP_COMPACT: {
                break;
            }
            default: {
            }
        }

        i++;
    }

    Transaction *trPointer = init_transaction(requests, i);

    return trPointer;
}

