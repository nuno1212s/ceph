#include "FebftSmr.h"
#include "FebftMonitor.h"

using std::string;

using ceph::bufferlist;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::to_timespan;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mon.name, mon.rank, paxos_name, state, first_committed, last_committed)

static std::ostream &_prefix(std::ostream *_dout, FebftMonitor &mon, const string &name,
                             int rank, const string &febft_name, int state,
                             version_t first_committed, version_t last_committed) {
    return *_dout << "mon." << name << "@" << rank
                  << "(" << mon.get_state_name() << ")"
                  << ".paxos(" << febft_name << " "
                  << " c " << first_committed << ".." << last_committed
                  << ") ";
}


Transaction *translate_transaction(MonitorDBStore::TransactionRef t);

Transaction *init_read_transaction(const std::string &svc_name, const std::string &key);

FebftSMR::FebftSMR(FebftMonitor &mon, const std::string &name) : name(name), mon(mon) {

    //TODO: How to get this?
    replica_id = 2;

    int n = 4, f = 1;

    replica = init_replica(replica_id);

    //TODO: Init main replica loop thread.

    smr_client = init_client(replica_id, n, f, ctx_callback);
}

void FebftSMR::init_logger() {

}

void FebftSMR::init() {

    this->replica = ::init_replica(this->replica_id);

    this->smr_client = ::init_client(this->replica_id, 4, 1, ::ctx_callback);
}

bool FebftSMR::is_active() const {
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
                last_committed > 0 && is_lease_valid(); // must have a value alone, or have lease
    dout(5) << __func__ << " = " << (int) ret
            << " - now=" << ceph_clock_now()
            << " lease_expire=" << lease_expire
            << " has v" << v << " lc " << last_committed
            << dendl;
    return ret;
}

bool FebftSMR::is_writeable() {
    return ::is_writeable(this->smr_client);
}

bool FebftSMR::is_writing() const {
    return ::is_writing(this->smr_client);
}

void FebftSMR::wait_for_active(MonOpRequestRef o, Context *c) {
    if (o)
        o->mark_event("febft:wait_for_active");

    ::wait_for_active(this->smr_client, c);
}

void FebftSMR::wait_for_readable(MonOpRequestRef o, Context *c, version_t ver) {

    if (o)
        o->mark_event("febft:wait_for_readable");

    ::wait_for_readable(this->smr_client, c);
}

void FebftSMR::wait_for_writeable(MonOpRequestRef o, Context *c) {
    if (o)
        o->mark_event("febft:wait_for_writeable");

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

    auto time = ::get_last_committed_time(this -> smr_client);



    utime_t()
}

version_t FebftSMR::get_first_committed() const {
    return ::get_first_committed(this->smr_client);
}

version_t FebftSMR::get_version() const {
    return ::get_last_committed(this->smr_client);
}

void FebftSMR::dump_info(Formatter *f) {

}

void FebftSMR::restart() {

}

void FebftSMR::shutdown() {

}

bool FebftSMR::read(const std::string &key, buffer::list &bl) {

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
    return get_store()->exists(service_name, key);
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

