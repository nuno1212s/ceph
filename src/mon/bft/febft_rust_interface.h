#ifndef CEPH_FEBFT_RUST_INTERFACE_H
#define CEPH_FEBFT_RUST_INTERFACE_H

#include "mon/MonitorDBStore.h"
#include "febft_interface.h"

//Handle prepare
//Handle commit phase
//Handle executed

void handle_smr_prepare_phase(void *smr, uint64_t seqno) {
    //state updating
}

void handle_smr_committed_phase(void *str, uint64_t seqno) {
    //state writing
}

void handle_smr_executed_phase(void *str, uint64_t seqno) {
    //Force refresh
    //Set state active
}


void set_function(void *dbStore, char *prefix, char *key, SizedData data) {

    auto *store = (MonitorDBStore *) dbStore;

    MonitorDBStore::TransactionRef t;

    auto str_prefix = std::string(prefix);
    auto str_key = std::string(key);

    ceph::buffer::list l;

    l.append((char *) data.data, data.size);

    t->put(str_prefix, str_key, l);

    store->apply_transaction(t);
}

void rm_key_function(void *dbStore, char *prefix, char *key) {

    auto *store = (MonitorDBStore *) dbStore;

    MonitorDBStore::TransactionRef t;

    auto str_prefix = std::string(prefix);
    auto str_key = std::string(key);

    t->erase(str_prefix, str_key);

    store->apply_transaction(t);
}

void rm_range_function(void *dbStore, char *prefix, char *key, char *end) {

    auto *store = (MonitorDBStore *) dbStore;

    MonitorDBStore::TransactionRef t;

    auto str_prefix = std::string(prefix);
    auto str_key = std::string(key);
    auto str_end = std::string(end);

    t->erase_range(str_prefix, str_key, str_end);

    store->apply_transaction(t);
}

void compact_prefix_function(void *dbStore, char *prefix) {

    auto *store = (MonitorDBStore *) dbStore;

    MonitorDBStore::TransactionRef t;

    auto str_prefix = std::string(prefix);

    t->compact_prefix(str_prefix);

    store->apply_transaction(t);
}

void compact_range_function(void *dbStore, char *prefix, char *start, char *end) {

    auto *store = (MonitorDBStore *) dbStore;

    MonitorDBStore::TransactionRef t;

    auto str_prefix = std::string(prefix);
    auto str_key = std::string(start);
    auto str_end = std::string(end);

    t->compact_range(str_prefix, str_key, str_end);

    store->apply_transaction(t);
}

void ctx_result_callback(void *context, int result) {
    auto ctx = (Context *) context;

    ctx->complete(result);
}

void ctx_callback(void *context) {

    auto ctx = (Context *) context;

    ctx->complete(0);
}

std::unique_ptr<SizedData> transform_ceph_buffer_to_rust(const ceph::buffer::list &bl) {

    std::unique_ptr<SizedData> sized_data{new SizedData{(uint8_t *) bl.buffers().front().c_str(), bl.length()}};

    return sized_data;
}

ceph::buffer::list transform_rust_buffer_to_ceph(SizedData &data) {



}

#endif //CEPH_FEBFT_RUST_INTERFACE_H
