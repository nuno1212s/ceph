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

/**
 * Returns a unique ptr to a sized data obj that in turn points to the data stored by a ceph::buffer::list.
 *
 * The memory contained within the sized data buffer will remain alive for as long as @param bl is alive.
 *
 * @param bl
 * @return
 */
std::unique_ptr<SizedData> transform_ceph_buffer_to_rust(const ceph::buffer::list &bl) {

    std::unique_ptr<SizedData> sized_data{new SizedData{(uint8_t *) bl.buffers().front().c_str(), bl.length()}};

    return sized_data;
}


/**
 * Transforms a rust compatible data buffer into a ceph compatible data buffer
 *
 * If @param copy is false, then the underlying memory stored in the ceph::buffer will
 * be discarded when the sized data data is discarded (this will then depend on where this memory came from initially)
 *
 * If @param copy is true, then the underlying memory will be copied and therefore will live for as long as the ceph buffer.
 *
 * @param data
 * @param copy
 * @return
 */
void append_rust_buffer_to_ceph_buffer(SizedData &data, ceph::buffer::list &bl, bool copy = false) {

    if (copy) {
        auto mem = (char *) malloc(sizeof(char) * data.size);

        memcpy(mem, data.data, data.size);

        bl.append(mem, data.size);
    } else {
        bl.append((char *) data.data, data.size);
    }
}

/**
 * Transforms a rust compatible data buffer into a ceph compatible data buffer
 *
 * If @param copy is false, then the underlying memory stored in the ceph::buffer will
 * be discarded when the sized data data is discarded (this will then depend on where this memory came from initially)
 *
 * If @param copy is true, then the underlying memory will be copied and therefore will live for as long as the ceph buffer.
 *
 * @param data
 * @param copy
 * @return
 */
ceph::buffer::list transform_rust_buffer_to_ceph(SizedData &data, bool copy = false) {

    ceph::buffer::list bl;

    if (copy) {
        auto mem = (char *) malloc(sizeof(char) * data.size);

        memcpy(mem, data.data, data.size);

        bl.append(mem, data.size);
    } else {
        bl.append((char *) data.data, data.size);
    }

    return bl;
}

utime_t translate_time(uint64_t time_in_ms) {

    uint32_t seconds = time_in_ms / 1000;

    uint32_t nanos = time_in_ms - seconds;

    return {seconds, static_cast<int>(nanos)};
}

#endif //CEPH_FEBFT_RUST_INTERFACE_H
