#ifndef CEPH_FEBFT_RUST_INTERFACE_H
#define CEPH_FEBFT_RUST_INTERFACE_H

#include "mon/MonitorDBStore.h"
#include "febft_interface.h"

//Handle prepare
//Handle commit phase
//Handle executed
SizedData *transform_ceph_buffer_to_rust(const ceph::buffer::list &bl);

ceph::buffer::list transform_rust_buffer_to_ceph(SizedData &data, bool copy = false);

void handle_refresh(void *smr, uint64_t seqno) {
    auto febft_smr = (FebftSMR *) smr;

    febft_smr->handle_committed_values(seqno);
}

KVGetResult get_from_db(void *db, char *prefix, char *key) {

    KVGetResult get_result {};

    auto kv_db = (MonitorDBStore *) db;

    std::string prefix_str(prefix);
    std::string key_str(key);

    ceph::buffer::list bl;

    int err = kv_db->get(prefix_str, key_str, bl);

    if (err != 0) {
        get_result.err = err;
        get_result.errormsg = "Failed to get prefix + key from db";
        get_result.result = nullptr;
    } else {
        get_result.result = transform_ceph_buffer_to_rust(bl);
        get_result.err = 0;
        get_result.errormsg = nullptr;
    }

    return get_result;
}

KVSetFunction set_db(void *db, char *prefix, char *key, SizedData data) {

    KVSetFunction set_result {};

    auto kv_db = (MonitorDBStore *) db;

    std::string prefix_str(prefix);
    std::string key_str(key);

    auto bl = transform_rust_buffer_to_ceph(data, true);

    auto t(std::make_shared<MonitorDBStore::Transaction>());

    t->put(prefix_str, key_str, bl);

    kv_db->apply_transaction(t);

    //TODO: See if there were any errors

    set_result.err = 0;
    set_result.errormsg = nullptr;

    return set_result;
}

KVRMResult rm_key_db(void *db, char*prefix, char*key) {

    KVRMResult rm_result {};


    auto kv_db = (MonitorDBStore *) db;

    std::string prefix_str(prefix);
    std::string key_str(key);

    auto t(std::make_shared<MonitorDBStore::Transaction>());

    t->erase(prefix_str, key_str);

    kv_db->apply_transaction(t);

    //TODO: See if there were any errors

    rm_result.err = 0;
    rm_result.errormsg = nullptr;

    return rm_result;
}

KVRMRangeResult  rm_range_db(void *db, char *prefix, char* start, char* end) {

    KVRMRangeResult rm_result {};

    auto kv_db = (MonitorDBStore *) db;

    std::string prefix_str(prefix);
    std::string start_str(start);
    std::string end_str(end);

    auto t(std::make_shared<MonitorDBStore::Transaction>());

    t->erase_range(prefix_str, start_str, end_str);

    kv_db->apply_transaction(t);

    //TODO: See if there were any errors

    rm_result.err = 0;
    rm_result.errormsg = nullptr;

    return rm_result;
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
SizedData *transform_ceph_buffer_to_rust(const ceph::buffer::list &bl) {
    auto *sized_data = new SizedData{(uint8_t *) bl.buffers().front().c_str(), bl.length()};

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
