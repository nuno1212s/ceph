#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>


/// The current version of the wire protocol.
static const uint32_t WireMessage_CURRENT_VERSION = 0;

/// The current version of the wire protocol.
static const uint32_t WireMessage_CURRENT_VERSION = 0;

struct CephClient;

struct CephExecutor;

struct CephRequest;

/// Represents a replica in `febft`.
template<typename S = void, typename T = void>
struct Replica;

/// Represents a replica in `febft`.
template<typename S = void, typename T = void>
struct Replica;

/// Represents a sequence number attributed to a client request
/// during a `Consensus` instance.
struct SeqNo;

/// Represents a sequence number attributed to a client request
/// during a `Consensus` instance.
struct SeqNo;

///Strict log mode initializer
struct StrictPersistentLog;

///Strict log mode initializer
struct StrictPersistentLog;

/// Request Message List This has to exist because ceph handles things by having a global transaction where all servers dump their info and then Proposing this transaction (with many operations). This is kind of done by febft with its batching, so it would almost make sense to have one client per service, but that would probably require a pretty decent change so it's much easier if we also just support lists of requests on the SMR level
struct Transaction;

struct TransactionReply;

struct SizedData {
    uint8_t *data;
    size_t size;
};

using SetFunction = void (*)(void *db, char *prefix, char *key, SizedData data);

using GetFunction = SizedData(*)(void *db, char *prefix, char *key);

using RmKeyFunction = void (*)(void *db, char *prefix, char *key);

using RmKeyRangeFunction = void (*)(void *db, char *prefix, char *start, char *end);

using CompactPrefixFunction = void (*)(void *db, char *prefix);

using CompactRangeFunction = void (*)(void *db, char *prefix, char *start, char *end);

struct KeyValueDB {
    void *db;
    SetFunction set_f;
    GetFunction get_f;
    RmKeyFunction rm_key_f;
    RmKeyRangeFunction rm_range_f;
    CompactPrefixFunction compact_prefix_f;
    CompactRangeFunction compact_range_f;
};

using CallbackContext = void (*)(void *context);






extern "C" {

void block_on_replica(Replica<CephExecutor, StrictPersistentLog> *replica);

TransactionReply *do_blocking_request(CephClient *client, Transaction *request);

uint64_t get_first_committed(CephClient *client);

uint64_t get_last_committed(CephClient *client);

uint64_t get_last_committed_time(CephClient *client);

void *init(size_t threadpool_threads, size_t async_threads);

KeyValueDB *initKVDB(void *db,
                     SetFunction set,
                     GetFunction get,
                     RmKeyFunction rm_key,
                     RmKeyRangeFunction rm_range,
                     CompactPrefixFunction compact_prefix,
                     CompactRangeFunction compact_range);

///Initialize a febft client
///Ceph will then use this client to propose operations on the SMR
CephClient *init_client(uint32_t rank,
                        size_t n,
                        size_t f,
                        CallbackContext callback,
                        uint64_t first_seq,
                        uint64_t last_seq,
                        uint64_t last_seq_time);

///Initialize a febft replica
///Ceph will not interact with the generated replica, only with the client.
/// This replica will continue to run "independently" of ceph
Replica<CephExecutor, StrictPersistentLog> *init_replica(uint32_t rank);

///Initialize a transaction object with the given requests
///Requests should be passed in an array (C Style) with the corresponding size
Transaction *init_transaction(CephRequest **requests, size_t request_count);

CephRequest *init_write_erase(char *prefix, char *key);

CephRequest *init_write_erase_range(char *prefix, char *key, char *end);

CephRequest *init_write_put_req(char *prefix, char *key, SizedData data);

bool is_active(CephClient *client);

bool is_leader(CephClient *client);

bool is_writeable(CephClient *client);

bool is_writing(CephClient *client);

void queue_finisher(CephClient *client, void *context);

void shutdown(void *guard);

void wait_for_active(CephClient *client, void *context);

void wait_for_readable(CephClient *client, void *context);

void wait_for_writeable(CephClient *client, void *context);

} // extern "C"
