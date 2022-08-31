#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>


/// The current version of the wire protocol.
static const uint32_t WireMessage_CURRENT_VERSION = 0;

struct CephClient;

/// Represents a replica in `febft`.
template<typename S = void>
struct Replica;

struct Reply;

struct Request;

/// Represents a sequence number attributed to a client request
/// during a `Consensus` instance.
struct SeqNo;

/// A `NodeId` represents the id of a process in the BFT system.
using NodeId = uint32_t;

struct SizedData {
  uint8_t *data;
  size_t size;
};

using SetFunction = void(*)(void *db, char *prefix, char *key, SizedData data);

using GetFunction = SizedData(*)(void *db, char *prefix, char *key);

using RmKeyFunction = void(*)(void *db, char *prefix, char *key);

using RmKeyRangeFunction = void(*)(void *db, char *prefix, char *start, char *end);

using CompactPrefixFunction = void(*)(void *db, char *prefix);

using CompactRangeFunction = void(*)(void *db, char *prefix, char *start, char *end);

struct KeyValueDB {
  void *db;
  SetFunction set_f;
  GetFunction get_f;
  RmKeyFunction rm_key_f;
  RmKeyRangeFunction rm_range_f;
  CompactPrefixFunction compact_prefix_f;
  CompactRangeFunction compact_range_f;
};

struct CephExecutor;

using CallbackContext = void(*)(void *context);






extern "C" {

void block_on_replica(Replica<CephExecutor> *replica);

Reply *do_blocking_request(CephClient *client, Request *request);

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
CephClient *init_client(uint32_t rank, size_t n, size_t f, CallbackContext callback);

///Initialize a febft replica
///Ceph will
Replica<CephExecutor> *init_replica(uint32_t rank, KeyValueDB *key_val_db);

Request *init_write_compact_prefix(char *prefix);

Request *init_write_compact_range(char *prefix, char *key, char *end);

Request *init_write_erase(char *prefix, char *key);

Request *init_write_erase_range(char *prefix, char *key, char *end);

Request *init_write_put_req(char *prefix, char *key, SizedData data);

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
