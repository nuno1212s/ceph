#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>


/// The current version of the wire protocol.
static const uint32_t WireMessage_CURRENT_VERSION = 0;

struct CephClient;

struct CephExecutor;

struct CephRequest;

struct NoPersistentLog;

/// Represents a replica in `febft`.
template<typename S = void, typename T = void>
struct Replica;


/// Represents a sequence number attributed to a client request
/// during a `Consensus` instance.
struct SeqNo;

/// Request Message List This has to exist because ceph handles things by having a global transaction where all servers dump their info and then Proposing this transaction (with many operations). This is kind of done by febft with its batching, so it would almost make sense to have one client per service, but that would probably require a pretty decent change so it's much easier if we also just support lists of requests on the SMR level
struct Transaction;

struct TransactionReply;

/// The result of attempting to initialize a client
struct ClientResult {
  CephClient *client;
  uint32_t error;
  char *str;
};

using CallbackContext = void(*)(void *context);

using HandleComittedInSMR = void(*)(void *monitor, uint64_t seq_no);

/// The result of attempting to initialize a replica
struct ReplicaResult {
  Replica<CephExecutor, NoPersistentLog> *replica;
  uint32_t error;
  char *str;
};

struct SizedData {
  const uint8_t *data;
  size_t size;
};

extern "C" {

///Dispose of the given replies. This will deallocate the memory
/// Corresponding to these replies
void dispose_of_replies(TransactionReply *replies);

///Dispose of the given transaction. This will deallocate the memory
/// corresponding to the replies that are contained within
void dispose_of_transaction(Transaction *transaction);

TransactionReply *do_blocking_request(CephClient *client, Transaction *request);

void febft_shutdown(void *guard);

uint64_t get_first_committed(CephClient *client);

uint64_t get_last_committed(CephClient *client);

uint64_t get_last_committed_time(CephClient *client);

uint32_t get_leader(CephClient *client);

uint64_t get_leader_since(CephClient *client);

uint64_t get_quorum_age(CephClient *client);

uint32_t get_view_seq(CephClient *client);

void *init(size_t threadpool_threads, size_t async_threads, size_t replica_id);

///Initialize a febft client
///Ceph will then use this client to propose operations on the SMR
ClientResult init_client(uint32_t rank,
                         size_t n,
                         size_t f,
                         CallbackContext callback,
                         void *smr_ref,
                         HandleComittedInSMR handle_committed);

///Initialize a read request
CephRequest *init_read_req(const char *prefix, const char *key);

///Initialize a febft replica
///Ceph will not interact with the generated replica, only with the client.
/// This replica will continue to run "independently" of ceph
ReplicaResult init_replica(uint32_t rank);

///Initialize a transaction object with the given requests
///Requests should be passed in an array (C Style) with the corresponding size
Transaction *init_transaction(CephRequest *const *requests, size_t request_count);

CephRequest *init_write_erase(const char *prefix, const char *key);

CephRequest *init_write_erase_range(const char *prefix, const char *key, const char *end);

CephRequest *init_write_put_req(const char *prefix, const char *key, const SizedData *data);

bool is_active(CephClient *client);

bool is_leader(CephClient *client);

///Checks if the given transaction reply is valid.
/// Atm we only allow one read request per transaction because of how we are using it in ceph, but this could/should
/// be changed in the future. Also we can change to unordered requests for faster responses
bool is_valid_read_response(TransactionReply *response);

///Checks if the given transaction reply has all valid responses.
/// If a single transaction was not successfull this reports.
/// In theory, ceph's model kinda expects ACID transactions, but that would be for a later time, and would be
/// at the executor level.
bool is_valid_write_response(TransactionReply *response);

bool is_writeable(CephClient *client);

bool is_writing(CephClient *client);

void queue_finisher(CephClient *client, void *context);

///Read the read response from a given ceph reply
///
/// Note that the sized data returned from here will only live as long
/// as the transaction reply
SizedData read_read_response_from(TransactionReply *response);

void start_replica_thread(Replica<CephExecutor, NoPersistentLog> *replica);

void wait_for_active(CephClient *client, void *context);

void wait_for_readable(CephClient *client, void *context);

void wait_for_writeable(CephClient *client, void *context);

} // extern "C"
