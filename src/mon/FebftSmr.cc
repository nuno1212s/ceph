#include "FebftSmr.h"

void FebftSMR::init_logger() {

}

void FebftSMR::init() {

}

bool FebftSMR::is_active() const {
    return false;
}

bool FebftSMR::is_updating() const {
    return false;
}

bool FebftSMR::is_readable(version_t v) const {
    return false;
}

bool FebftSMR::is_writeable() {
    return false;
}

bool FebftSMR::is_writing() const {
    return false;
}

bool FebftSMR::is_writing_previous() const {
    return false;
}

void FebftSMR::wait_for_active(MonOpRequestRef o, Context *c) {

}

void FebftSMR::wait_for_readable(MonOpRequestRef o, Context *c, version_t ver) {

}

void FebftSMR::wait_for_writeable(MonOpRequestRef o, Context *c) {

}

bool FebftSMR::is_plugged() const {
    return false;
}

void FebftSMR::plug() {

}

void FebftSMR::unplug() {

}

void FebftSMR::queue_pending_finisher(Context *onfinished) {

}

void FebftSMR::read_and_prepare_transactions(MonitorDBStore::TransactionRef tx, version_t first, version_t last) {

}

void FebftSMR::dispatch(MonOpRequestRef op) {

}

utime_t FebftSMR::get_last_commit_time() {
    return utime_t();
}

version_t FebftSMR::get_first_committed() {
    return 0;
}

version_t FebftSMR::get_version() {
    return 0;
}

void FebftSMR::dump_info(Formatter *f) {

}

void FebftSMR::restart() {

}

void FebftSMR::shutdown() {

}

bool FebftSMR::read(const std::string &key, buffer::list &bl) {
    return false;
}

bool FebftSMR::read(version_t v, buffer::list &bl) {
    return false;
}

version_t FebftSMR::read_current(buffer::list &bl) {
    return 0;
}

int FebftSMR::read_version_from_service(const std::string &service_name, const std::string &key, buffer::list &bl) {
    return 0;
}

int FebftSMR::read_version_from_service(const std::string &service_name, version_t v, buffer::list &bl) {
    return 0;
}

version_t FebftSMR::read_current_from_service(const std::string &service_name, const std::string &key) {
    return 0;
}

bool FebftSMR::exists_in_service(const std::string &service_name, const std::string &key) {
    return false;
}

MonitorDBStore::TransactionRef FebftSMR::get_pending_transaction() {
    return MonitorDBStore::TransactionRef();
}

bool FebftSMR::trigger_propose() {
    return false;
}

void FebftSMR::cancel_events() {

}

MonitorDBStore *FebftSMR::get_store() {
    return nullptr;
}

std::string FebftSMR::get_name() const {
    return std::string();
}
