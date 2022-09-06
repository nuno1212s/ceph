#include "FebftMonitor.h"
#include <iterator>
#include <sstream>
#include <tuple>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>
#include <cstring>
#include <boost/scope_exit.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "json_spirit/json_spirit_reader.h"
#include "json_spirit/json_spirit_writer.h"

#include "common/version.h"
#include "common/blkdev.h"
#include "common/cmdparse.h"
#include "common/signal.h"

#include "osd/OSDMap.h"

#include "mon/MonitorDBStore.h"

#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonSync.h"
#include "messages/MMonScrub.h"
#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MTimeCheck2.h"
#include "messages/MPing.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "global/signal_handler.h"
#include "common/Formatter.h"
#include "include/stringify.h"
#include "include/color.h"
#include "include/ceph_fs.h"
#include "include/str_list.h"

#include "mon/OSDMonitor.h"
#include "mon/MDSMonitor.h"
#include "mon/MonmapMonitor.h"
#include "mon/LogMonitor.h"
#include "mon/AuthMonitor.h"
#include "mon/MgrMonitor.h"
#include "mon/MgrStatMonitor.h"
#include "mon/ConfigMonitor.h"
#include "mon/KVMonitor.h"
#include "mon/HealthMonitor.h"
#include "common/config.h"
#include "common/cmdparse.h"
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "perfglue/heap_profiler.h"

#include "auth/none/AuthNoneClientHandler.h"

using std::ostream;
using std::ostringstream;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

static ostream &_prefix(std::ostream *_dout, FebftMonitor *mon) {
    return *_dout << "mon." << mon->name << "@" << mon->rank
                  << "(" << ").monmap v" << mon->monmap->epoch << " ";
}


#undef FLAG
#undef COMMAND
#undef COMMAND_WITH_FLAG
#define FLAG(f) (MonCommand::FLAG_##f)
#define COMMAND(parsesig, helptext, modulename, req_perms)    \
  {parsesig, helptext, modulename, req_perms, FLAG(NONE)},
#define COMMAND_WITH_FLAG(parsesig, helptext, modulename, req_perms, flags) \
  {parsesig, helptext, modulename, req_perms, flags},
MonCommand mon_commands[] = {
#include "mon/MonCommands.h"
};
#undef COMMAND
#undef COMMAND_WITH_FLAG

FebftMonitor::FebftMonitor(CephContext *cct_, std::string nm, MonitorDBStore *store, Messenger *m, Messenger *mgr_m,
                           MonMap *map)
        : AbstractMonitor(cct_, store, nm, m, mgr_m, map) {

    febft = std::make_unique<FebftSMR>(*this, "febft");

    services[PAXOS_MDSMAP].reset(new MDSMonitor(*this, *febft, "mdsmap"));
    services[PAXOS_MONMAP].reset(new MonmapMonitor(*this, *febft, "monmap"));
    services[PAXOS_OSDMAP].reset(new OSDMonitor(cct, *this, *febft, "osdmap"));
    services[PAXOS_LOG].reset(new LogMonitor(*this, *febft, "logm"));
    services[PAXOS_AUTH].reset(new AuthMonitor(*this, *febft, "auth"));
    services[PAXOS_MGR].reset(new MgrMonitor(*this, *febft, "mgr"));
    services[PAXOS_MGRSTAT].reset(new MgrStatMonitor(*this, *febft, "mgrstat"));
    services[PAXOS_HEALTH].reset(new HealthMonitor(*this, *febft, "health"));
    services[PAXOS_CONFIG].reset(new ConfigMonitor(*this, *febft, "config"));
    services[PAXOS_KV].reset(new KVMonitor(*this, *febft, "kv"));


    // prepare local commands
    local_mon_commands.resize(std::size(mon_commands));
    for (unsigned i = 0; i < std::size(mon_commands); ++i) {
        local_mon_commands[i] = mon_commands[i];
    }

    MonCommand::encode_vector(local_mon_commands, local_mon_commands_bl);

    prenautilus_local_mon_commands = local_mon_commands;
    for (auto &i: prenautilus_local_mon_commands) {
        std::string n = cmddesc_get_prenautilus_compat(i.cmdstring);
        if (n != i.cmdstring) {
            dout(20) << " pre-nautilus cmd " << i.cmdstring << " -> " << n << dendl;
            i.cmdstring = n;
        }
    }

    MonCommand::encode_vector(prenautilus_local_mon_commands, prenautilus_local_mon_commands_bl);

    // assume our commands until we have an election.  this only means
    // we won't reply with EINVAL before the election; any command that
    // actually matters will wait until we have quorum etc and then
    // retry (and revalidate).
    leader_mon_commands = local_mon_commands;
}

int FebftMonitor::preinit() {

    std::unique_lock l(lock);

    dout(1) << "preinit fsid " << monmap->fsid << dendl;

    int r = sanitize_options();
    if (r < 0) {
        derr << "option sanitization failed!" << dendl;
        return r;
    }

    // verify cluster_uuid
    {
        int r = check_fsid();
        if (r == -ENOENT)
            r = write_fsid();
        if (r < 0) {
            return r;
        }
    }

    // open compatset
    read_features();

    // have we ever joined a quorum?
    has_ever_joined = (store->get(MONITOR_NAME, "joined") != 0);
    dout(10) << "has_ever_joined = " << (int) has_ever_joined << dendl;

    if (!has_ever_joined) {
        // impose initial quorum restrictions?
        list<string> initial_members;
        get_str_list(g_conf()->mon_initial_members, initial_members);

        if (!initial_members.empty()) {
            dout(1) << " initial_members " << initial_members << ", filtering seed monmap" << dendl;

            monmap->set_initial_members(
                    g_ceph_context, initial_members, name, messenger->get_myaddrs(),
                    &extra_probe_peers);

            dout(10) << " monmap is " << *monmap << dendl;
            dout(10) << " extra probe peers " << extra_probe_peers << dendl;
        }
    } else if (!monmap->contains(name)) {
        derr << "not in monmap and have been in a quorum before; "
             << "must have been removed" << dendl;
        if (g_conf()->mon_force_quorum_join) {
            dout(0) << "we should have died but "
                    << "'mon_force_quorum_join' is set -- allowing boot" << dendl;
        } else {
            derr << "commit suicide!" << dendl;
            return -ENOENT;
        }
    }

    if (is_keyring_required()) {
        // we need to bootstrap authentication keys so we can form an
        // initial quorum.
        if (authmon()->get_last_committed() == 0) {
            dout(10) << "loading initial keyring to bootstrap authentication for mkfs" << dendl;
            bufferlist bl;
            int err = store->get("mkfs", "keyring", bl);
            if (err == 0 && bl.length() > 0) {
                // Attempt to decode and extract keyring only if it is found.
                KeyRing keyring;
                auto p = bl.cbegin();
                decode(keyring, p);
                extract_save_mon_key(keyring);
            }
        }

        string keyring_loc = g_conf()->mon_data + "/keyring";

        r = keyring.load(cct, keyring_loc);
        if (r < 0) {
            EntityName mon_name;
            mon_name.set_type(CEPH_ENTITY_TYPE_MON);
            EntityAuth mon_key;
            if (key_server.get_auth(mon_name, mon_key)) {
                dout(1) << "copying mon. key from old db to external keyring" << dendl;
                keyring.add(mon_name, mon_key);
                bufferlist bl;
                keyring.encode_plaintext(bl);
                write_default_keyring(bl);
            } else {
                derr << "unable to load initial keyring " << g_conf()->keyring << dendl;
                return r;
            }
        }
    }

    admin_hook = new AdminHook(this);
    AdminSocket *admin_socket = cct->get_admin_socket();

    // unlock while registering to avoid mon_lock -> admin socket lock dependency.
    l.unlock();
    // register tell/asock commands
    for (const auto &command: local_mon_commands) {
        if (!command.is_tell()) {
            continue;
        }
        const auto prefix = ceph::common::cmddesc_get_prefix(command.cmdstring);
        if (prefix == "injectargs" ||
            prefix == "version" ||
            prefix == "tell") {
            // not registerd by me
            continue;
        }
        r = admin_socket->register_command(command.cmdstring, admin_hook,
                                           command.helpstring);
        ceph_assert(r == 0);
    }
    l.lock();

    // add ourselves as a conf observer
    g_conf().add_observer(this);

    messenger->set_auth_client(this);
    messenger->set_auth_server(this);
    mgr_messenger->set_auth_client(this);

    auth_registry.refresh_config();

}

int FebftMonitor::init() {

    dout(2) << "init" << dendl;
    std::lock_guard l(lock);

    finisher.start();

    // start ticker
    timer.init();
    new_tick();

    cpu_tp.start();


    // i'm ready!
    messenger->add_dispatcher_tail(this);

    // kickstart pet mgrclient
    mgr_client.init();
    mgr_messenger->add_dispatcher_tail(&mgr_client);
    mgr_messenger->add_dispatcher_tail(this);  // for auth ms_* calls
    mgrmon()->prime_mgr_client();

    // generate list of filestore OSDs
    osdmon()->get_filestore_osd_list();

    // add features of myself into feature_map
    session_map.feature_map.add_mon(con_self->get_features());

    return 0;
}

void FebftMonitor::get_mon_status(ceph::Formatter *f) {

    f->open_object_section("mon_status");
    f->dump_string("name", name);
    f->dump_int("rank", rank);
    f->dump_string("state", get_state_name());
    f->dump_int("election_epoch", get_epoch());

    f->open_array_section("quorum");
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p) {
        f->dump_int("mon", *p);
    }
    f->close_section(); // quorum

    if (!quorum.empty()) {
        f->dump_int(
                "quorum_age",
                quorum_age());
    }

    f->open_object_section("features");
    f->dump_stream("required_con") << required_features;
    mon_feature_t req_mon_features = get_required_mon_features();
    req_mon_features.dump(f, "required_mon");
    f->dump_stream("quorum_con") << quorum_con_features;
    quorum_mon_features.dump(f, "quorum_mon");
    f->close_section(); // features

    f->open_array_section("outside_quorum");
    for (set<string>::iterator p = outside_quorum.begin(); p != outside_quorum.end(); ++p)
        f->dump_string("mon", *p);
    f->close_section(); // outside_quorum

    f->open_array_section("extra_probe_peers");
    for (set<entity_addrvec_t>::iterator p = extra_probe_peers.begin();
         p != extra_probe_peers.end();
         ++p) {
        f->dump_object("peer", *p);
    }

    f->close_section(); // extra_probe_peers

    if (g_conf()->mon_sync_provider_kill_at > 0)
        f->dump_int("provider_kill_at", g_conf()->mon_sync_provider_kill_at);
    if (g_conf()->mon_sync_requester_kill_at > 0)
        f->dump_int("requester_kill_at", g_conf()->mon_sync_requester_kill_at);

    f->open_object_section("monmap");
    monmap->dump(f);
    f->close_section();

    f->dump_object("feature_map", session_map.feature_map);
    f->close_section(); // mon_status
}

int FebftMonitor::do_admin_command(std::string_view command, const cmdmap_t &cmdmap, ceph::Formatter *f,
                                   std::ostream &err, std::ostream &out) {
    std::lock_guard l(lock);

    int r = 0;
    string args;
    for (auto p = cmdmap.begin();
         p != cmdmap.end(); ++p) {
        if (p->first == "prefix")
            continue;
        if (!args.empty())
            args += ", ";
        args += cmd_vartype_stringify(p->second);
    }
    args = "[" + args + "]";

    bool read_only = (command == "mon_status" ||
                      command == "mon metadata" ||
                      command == "quorum_status" ||
                      command == "ops" ||
                      command == "sessions");

    (read_only ? audit_clog->debug() : audit_clog->info())
            << "from='admin socket' entity='admin socket' "
            << "cmd='" << command << "' args=" << args << ": dispatch";

    if (command == "mon_status") {
        get_mon_status(f);
    } else if (command == "quorum_status") {
        _quorum_status(f, out);
    } else if (command == "sync_force") {
        bool validate = false;
        if (!cmd_getval(cmdmap, "yes_i_really_mean_it", validate)) {
            std::string v;
            if (cmd_getval(cmdmap, "validate", v) &&
                v == "--yes-i-really-mean-it") {
                validate = true;
            }
        }
        if (!validate) {
            err << "are you SURE? this will mean the monitor store will be erased "
                   "the next time the monitor is restarted.  pass "
                   "'--yes-i-really-mean-it' if you really do.";
            r = -EPERM;
            goto abort;
        }
        sync_force(f);
    } else if (command.compare(0, 23, "add_bootstrap_peer_hint") == 0 ||
               command.compare(0, 24, "add_bootstrap_peer_hintv") == 0) {
        if (!_add_bootstrap_peer_hint(command, cmdmap, out))
            goto abort;
    } else if (command == "quorum enter" || command == "quorum exit") {
        err << "This command is not available in BFT mode!";
    } else if (command == "ops") {
        (void) op_tracker.dump_ops_in_flight(f);
    } else if (command == "sessions") {
        f->open_array_section("sessions");
        for (auto p: session_map.sessions) {
            f->dump_object("session", *p);
        }
        f->close_section();
    } else if (command == "dump_historic_ops") {
        if (!op_tracker.dump_historic_ops(f)) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "dump_historic_ops_by_duration") {
        if (op_tracker.dump_historic_ops(f, true)) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "dump_historic_slow_ops") {
        if (op_tracker.dump_historic_slow_ops(f, {})) {
            err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
        }
    } else if (command == "quorum") {
        err << "This command is not available in BFT mode!";
    } else if (command == "connection scores dump" || command == "connection scores reset") {
        err << "This command is not available in BFT mode!";
    } else if (command == "smart") {
        string want_devid;
        cmd_getval(cmdmap, "devid", want_devid);

        string devname = store->get_devname();
        if (devname.empty()) {
            err << "could not determine device name for " << store->get_path();
            r = -ENOENT;
            goto abort;
        }
        set<string> devnames;
        get_raw_devices(devname, &devnames);
        json_spirit::mObject json_map;
        uint64_t smart_timeout = cct->_conf.get_val<uint64_t>(
                "mon_smart_report_timeout");
        for (auto &devname: devnames) {
            string err;
            string devid = get_device_id(devname, &err);
            if (want_devid.size() && want_devid != devid) {
                derr << "get_device_id failed on " << devname << ": " << err << dendl;
                continue;
            }
            json_spirit::mValue smart_json;
            if (block_device_get_metrics(devname, smart_timeout,
                                         &smart_json)) {
                dout(10) << "block_device_get_metrics failed for /dev/" << devname
                         << dendl;
                continue;
            }
            json_map[devid] = smart_json;
        }
        json_spirit::write(json_map, out, json_spirit::pretty_print);
    } else if (command == "heap") {
        if (!ceph_using_tcmalloc()) {
            err << "could not issue heap profiler command -- not using tcmalloc!";
            r = -EOPNOTSUPP;
            goto abort;
        }
        string cmd;
        if (!cmd_getval(cmdmap, "heapcmd", cmd)) {
            err << "unable to get value for command \"" << cmd << "\"";
            r = -EINVAL;
            goto abort;
        }
        std::vector<std::string> cmd_vec;
        get_str_vec(cmd, cmd_vec);
        string val;
        if (cmd_getval(cmdmap, "value", val)) {
            cmd_vec.push_back(val);
        }
        ceph_heap_profiler_handle_command(cmd_vec, out);
    } else if (command == "compact") {
        dout(1) << "triggering manual compaction" << dendl;
        auto start = ceph::coarse_mono_clock::now();
        store->compact_async();
        auto end = ceph::coarse_mono_clock::now();
        auto duration = ceph::to_seconds<double>(end - start);
        dout(1) << "finished manual compaction in "
                << duration << " seconds" << dendl;
        out << "compacted " << g_conf().get_val<std::string>("mon_keyvaluedb")
            << " in " << duration << " seconds";
    } else {
        ceph_abort_msg("bad AdminSocket command binding");
    }
    (read_only ? audit_clog->debug() : audit_clog->info())
            << "from='admin socket' "
            << "entity='admin socket' "
            << "cmd=" << command << " "
            << "args=" << args << ": finished";
    return r;

    abort:
    (read_only ? audit_clog->debug() : audit_clog->info())
            << "from='admin socket' "
            << "entity='admin socket' "
            << "cmd=" << command << " "
            << "args=" << args << ": aborted";
    return r;
}

void FebftMonitor::handle_get_version(MonOpRequestRef op) {

    auto m = op->get_req<MMonGetVersion>();
    dout(10) << "handle_get_version " << *m << dendl;
    Service *svc = NULL;

    MonSession *s = op->get_session();
    ceph_assert(s);

    if (!is_leader() && !is_peon()) {
        dout(10) << " waiting for quorum" << dendl;
        //waitfor_quorum.push_back(new C_RetryMessage(this, op));
        //This shouldn't be possible with febft
        goto out;
    }

    if (m->what == "mdsmap") {
        svc = mdsmon();
    } else if (m->what == "fsmap") {
        svc = mdsmon();
    } else if (m->what == "osdmap") {
        svc = osdmon();
    } else if (m->what == "monmap") {
        svc = monmon();
    } else {
        derr << "invalid map type " << m->what << dendl;
    }

    if (svc) {
        if (!svc->is_readable()) {
            svc->wait_for_readable(op, new C_RetryMessage(this, op));
            goto out;
        }

        MMonGetVersionReply *reply = new MMonGetVersionReply();
        reply->handle = m->handle;
        reply->version = svc->get_last_committed();
        reply->oldest_version = svc->get_first_committed();
        reply->set_tid(m->get_tid());

        m->get_connection()->send_message(reply);
    }

    out:
    return;
}

void FebftMonitor::_quorum_status(ceph::Formatter *f, ostream &ss) {

    bool free_formatter = false;

    if (!f) {
        // louzy/lazy hack: default to json if no formatter has been defined
        f = new JSONFormatter();
        free_formatter = true;
    }
    f->open_object_section("quorum_status");
    f->dump_int("election_epoch", get_epoch());

    f->open_array_section("quorum");
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
        f->dump_int("mon", *p);
    f->close_section(); // quorum

    list<string> quorum_names = get_quorum_names();
    f->open_array_section("quorum_names");
    for (list<string>::iterator p = quorum_names.begin(); p != quorum_names.end(); ++p)
        f->dump_string("mon", *p);
    f->close_section(); // quorum_names

    f->dump_string("quorum_leader_name", quorum.empty() ? string() : monmap->get_name(get_leader()));

    if (!quorum.empty()) {
        f->dump_int(
                "quorum_age",
                quorum_age());
    }

    f->open_object_section("features");
    f->dump_stream("quorum_con") << quorum_con_features;
    quorum_mon_features.dump(f, "quorum_mon");
    f->close_section();

    f->open_object_section("monmap");
    monmap->dump(f);
    f->close_section(); // monmap

    f->close_section(); // quorum_status
    f->flush(ss);
    if (free_formatter)
        delete f;
}

void FebftMonitor::handle_command(MonOpRequestRef op) {
    ceph_assert(op->is_type_command());
    auto m = op->get_req<MMonCommand>();
    if (m->fsid != monmap->fsid) {
        dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid
                << dendl;
        reply_command(op, -EPERM, "wrong fsid", 0);
        return;
    }

    MonSession *session = op->get_session();
    if (!session) {
        dout(5) << __func__ << " dropping stray message " << *m << dendl;
        return;
    }

    if (m->cmd.empty()) {
        reply_command(op, -EINVAL, "no command specified", 0);
        return;
    }

    string prefix;
    vector<string> fullcmd;
    cmdmap_t cmdmap;
    stringstream ss, ds;
    bufferlist rdata;
    string rs;
    int r = -EINVAL;
    rs = "unrecognized command";

    if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
        // ss has reason for failure
        r = -EINVAL;
        rs = ss.str();
        if (!m->get_source().is_mon())  // don't reply to mon->mon commands
            reply_command(op, r, rs, 0);
        return;
    }

    // check return value. If no prefix parameter provided,
    // return value will be false, then return error info.
    if (!cmd_getval(cmdmap, "prefix", prefix)) {
        reply_command(op, -EINVAL, "command prefix not found", 0);
        return;
    }

    // check prefix is empty
    if (prefix.empty()) {
        reply_command(op, -EINVAL, "command prefix must not be empty", 0);
        return;
    }

    if (prefix == "get_command_descriptions") {
        bufferlist rdata;
        Formatter *f = Formatter::create("json");

        std::vector<MonCommand> commands = static_cast<MgrMonitor *>(
                services[PAXOS_MGR].get())->get_command_descs();

        for (auto &c: leader_mon_commands) {
            commands.push_back(c);
        }

        auto features = m->get_connection()->get_features();
        format_command_descriptions(commands, f, features, &rdata);
        delete f;
        reply_command(op, 0, "", rdata, 0);
        return;
    }

    dout(0) << "handle_command " << *m << dendl;

    string format = ceph::common::cmd_getval_or<string>(cmdmap, "format", "plain");
    boost::scoped_ptr<Formatter> f(Formatter::create(format));

    get_str_vec(prefix, fullcmd);

    // make sure fullcmd is not empty.
    // invalid prefix will cause empty vector fullcmd.
    // such as, prefix=";,,;"
    if (fullcmd.empty()) {
        reply_command(op, -EINVAL, "command requires a prefix to be valid", 0);
        return;
    }

    std::string_view module = fullcmd[0];

    // validate command is in leader map

    const MonCommand *leader_cmd;
    const auto &mgr_cmds = mgrmon()->get_command_descs();
    const MonCommand *mgr_cmd = nullptr;
    if (!mgr_cmds.empty()) {
        mgr_cmd = _get_moncommand(prefix, mgr_cmds);
    }
    leader_cmd = _get_moncommand(prefix, leader_mon_commands);
    if (!leader_cmd) {
        leader_cmd = mgr_cmd;
        if (!leader_cmd) {
            reply_command(op, -EINVAL, "command not known", 0);
            return;
        }
    }
    // validate command is in our map & matches, or forward if it is allowed
    const MonCommand *mon_cmd = _get_moncommand(
            prefix,
            get_local_commands(quorum_mon_features));
    if (!mon_cmd) {
        mon_cmd = mgr_cmd;
    }
    if (!is_leader()) {
        if (!mon_cmd) {
            if (leader_cmd->is_noforward()) {
                reply_command(op, -EINVAL,
                              "command not locally supported and not allowed to forward",
                              0);
                return;
            }

            dout(10) << "Command not locally supported, forwarding request "
                     << m << dendl;
            forward_request_leader(op);
            return;
        } else if (!mon_cmd->is_compat(leader_cmd)) {
            if (mon_cmd->is_noforward()) {
                reply_command(op, -EINVAL,
                              "command not compatible with leader and not allowed to forward",
                              0);
                return;
            }
            dout(10) << "Command not compatible with leader, forwarding request "
                     << m << dendl;
            forward_request_leader(op);
            return;
        }
    }

    if (mon_cmd->is_obsolete() ||
        (cct->_conf->mon_debug_deprecated_as_obsolete
         && mon_cmd->is_deprecated())) {
        reply_command(op, -ENOTSUP,
                      "command is obsolete; please check usage and/or man page",
                      0);
        return;
    }

    if (session->proxy_con && mon_cmd->is_noforward()) {
        dout(10) << "Got forward for noforward command " << m << dendl;
        reply_command(op, -EINVAL, "forward for noforward command", rdata, 0);
        return;
    }

    /* what we perceive as being the service the command falls under */
    string service(mon_cmd->module);

    dout(25) << __func__ << " prefix='" << prefix
             << "' module='" << module
             << "' service='" << service << "'" << dendl;

    bool cmd_is_rw =
            (mon_cmd->requires_perm('w') || mon_cmd->requires_perm('x'));

    // validate user's permissions for requested command
    map<string, string> param_str_map;

    // Catch bad_cmd_get exception if _generate_command_map() throws it
    try {
        _generate_command_map(cmdmap, param_str_map);
    }
    catch (ceph::common::bad_cmd_get &e) {
        reply_command(op, -EINVAL, e.what(), 0);
    }

    if (!_allowed_command(session, service, prefix, cmdmap,
                          param_str_map, mon_cmd)) {
        dout(1) << __func__ << " access denied" << dendl;
        if (prefix != "config set" && prefix != "config-key set")
            (cmd_is_rw ? audit_clog->info() : audit_clog->debug())
                    << "from='" << session->name << " " << session->addrs << "' "
                    << "entity='" << session->entity_name << "' "
                    << "cmd=" << m->cmd << ":  access denied";
        reply_command(op, -EACCES, "access denied", 0);
        return;
    }

    if (prefix != "config set" && prefix != "config-key set")
        (cmd_is_rw ? audit_clog->info() : audit_clog->debug())
                << "from='" << session->name << " " << session->addrs << "' "
                << "entity='" << session->entity_name << "' "
                << "cmd=" << m->cmd << ": dispatch";

    // compat kludge for legacy clients trying to tell commands that are
    // new.  see bottom of MonCommands.h.  we need to handle both (1)
    // pre-octopus clients and (2) octopus clients with a mix of pre-octopus
    // and octopus mons.
    if ((!HAVE_FEATURE(m->get_connection()->get_features(), SERVER_OCTOPUS) ||
         monmap->min_mon_release < ceph_release_t::octopus) &&
        (prefix == "injectargs" ||
         prefix == "smart" ||
         prefix == "mon_status" ||
         prefix == "heap")) {
        if (m->get_connection()->get_messenger() == 0) {
            // Prior to octopus, monitors might forward these messages
            // around. that was broken at baseline, and if we try to process
            // this message now, it will assert out when we try to send a
            // message in reply from the asok/tell worker (see
            // AnonConnection).  Just reply with an error.
            dout(5) << __func__ << " failing forwarded command from a (presumably) "
                    << "pre-octopus peer" << dendl;
            reply_command(
                    op, -EBUSY,
                    "failing forwarded tell command in mixed-version mon cluster", 0);
            return;
        }
        dout(5) << __func__ << " passing command to tell/asok" << dendl;
        cct->get_admin_socket()->queue_tell_command(m);
        return;
    }

    if (mon_cmd->is_mgr()) {
        const auto &hdr = m->get_header();
        uint64_t size = hdr.front_len + hdr.middle_len + hdr.data_len;
        uint64_t max = g_conf().get_val<Option::size_t>("mon_client_bytes")
                       * g_conf().get_val<double>("mon_mgr_proxy_client_bytes_ratio");
        if (mgr_proxy_bytes + size > max) {
            dout(10) << __func__ << " current mgr proxy bytes " << mgr_proxy_bytes
                     << " + " << size << " > max " << max << dendl;
            reply_command(op, -EAGAIN, "hit limit on proxied mgr commands", rdata, 0);
            return;
        }
        mgr_proxy_bytes += size;
        dout(10) << __func__ << " proxying mgr command (+" << size
                 << " -> " << mgr_proxy_bytes << ")" << dendl;
        C_MgrProxyCommand *fin = new C_MgrProxyCommand((AbstractMonitor *) this, op, size);
        mgr_client.start_command(m->cmd,
                                 m->get_data(),
                                 &fin->outbl,
                                 &fin->outs,
                                 new C_OnFinisher(fin, &finisher));
        return;
    }

    if ((module == "mds" || module == "fs") &&
        prefix != "fs authorize") {
        mdsmon()->dispatch(op);
        return;
    }
    if ((module == "osd" ||
         prefix == "pg map" ||
         prefix == "pg repeer") &&
        prefix != "osd last-stat-seq") {
        osdmon()->dispatch(op);
        return;
    }
    if (module == "config") {
        configmon()->dispatch(op);
        return;
    }

    if (module == "mon" &&
        /* Let the Monitor class handle the following commands:
         *  'mon scrub'
         */
        prefix != "mon scrub" &&
        prefix != "mon metadata" &&
        prefix != "mon versions" &&
        prefix != "mon count-metadata" &&
        prefix != "mon ok-to-stop" &&
        prefix != "mon ok-to-add-offline" &&
        prefix != "mon ok-to-rm") {
        monmon()->dispatch(op);
        return;
    }
    if (module == "health" && prefix != "health") {
        healthmon()->dispatch(op);
        return;
    }
    if (module == "auth" || prefix == "fs authorize") {
        authmon()->dispatch(op);
        return;
    }
    if (module == "log") {
        logmon()->dispatch(op);
        return;
    }

    if (module == "config-key") {
        kvmon()->dispatch(op);
        return;
    }

    if (module == "mgr") {
        mgrmon()->dispatch(op);
        return;
    }

    if (prefix == "fsid") {
        if (f) {
            f->open_object_section("fsid");
            f->dump_stream("fsid") << monmap->fsid;
            f->close_section();
            f->flush(rdata);
        } else {
            ds << monmap->fsid;
            rdata.append(ds);
        }
        reply_command(op, 0, "", rdata, 0);
        return;
    }

    if (prefix == "mon scrub") {
        //Febft is responsible for maintaining consistent states across replicas
        return;
    }

    if (prefix == "time-sync-status") {
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        f->open_object_section("time_sync");
        if (!timecheck_skews.empty()) {
            f->open_object_section("time_skew_status");
            for (auto &i: timecheck_skews) {
                double skew = i.second;
                double latency = timecheck_latencies[i.first];
                string name = monmap->get_name(i.first);
                ostringstream tcss;
                health_status_t tcstatus = timecheck_status(tcss, skew, latency);
                f->open_object_section(name.c_str());
                f->dump_float("skew", skew);
                f->dump_float("latency", latency);
                f->dump_stream("health") << tcstatus;
                if (tcstatus != HEALTH_OK) {
                    f->dump_stream("details") << tcss.str();
                }
                f->close_section();
            }
            f->close_section();
        }
        f->open_object_section("timechecks");
        f->dump_unsigned("epoch", get_epoch());
        f->dump_int("round", timecheck_round);
        f->dump_stream("round_status") << ((timecheck_round % 2) ?
                                           "on-going" : "finished");
        f->close_section();
        f->close_section();
        f->flush(rdata);
        r = 0;
        rs = "";
    } else if (prefix == "status" ||
               prefix == "health" ||
               prefix == "df") {
        string detail;
        cmd_getval(cmdmap, "detail", detail);

        if (prefix == "status") {
            // get_cluster_status handles f == NULL
            get_cluster_status(ds, f.get(), session);

            if (f) {
                f->flush(ds);
                ds << '\n';
            }
            rdata.append(ds);
        } else if (prefix == "health") {
            string plain;
            healthmon()->get_health_status(detail == "detail", f.get(), f ? nullptr : &plain);
            if (f) {
                f->flush(ds);
                rdata.append(ds);
            } else {
                rdata.append(plain);
            }
        } else if (prefix == "df") {
            bool verbose = (detail == "detail");
            if (f)
                f->open_object_section("stats");

            mgrstatmon()->dump_cluster_stats(&ds, f.get(), verbose);
            if (!f) {
                ds << "\n \n";
            }
            mgrstatmon()->dump_pool_stats(osdmon()->osdmap, &ds, f.get(), verbose);

            if (f) {
                f->close_section();
                f->flush(ds);
                ds << '\n';
            }
        } else {
            ceph_abort_msg("We should never get here!");
            return;
        }
        rdata.append(ds);
        rs = "";
        r = 0;
    } else if (prefix == "report") {
        // some of the report data is only known by leader, e.g. osdmap_clean_epochs

        /*
        if (!is_leader() && !is_peon()) {
            dout(10) << " waiting for quorum" << dendl;
            waitfor_quorum.push_back(new C_RetryMessage(this, op));
            return;
        }
        if (!is_leader()) {
            forward_request_leader(op);
            return;
        }
         */
        // this must be formatted, in its current form
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        f->open_object_section("report");
        f->dump_stream("cluster_fingerprint") << fingerprint;
        f->dump_string("version", ceph_version_to_str());
        f->dump_string("commit", git_version_to_str());
        f->dump_stream("timestamp") << ceph_clock_now();

        vector<string> tagsvec;
        cmd_getval(cmdmap, "tags", tagsvec);
        string tagstr = str_join(tagsvec, " ");
        if (!tagstr.empty())
            tagstr = tagstr.substr(0, tagstr.find_last_of(' '));
        f->dump_string("tag", tagstr);

        healthmon()->get_health_status(true, f.get(), nullptr);

        monmon()->dump_info(f.get());
        osdmon()->dump_info(f.get());
        mdsmon()->dump_info(f.get());
        authmon()->dump_info(f.get());
        mgrstatmon()->dump_info(f.get());
        logmon()->dump_info(f.get());

        febft->dump_info(f.get());

        f->close_section();
        f->flush(rdata);

        ostringstream ss2;
        ss2 << "report " << rdata.crc32c(CEPH_MON_PORT_LEGACY);
        rs = ss2.str();
        r = 0;
    } else if (prefix == "osd last-stat-seq") {
        int64_t osd = 0;
        cmd_getval(cmdmap, "id", osd);
        uint64_t seq = mgrstatmon()->get_last_osd_stat_seq(osd);
        if (f) {
            f->dump_unsigned("seq", seq);
            f->flush(ds);
        } else {
            ds << seq;
            rdata.append(ds);
        }
        rs = "";
        r = 0;
    } else if (prefix == "node ls") {
        string node_type("all");
        cmd_getval(cmdmap, "type", node_type);
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        if (node_type == "all") {
            f->open_object_section("nodes");
            print_nodes(f.get(), ds);
            osdmon()->print_nodes(f.get());
            mdsmon()->print_nodes(f.get());
            mgrmon()->print_nodes(f.get());
            f->close_section();
        } else if (node_type == "mon") {
            print_nodes(f.get(), ds);
        } else if (node_type == "osd") {
            osdmon()->print_nodes(f.get());
        } else if (node_type == "mds") {
            mdsmon()->print_nodes(f.get());
        } else if (node_type == "mgr") {
            mgrmon()->print_nodes(f.get());
        }
        f->flush(ds);
        rdata.append(ds);
        rs = "";
        r = 0;
    } else if (prefix == "features") {
        /*if (!is_leader() && !is_peon()) {
            dout(10) << " waiting for quorum" << dendl;
            waitfor_quorum.push_back(new C_RetryMessage(this, op));
            return;
        }
        if (!is_leader()) {
            forward_request_leader(op);
            return;
        }*/
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        FeatureMap fm;
        get_combined_feature_map(&fm);
        f->dump_object("features", fm);
        f->flush(rdata);
        rs = "";
        r = 0;
    } else if (prefix == "mon metadata") {
        if (!f)
            f.reset(Formatter::create("json-pretty"));

        string name;
        bool all = !cmd_getval(cmdmap, "id", name);
        if (!all) {
            // Dump a single mon's metadata
            int mon = monmap->get_rank(name);
            if (mon < 0) {
                rs = "requested mon not found";
                r = -ENOENT;
                goto out;
            }
            f->open_object_section("mon_metadata");
            r = get_mon_metadata(mon, f.get(), ds);
            f->close_section();
        } else {
            // Dump all mons' metadata
            r = 0;
            f->open_array_section("mon_metadata");
            for (unsigned int rank = 0; rank < monmap->size(); ++rank) {
                std::ostringstream get_err;
                f->open_object_section("mon");
                f->dump_string("name", monmap->get_name(rank));
                r = get_mon_metadata(rank, f.get(), get_err);
                f->close_section();
                if (r == -ENOENT || r == -EINVAL) {
                    dout(1) << get_err.str() << dendl;
                    // Drop error, list what metadata we do have
                    r = 0;
                } else if (r != 0) {
                    derr << "Unexpected error from get_mon_metadata: "
                         << cpp_strerror(r) << dendl;
                    ds << get_err.str();
                    break;
                }
            }
            f->close_section();
        }

        f->flush(ds);
        rdata.append(ds);
        rs = "";
    } else if (prefix == "mon versions") {
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        count_metadata("ceph_version", f.get());
        f->flush(ds);
        rdata.append(ds);
        rs = "";
        r = 0;
    } else if (prefix == "mon count-metadata") {
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        string field;
        cmd_getval(cmdmap, "property", field);
        count_metadata(field, f.get());
        f->flush(ds);
        rdata.append(ds);
        rs = "";
        r = 0;
    } else if (prefix == "quorum_status") {
        // make sure our map is readable and up to date
        if (!is_leader() && !is_peon()) {
            dout(10) << " waiting for quorum" << dendl;
            waitfor_quorum.push_back(new C_RetryMessage(this, op));
            return;
        }
        _quorum_status(f.get(), ds);
        rdata.append(ds);
        rs = "";
        r = 0;
    } else if (prefix == "mon ok-to-stop") {
        vector<string> ids, invalid_ids;
        if (!cmd_getval(cmdmap, "ids", ids)) {
            r = -EINVAL;
            goto out;
        }
        set<string> wouldbe;
        for (auto rank: quorum) {
            wouldbe.insert(monmap->get_name(rank));
        }
        for (auto &n: ids) {
            if (monmap->contains(n)) {
                wouldbe.erase(n);
            } else {
                invalid_ids.push_back(n);
            }
        }
        if (!invalid_ids.empty()) {
            r = 0;
            rs = "invalid mon(s) specified: " + stringify(invalid_ids);
            goto out;
        }

        if (wouldbe.size() < monmap->min_quorum_size()) {
            r = -EBUSY;
            rs = "not enough monitors would be available (" + stringify(wouldbe) +
                 ") after stopping mons " + stringify(ids);
            goto out;
        }
        r = 0;
        rs = "quorum should be preserved (" + stringify(wouldbe) +
             ") after stopping " + stringify(ids);
    } else if (prefix == "mon ok-to-add-offline") {
        if (quorum.size() < monmap->min_quorum_size(monmap->size() + 1)) {
            rs = "adding a monitor may break quorum (until that monitor starts)";
            r = -EBUSY;
            goto out;
        }
        rs = "adding another mon that is not yet online will not break quorum";
        r = 0;
    } else if (prefix == "mon ok-to-rm") {
        string id;
        if (!cmd_getval(cmdmap, "id", id)) {
            r = -EINVAL;
            rs = "must specify a monitor id";
            goto out;
        }
        if (!monmap->contains(id)) {
            r = 0;
            rs = "mon." + id + " does not exist";
            goto out;
        }
        int rank = monmap->get_rank(id);
        if (quorum.count(rank) &&
            quorum.size() - 1 < monmap->min_quorum_size(monmap->size() - 1)) {
            r = -EBUSY;
            rs = "removing mon." + id + " would break quorum";
            goto out;
        }
        r = 0;
        rs = "safe to remove mon." + id;
    } else if (prefix == "version") {
        if (f) {
            f->open_object_section("version");
            f->dump_string("version", pretty_version_to_str());
            f->close_section();
            f->flush(ds);
        } else {
            ds << pretty_version_to_str();
        }
        rdata.append(ds);
        rs = "";
        r = 0;
    } else if (prefix == "versions") {
        if (!f)
            f.reset(Formatter::create("json-pretty"));
        map<string, int> overall;
        f->open_object_section("version");
        map<string, int> mon, mgr, osd, mds;

        count_metadata("ceph_version", &mon);
        f->open_object_section("mon");
        for (auto &p: mon) {
            f->dump_int(p.first.c_str(), p.second);
            overall[p.first] += p.second;
        }
        f->close_section();

        mgrmon()->count_metadata("ceph_version", &mgr);
        f->open_object_section("mgr");
        for (auto &p: mgr) {
            f->dump_int(p.first.c_str(), p.second);
            overall[p.first] += p.second;
        }
        f->close_section();

        osdmon()->count_metadata("ceph_version", &osd);
        f->open_object_section("osd");
        for (auto &p: osd) {
            f->dump_int(p.first.c_str(), p.second);
            overall[p.first] += p.second;
        }
        f->close_section();

        mdsmon()->count_metadata("ceph_version", &mds);
        f->open_object_section("mds");
        for (auto &p: mds) {
            f->dump_int(p.first.c_str(), p.second);
            overall[p.first] += p.second;
        }
        f->close_section();

        for (auto &p: mgrstatmon()->get_service_map().services) {
            auto &service = p.first;
            if (ServiceMap::is_normal_ceph_entity(service)) {
                continue;
            }
            f->open_object_section(service.c_str());
            map<string, int> m;
            p.second.count_metadata("ceph_version", &m);
            for (auto &q: m) {
                f->dump_int(q.first.c_str(), q.second);
                overall[q.first] += q.second;
            }
            f->close_section();
        }

        f->open_object_section("overall");
        for (auto &p: overall) {
            f->dump_int(p.first.c_str(), p.second);
        }
        f->close_section();
        f->close_section();
        f->flush(rdata);
        rs = "";
        r = 0;
    }

    out:
    if (!m->get_source().is_mon())  // don't reply to mon->mon commands
        reply_command(op, r, rs, rdata, 0);
}

void FebftMonitor::notify_new_monmap(bool can_change_external_state) {
    //TODO: Unsure of what to do in case of updated monmap
}


void FebftMonitor::_dispatch_op(MonOpRequestRef op) {
    /* messages that should only be sent by another monitor */
    switch (op->get_req()->get_type()) {

        case MSG_ROUTE:
            handle_route(op);
            return;

        case MSG_MON_PROBE:
            //TODO: Handle this?
            //handle_probe(op);
            return;

            // Sync (i.e., the new slurp, but on steroids)
        case MSG_MON_SYNC:
            //Synchronization operations are handled by febft
            return;
        case MSG_MON_SCRUB:
            //Not implemented on febft
            return;

            /* log acks are sent from a monitor we sent the MLog to, and are
               never sent by clients to us. */
        case MSG_LOGACK:
            log_client.handle_log_ack((MLogAck *) op->get_req());
            return;

            // monmap
        case MSG_MON_JOIN:
            op->set_type_service();
            services[PAXOS_MONMAP]->dispatch(op);
            return;

            // paxos
        case MSG_MON_PAXOS:
            //We don't want to handle paxos messages since all of that will
            //be handled by febft
            return;

            // elector messages
        case MSG_MON_ELECTION:
        case MSG_MON_PING:
            //Elections are not handled here in ceph
            return;

        case MSG_FORWARD:
            //Messages don't have to be forwarded here, they will be
            //given to the febft client which will then send the message to all replicas
            return;

        case MSG_TIMECHECK:
            dout(5) << __func__ << " ignoring " << op << dendl;
            return;
        case MSG_TIMECHECK2:
            handle_timecheck(op);
            return;

        case MSG_MON_HEALTH:
            dout(5) << __func__ << " dropping deprecated message: "
                    << *op->get_req() << dendl;
            break;
        case MSG_MON_HEALTH_CHECKS:
            op->set_type_service();
            services[PAXOS_HEALTH]->dispatch(op);
            return;
    }

    dout(1) << "dropping unexpected " << *(op->get_req()) << dendl;
}

void FebftMonitor::refresh_from_smr(bool *need_bootstrap) {
    dout(10) << __func__ << dendl;

    bufferlist bl;
    int r = store->get(MONITOR_NAME, "cluster_fingerprint", bl);
    if (r >= 0) {
        try {
            auto p = bl.cbegin();
            decode(fingerprint, p);
        }
        catch (ceph::buffer::error &e) {
            dout(10) << __func__ << " failed to decode cluster_fingerprint" << dendl;
        }
    } else {
        dout(10) << __func__ << " no cluster_fingerprint" << dendl;
    }

    for (auto &svc: services) {
        svc->refresh(need_bootstrap);
    }
    for (auto &svc: services) {
        svc->post_refresh();
    }

    load_metadata();
}

void FebftMonitor::tick() {
    // ok go.
    dout(11) << "tick" << dendl;
    const utime_t now = ceph_clock_now();

    // Check if we need to emit any delayed health check updated messages
    if (is_leader()) {
        const auto min_period = g_conf().get_val<int64_t>(
                "mon_health_log_update_period");
        for (auto &svc: services) {
            auto health = svc->get_health_checks();

            for (const auto &i: health.checks) {
                const std::string &code = i.first;
                const std::string &summary = i.second.summary;
                const health_status_t severity = i.second.severity;

                auto status_iter = health_check_log_times.find(code);
                if (status_iter == health_check_log_times.end()) {
                    continue;
                }

                auto &log_status = status_iter->second;
                bool const changed = log_status.last_message != summary
                                     || log_status.severity != severity;

                if (changed && now - log_status.updated_at > min_period) {
                    log_status.last_message = summary;
                    log_status.updated_at = now;
                    log_status.severity = severity;

                    ostringstream ss;
                    ss << "Health check update: " << summary << " (" << code << ")";
                    clog->health(severity) << ss.str();
                }
            }
        }
    }

    for (auto &svc: services) {
        svc->tick();
        svc->maybe_trim();
    }

    // trim sessions
    {
        std::lock_guard l(session_map_lock);
        auto p = session_map.sessions.begin();

        bool out_for_too_long = (!exited_quorum.is_zero() &&
                                 now > (exited_quorum + 2 * g_conf()->mon_lease));

        while (!p.end()) {
            MonSession *s = *p;
            ++p;

            // don't trim monitors
            if (s->name.is_mon())
                continue;

            if (s->session_timeout < now && s->con) {
                // check keepalive, too
                s->session_timeout = s->con->get_last_keepalive();
                s->session_timeout += g_conf()->mon_session_timeout;
            }
            if (s->session_timeout < now) {
                dout(10) << " trimming session " << s->con << " " << s->name
                         << " " << s->addrs
                         << " (timeout " << s->session_timeout
                         << " < now " << now << ")" << dendl;
            } else if (out_for_too_long) {
                // boot the client Session because we've taken too long getting back in
                dout(10) << " trimming session " << s->con << " " << s->name
                         << " because we've been out of quorum too long" << dendl;
            } else {
                continue;
            }

            s->con->mark_down();
            remove_session(s);
            logger->inc(l_mon_session_trim);
        }
    }

    sync_trim_providers();

    if (!maybe_wait_for_quorum.empty()) {
        finish_contexts(g_ceph_context, maybe_wait_for_quorum);
    }

    if (is_leader() && febft->is_active() && fingerprint.is_zero()) {
        // this is only necessary on upgraded clusters.
        MonitorDBStore::TransactionRef t = febft->get_pending_transaction();
        prepare_new_fingerprint(t);
        febft->trigger_propose();
    }

    mgr_client.update_daemon_health(get_health_metrics());
    new_tick();
}

void FebftMonitor::shutdown() {
    dout(1) << "shutdown" << dendl;

    lock.lock();

    {
        std::lock_guard l(auth_lock);
        authmon()->_set_mon_num_rank(0, 0);
    }

    lock.unlock();
    g_conf().remove_observer(this);
    lock.lock();

    if (admin_hook) {
        cct->get_admin_socket()->unregister_commands(admin_hook);
        delete admin_hook;
        admin_hook = NULL;
    }

    mgr_client.shutdown();

    lock.unlock();
    finisher.wait_for_empty();
    finisher.stop();
    lock.lock();

    // clean up
    febft->shutdown();
    for (auto &svc: services) {
        svc->shutdown();
    }

    finish_contexts(g_ceph_context, waitfor_quorum, -ECANCELED);
    finish_contexts(g_ceph_context, maybe_wait_for_quorum, -ECANCELED);

    timer.shutdown();

    cpu_tp.stop();

    remove_all_sessions();

    log_client.shutdown();

    // unlock before msgr shutdown...
    lock.unlock();

    // shutdown messenger before removing logger from perfcounter collection,
    // otherwise _ms_dispatch() will try to update deleted logger
    messenger->shutdown();
    mgr_messenger->shutdown();

    if (logger) {
        cct->get_perfcounters_collection()->remove(logger);
    }

    if (cluster_logger) {
        if (cluster_logger_registered)
            cct->get_perfcounters_collection()->remove(cluster_logger);
        delete cluster_logger;
        cluster_logger = NULL;
    }
}