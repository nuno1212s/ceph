set(lib_mon_srcs
        ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxKeyServer.cc
        ${CMAKE_SOURCE_DIR}/src/auth/cephx/CephxServiceHandler.cc
        ${CMAKE_SOURCE_DIR}/src/auth/AuthServiceHandler.cc
        OSDMonitor.cc
        MDSMonitor.cc
        CommandHandler.cc
        FSCommands.cc
        MgrMonitor.cc
        MgrStatMonitor.cc
        MonmapMonitor.cc
        LogMonitor.cc
        AuthMonitor.cc
        ConfigMap.cc
        ConfigMonitor.cc
        cft/Elector.cc
        ElectionLogic.cc
        ConnectionTracker.cc
        HealthMonitor.cc
        KVMonitor.cc
        ../mds/MDSAuthCaps.cc
        ../mgr/mgr_commands.cc
        ../osd/OSDCap.cc
        ../mgr/MgrCap.cc
        Service.cpp Service.h SMRProtocol.h
        AbstractMonitor.cc AbstractMonitor.h
        cft/PaxosMonitor.cc cft/PaxosMonitor.h
        cft/PaxosSmr.cc cft/PaxosSmr.h
        bft/FebftSmr.cc bft/FebftSmr.h
        bft/febft_rust_interface.h)

if (TARGET mgr_cap_obj)
    list(APPEND lib_mon_srcs $<TARGET_OBJECTS:mgr_cap_obj>)
endif ()

if (HAVE_GSSAPI)
    list(APPEND lib_mon_srcs
            ${CMAKE_SOURCE_DIR}/src/auth/krb/KrbServiceHandler.cpp)
endif ()

add_library(mon STATIC
        ${lib_mon_srcs})
target_link_libraries(mon
        kv
        heap_profiler
        fmt::fmt)
if (WITH_JAEGER)
    target_link_libraries(mon jaeger_base)
endif ()
