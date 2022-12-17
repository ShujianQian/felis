cmake_minimum_required(VERSION 3.10)

project(felis C CXX ASM)

set(CMAKE_CXX_STANDARD 17)

#=======================================================================================================================
#
#=======================================================================================================================

#add_subdirectory(gopp)
#add_subdirectory(json11)
#add_subdirectory(masstree)
#add_subdirectory(spdlog)

add_definitions(-DCACHE_LINE_SIZE=64)

add_executable(db
        main.cc module.cc
        epoch.cc routine_sched.cc txn.cc log.cc vhandle.cc vhandle_sync.cc contention_manager.cc locality_manager.cc
        gc.cc index.cc mem.cc
        piece.cc masstree_index_impl.cc hashtable_index_impl.cc
        node_config.cc console.cc console_client.cc
        commit_buffer.cc shipping.cc entity.cc iface.cc slice.cc tcp_node.cc
        felis_probes.cc
        #priority.cc
        #extravhandle.cc extravhandle.h

        json11/json11.cpp

        masstree/kvthread.cc
        masstree/string.cc
        masstree/straccum.cc

        xxHash/xxhash.c

        util/os_linux.cc
        util/locks.cc

        pwv_graph.cc

        benchmark/ycsb/ycsb.cc
        benchmark/ycsb/ycsb_workload.cc
        #benchmark/ycsb/ycsb_priority.cc
        benchmark/ycsb_dist/ycsb_dist.cc
        benchmark/ycsb_dist/ycsb_dist_workload.cc

        benchmark/tpcc/tpcc.cc
        benchmark/tpcc/tpcc_workload.cc
        benchmark/tpcc/new_order.cc
        benchmark/tpcc/payment.cc
        benchmark/tpcc/delivery.cc
        benchmark/tpcc/order_status.cc
        benchmark/tpcc/stock_level.cc
        #benchmark/tpcc/tpcc_priority.cc
        #benchmark/tpcc/pri_stock.cc
        #benchmark/tpcc/pri_new_order_delivery.cc

        gopp/gopp.cc
        gopp/channels.cc
        gopp/start-x86_64.S

        coroutine.c
        coro_switch.asm
        coro_sched.cpp
        )

target_include_directories(db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/spdlog/include)

target_link_libraries(db pthread rt dl)

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(sample_test
        test/sample_test.cc
)
target_link_libraries(
        sample_test
        GTest::gtest_main
)

add_executable(coroutine_test test/coroutine_test.cc coroutine.c coro_switch.asm)
target_link_libraries(coroutine_test GTest::gtest_main)
target_include_directories(coroutine_test PUBLIC ${CMAKE_CURRENT_LIST_DIR}/)

include(GoogleTest)
gtest_discover_tests(sample_test)
gtest_discover_tests(coroutine_test)

add_executable(coroutine_benchmark benchmarks/coroutine_benchmark.c benchmarks/bst_benchmark.c coroutine.c coro_switch.asm)
target_include_directories(coroutine_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(coroutine_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/spdlog/include)
