# BUILD FILE SYNTAX: SKYLARK

includes = ['-I.', '-Ispdlog/include']

tpcc_headers = [
    'benchmark/tpcc/table_decl.h',
    'benchmark/tpcc/tpcc.h',
    'benchmark/tpcc/new_order.h',
    'benchmark/tpcc/payment.h',
    'benchmark/tpcc/delivery.h',
    'benchmark/tpcc/order_status.h',
    'benchmark/tpcc/stock_level.h',
    'benchmark/tpcc/tpcc_priority.h',
    'benchmark/tpcc/pri_stock.h',
    'benchmark/tpcc/pri_new_order_delivery.h',
]

tpcc_srcs = [
    'benchmark/tpcc/tpcc.cc',
    'benchmark/tpcc/tpcc_workload.cc',
    'benchmark/tpcc/new_order.cc',
    'benchmark/tpcc/payment.cc',
    'benchmark/tpcc/delivery.cc',
    'benchmark/tpcc/order_status.cc',
    'benchmark/tpcc/stock_level.cc',
    'benchmark/tpcc/tpcc_priority.cc',
    'benchmark/tpcc/pri_stock.cc',
    'benchmark/tpcc/pri_new_order_delivery.cc',
]

ycsb_headers = [
    'benchmark/ycsb/table_decl.h',
    'benchmark/ycsb/ycsb.h',
    'benchmark/ycsb/ycsb_priority.h',
]

ycsb_srcs = [
    'benchmark/ycsb/ycsb.cc',
    'benchmark/ycsb/ycsb_workload.cc',
    'benchmark/ycsb/ycsb_priority.cc',
]

db_headers = [
    'console.h', 'felis_probes.h', 'epoch.h', 'routine_sched.h', 'gc.h', 'index.h', 'index_common.h',
    'log.h', 'mem.h', 'module.h', 'opts.h', 'node_config.h', 'probe_utils.h', 'piece.h', 'piece_cc.h',
    'masstree_index_impl.h', 'hashtable_index_impl.h', 'varstr.h', 'sqltypes.h',
    'txn.h', 'txn_cc.h', 'vhandle.h', 'vhandle_sync.h', 'contention_manager.h', 'locality_manager.h', 'threshold_autotune.h',
    'commit_buffer.h', 'shipping.h', 'completion.h', 'entity.h',
    'slice.h', 'vhandle_cch.h', 'tcp_node.h',
    'util/arch.h', 'util/factory.h', 'util/linklist.h', 'util/locks.h', 'util/lowerbound.h', 'util/objects.h', 'util/random.h', 'util/types.h',
    'pwv_graph.h',
    'priority.h',
    'extravhandle.h',
]

db_srcs = [
    'epoch.cc', 'routine_sched.cc', 'txn.cc', 'log.cc', 'vhandle.cc', 'vhandle_sync.cc', 'contention_manager.cc', 'locality_manager.cc',
    'gc.cc', 'index.cc', 'mem.cc',
    'piece.cc', 'masstree_index_impl.cc', 'hashtable_index_impl.cc',
    'node_config.cc', 'console.cc', 'console_client.cc',
    'commit_buffer.cc', 'shipping.cc', 'entity.cc', 'iface.cc', 'slice.cc', 'tcp_node.cc',
    'felis_probes.cc',
    'priority.cc',
    'extravhandle.cc',
    'json11/json11.cpp',
    'spdlog/src/spdlog.cpp', 'spdlog/src/fmt.cpp', 'spdlog/src/stdout_sinks.cpp', 'spdlog/src/async.cpp', 'spdlog/src/cfg.cpp', 'spdlog/src/color_sinks.cpp', 'spdlog/src/file_sinks.cpp',
    'xxHash/xxhash.c', 'util/os_linux.cc', 'util/locks.cc',
    'pwv_graph.cc',
    'gopp/gopp.cc', 'gopp/channels.cc',
    'gopp/start-x86_64.S'] + [
        ('masstree/kvthread.cc', ['-include', 'masstree/build/config.h']),
	('masstree/string.cc', ['-include', 'masstree/build/config.h']),
	('masstree/straccum.cc', ['-include', 'masstree/build/config.h']),
    ]

libs = ['-pthread', '-lrt', '-ldl']
#test_srcs = ['test/promise_test.cc', 'test/serializer_test.cc', 'test/shipping_test.cc']
test_srcs = ['test/xnode_measure_test.cc']

cxx_library(
    name='tpcc',
    srcs=tpcc_srcs,
    compiler_flags=includes,
    headers=db_headers + tpcc_headers,
    link_whole=True,
)

cxx_library(
    name='ycsb',
    srcs=ycsb_srcs,
    compiler_flags=includes,
    headers=db_headers + ycsb_headers,
    link_whole=True,
)

cxx_binary(
    name='db',
    srcs=['main.cc', 'module.cc'] + db_srcs,
    headers=db_headers,
    compiler_flags=includes,
    linker_flags=libs,
    deps=[':tpcc', ':ycsb'],
)

cxx_test(
    name='dbtest',
    srcs=test_srcs + db_srcs,
    headers=db_headers,
    compiler_flags=includes,
    linker_flags=libs + ['-lgtest_main', '-lgtest'],
    deps=[':tpcc']
)
