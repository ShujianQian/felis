#include "ycsb_dist.h"
#include "index.h"
#include "txn_cc.h"
#include "pwv_graph.h"
#include "util/os.h"

namespace ycsb_dist {

using namespace felis;

static constexpr int kTotal = 10;
static constexpr int kNrMSBContentionKey = 6;



// static uint64_t *g_permutation_map;

struct RMWStruct {
  uint64_t write_keys[kTotal];
  uint64_t read_keys[kTotal];
};

struct DistRMWState {
  VHandle *rows[kTotal];
  InvokeHandle<DistRMWState, uint64_t> futures[kTotal];

  FutureValue<uint64_t> future_vals[kTotal]; // future value alloc array

  NodeBitmap nodes;

  std::atomic_ulong signal; // Used only if g_dependency
  FutureValue<void> deps; // Used only if g_dependency

  struct LookupCompletion : public TxnStateCompletion<DistRMWState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      state->rows[id] = rows[0];
      if (id < kTotal - Client::g_extra_read) {
        bool last = (id == kTotal - Client::g_extra_read - 1);
        handle(rows[0]).AppendNewVersion(last ? 0 : 1);
      }
      // init default future values
      for (int i = 0; i < kTotal; i++) {
        state->future_vals[i] = FutureValue<uint64_t>();
      }
    }
  };
};

template <>
RMWStruct Client::GenerateTransactionInput<RMWStruct>()
{
  RMWStruct s;

  int nr_lsb = 63 - __builtin_clzll(g_table_size) - kNrMSBContentionKey;
  size_t mask = 0;
  if (nr_lsb > 0) mask = (1 << nr_lsb) - 1;

  for (int i = 0; i < kTotal; i++) {
 again:
//     s.write_keys[i] = g_permutation_map[rand.next() % g_table_size];
    s.write_keys[i] = rand.next() % g_table_size;
//    if (i < g_contention_key) {
//      s.write_keys[i] &= ~mask;
//    } else {
//      if ((s.write_keys[i] & mask) == 0)
//        goto again;
//    }
    for (int j = 0; j < i; j++)
      if (s.write_keys[i] == s.write_keys[j])
        goto again;
  }

  return s;
}

char Client::zero_data[100];

// ------------------- Distributed RMW -----------------

// has constant array of future values (10 read/write)
// sender:
//  - read subscribe() signal()
//
// receiver
//  - wait() modify write
//
// allocate future value
// tcpnodetransport::transportfuturevalue()
// FutureValue::genericEpochObject(),  val->convert() make a future value to epoch object then send
// future value allocated within PaymentState, passed as TxnState type for payment txn
class DistRMWTxn : public Txn<DistRMWState>, public RMWStruct {
  Client *client;
public:
  DistRMWTxn(Client *client, uint64_t serial_id);
  void Run() override final;
  void Prepare() override final;
  void PrepareInsert() override final {}
  static void WriteRow(TxnRow vhandle);
  static void ReadRow(TxnRow vhandle);

  template <typename Func>
  void RunOnPartition(Func f) {
    auto handle = index_handle();
    for (int i = 0; i < kTotal; i++) {
      auto part = (write_keys[i] * NodeConfiguration::g_nr_threads) / Client::g_table_size;
      f(part, root, Tuple<unsigned long, int, decltype(state), decltype(handle), int>(write_keys[i], i, state, handle, part));
    }
  }
};

DistRMWTxn::DistRMWTxn(Client *client, uint64_t serial_id)
    : Txn<DistRMWState>(serial_id),
      RMWStruct(client->GenerateTransactionInput<RMWStruct>()),
      client(client)
{}

void DistRMWTxn::Prepare()
{
  YcsbDist::Key dbk[kTotal];
  auto current_node = util::Instance<NodeConfiguration>().node_id();
  for (int i = 0; i < kTotal; i++) dbk[i].k = write_keys[i];
  INIT_ROUTINE_BRK(8192);

////  txn_indexop_affinity = Ycsb
//  auto &locator = util::Instance<SliceLocator<YcsbDist>>();
//  state->nodes = GenerateNodeBitmap<YCSBDistSlicerRouter>(KeyParam<YcsbDist>(dbk, kTotal));
//  int num_version_appended = 0;
//  for (auto &p : state->nodes) {
//    auto [node, bitmap] = p;
//    auto op_ctx = TxnIndexOpContextEx<void>(index_handle(), state, bitmap, KeyParam<YcsbDist>(dbk, kTotal));
////    auto op_ctx = TxnIndexOpContext();
////    op_ctx.handle = index_handle();
////    op_ctx.state = state;
////    op_ctx.keys_bitmap = op_ctx.slices_bitmap = op_ctx.rels_bitmap = bitmap;
////    op_ctx.node_id = node;
////    op_ctx.src_node_id = current_node;
//    if (node != current_node) {
//      for (int i = 0; i < kTotal; i++) {
//        const uint16_t mask = 1 << i;
//        if (bitmap & mask) {
//          num_version_appended++;
//          auto aff = YCSBDistSlicerRouter::SliceToCoreId(locator.Locate(dbk[i]));
//          root->AttachRoutine(
//              op_ctx, node,
//              [](auto &ctx) {
//                auto completion = DistRMWState::LookupCompletion();
//                completion.handle = ctx.handle;
//                completion.state = GenericEpochObject<DistRMWState>(ctx.state);
//                auto op = TxnIndexLookupOpImpl(ctx, 0); //FIXME: hard coded
//                completion(0, op.result);
//              }, aff);
//        }
//      }
//    } else {
//      for (int i = 0; i < kTotal; i++) {
//        const uint16_t mask = 1 << i;
//        if (bitmap & mask) {
//          num_version_appended++;
//          auto completion = DistRMWState::LookupCompletion();
//          completion.handle = TxnHandle(op_ctx.handle);
//          completion.state = GenericEpochObject<DistRMWState>(op_ctx.state);
//          auto op = TxnIndexLookupOpImpl(op_ctx, 0); //FIXME: hard coded
//          completion(0, op.result);
//        }
//      }
//    }
//  }
//
//  assert(num_version_appended == 1);

//  txn_indexop_affinity = YCSBDistSlicerRouter::SliceToCoreId(locator.Locate(dbk[0]));
  state->nodes =
      TxnIndexLookup<YCSBDistSlicerRouter, DistRMWState::LookupCompletion, void>(
        nullptr,
        KeyParam<YcsbDist>(dbk, kTotal));

}

//namespace ycsb_dist {
//__thread uint64_t ycsb_dist_waiting_key;
//}

void DistRMWTxn::WriteRow(TxnRow vhandle)
{
  auto dbv = vhandle.Read<YcsbDist::Value>();
  dbv.v.assign(Client::zero_data, 100);
  dbv.v.resize_junk(999);
  vhandle.Write(dbv);
}

void DistRMWTxn::ReadRow(TxnRow vhandle)
{
  vhandle.Read<YcsbDist::Value>();
}

void DistRMWTxn::Run()
{
  auto &conf = util::Instance<NodeConfiguration>();
  for (auto &p: state->nodes) {
    auto [node, bitmap] = p;

//    if (conf.node_id() == node) {

//      for (int i = 0; i < kTotal - Client::g_extra_read - 1; i++) {
//        state->futures[i] = UpdateForKey(
//            node, state->rows[i],
//            [](const auto &ctx, VHandle *row) {
//
//
//              auto &[state, index_handle, reader_node] = ctx;
//              WriteRow(index_handle(row));
////	      state->future_vals[i].Subscribe(reader_node);
////	      state->future_vals[i].Signal();
//            }, reader_nodes[i]);
//
//      }

//      auto aff = std::numeric_limits<uint64_t>::max();
    YcsbDist::Key dbk;
    auto &locator = util::Instance<SliceLocator<YcsbDist>>();
      // auto aff = AffinityFromRows(bitmap, state->rows);
      //
//      auto aff = reader_nodes[];
//      int node = 1;
      for (int i = 0; i < kTotal; i++) {
        const uint16_t mask = 1 << i;
        if (bitmap & mask) {
          dbk.k = write_keys[i];
          auto aff = YCSBDistSlicerRouter::SliceToCoreId(locator.Locate(dbk));
          root->AttachRoutine(
              MakeContext(i, write_keys[i]), node,
              [](const auto &ctx) {
                auto &[state, index_handle, idx, write_key] = ctx;
//                ycsb_dist::ycsb_dist_waiting_key = write_key;
                WriteRow(index_handle(state->rows[idx]));
              },
              aff);
        }
      }
    
    // not local node
//    } else {
      
//    }
  }
}

void YcsbDistLoader::Run() {
  auto &mgr = util::Instance<felis::TableManager>();
  mgr.Create<YcsbDist>();

  void *buf = alloca(512);

  auto nr_threads = NodeConfiguration::g_nr_threads;
  for (auto t = 0; t < nr_threads; t++) {
    printf("t = %d\n", t);
    MasstreeIndex::ResetThreadInfo();

    mem::ParallelPool::SetCurrentAffinity(t);
    util::Cpu info;
    info.set_affinity(t);
    info.Pin();

//    unsigned long start = t * Client::g_table_size / nr_threads;
//    unsigned long end = (t + 1) * Client::g_table_size / nr_threads;

    for (unsigned long i = 0; i < Client::g_table_size; i++) {
      YcsbDist::Key dbk;
      dbk.k = i;
      DoOnSlice(
          dbk,
          t,
          [&](auto slice_id, auto core_id) {
            YcsbDist::Value dbv;
            dbv.v.resize_junk(999);

            auto handle = mgr.Get<ycsb_dist::YcsbDist>().SearchOrCreate(dbk.EncodeView(buf));
            felis::InitVersion(handle, dbv.Encode());
          });
    }
  }
  util::Cpu info;
  info.set_affinity(go::Scheduler::CurrentThreadPoolId() - 1);
  info.Pin();

  mem::ParallelPool::SetCurrentAffinity(-1);
  MasstreeIndex::ResetThreadInfo();

  for (unsigned long i = 0; i < Client::g_table_size; i++) {
    YcsbDist::Key dbk;
    dbk.k = i;
    auto slice_id = util::Instance<felis::SliceLocator<YcsbDist>>().Locate(dbk);
    if (YCSBDistSlicerRouter::SliceToNodeId(slice_id) == node_id) {
      assert(mgr.Get<ycsb_dist::YcsbDist>().Search(dbk.EncodeView(buf)) != nullptr);
    }
  }

  done = true;

  // Generate a random permutation
#if 0
  g_permutation_map = new uint64_t[Client::g_table_size];
  for (size_t i = 0; i < Client::g_table_size; i++) {
    g_permutation_map[i] = i;
  }
  util::FastRandom perm_rand(1001);
  for (size_t i = Client::g_table_size - 1; i >= 1; i--) {
    auto j = perm_rand.next() % (i + 1);
    std::swap(g_permutation_map[j], g_permutation_map[i]);
  }
#endif
}

size_t Client::g_table_size = 1 << 24;
double Client::g_theta = 0.00;
int Client::g_extra_read = 0;
int Client::g_contention_key = 0;
bool Client::g_dependency = false;
double Client::g_dist_factor = 0.00;

Client::Client() noexcept
{
  uint64_t node_id = util::Instance<NodeConfiguration>().node_id();
  rand.init(g_table_size, g_theta, node_id);
}

BaseTxn *Client::CreateTxn(uint64_t serial_id)
{
  // TODO: add distributed txns based on percentage
  return new DistRMWTxn(this, serial_id);
}

}
