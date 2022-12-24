#include "ycsb_dist.h"
#include "index.h"
#include "txn_cc.h"
#include "pwv_graph.h"
#include "util/os.h"
#include "node_config.h"

std::atomic<uint64_t> total_dist_generated = 0;
uint64_t epoch_nr = 0;
uint64_t generated_buffer_plan[255][255][255] = {0};

namespace ycsb_dist {

using namespace felis;

static constexpr int kTotal = 10;
static constexpr int kNrMSBContentionKey = 6;

struct RMWStruct {
  uint64_t write_keys[kTotal];
  uint64_t read_keys[kTotal];
};

struct DistRMWState {
  VHandle *rows[kTotal];
  VHandle *read_rows[kTotal];

  FutureValue<int32_t> future_vals[kTotal]; // future value alloc array

  NodeBitmap nodes;

  struct ReadRowLookupCompletion : public TxnStateCompletion<DistRMWState> {
    void operator()(int id, BaseTxn::LookupRowResult lookup_results) {
      state->read_rows[id] = lookup_results[0];
      state->future_vals[id] = FutureValue<int32_t>();
    }
  };

  struct WriteRowLookupCompletion : public TxnStateCompletion<DistRMWState> {
    void operator()(int id, BaseTxn::LookupRowResult lookup_results) {
      state->rows[id] = lookup_results[0];
      if (id < kTotal - Client::g_extra_read) {
        bool last = (id == kTotal - Client::g_extra_read - 1);
        handle(lookup_results[0]).AppendNewVersion(last ? 0 : 1);
      }
      state->future_vals[id] = FutureValue<int32_t>();
    }
  };
};

template <>
RMWStruct Client::GenerateTransactionInput<RMWStruct>()
{
  RMWStruct s;
  auto &locator = util::Instance<SliceLocator<YcsbDist>>();
  sql::YcsbKey read_dbk, write_dbk;

  int nr_lsb = 63 - __builtin_clzll(g_table_size) - kNrMSBContentionKey;
  size_t mask = 0;
  if (nr_lsb > 0) mask = (1 << nr_lsb) - 1;

  auto &conf = util::Instance<NodeConfiguration>();
  int curr_node = conf.node_id();

  for (int i = 0; i < kTotal; i++) {
    bool dist = (rand.next() % 100) < g_dist_factor;
 again:
    s.write_keys[i] = rand.next() % g_table_size;
    s.read_keys[i] = rand.next() % g_table_size;
    read_dbk.k = s.read_keys[i];
    write_dbk.k = s.write_keys[i];
    int read_node = YCSBDistSlicerRouter::SliceToNodeId(locator.Locate(read_dbk));
    int write_node = YCSBDistSlicerRouter::SliceToNodeId(locator.Locate(write_dbk));
    if (dist != (read_node != write_node)) goto again;
    for (int j = 0; j < i; j++) {
      if (s.write_keys[i] == s.write_keys[j]) goto again;
      if (s.read_keys[i] == s.read_keys[j]) goto again;
    }
    if (read_node != write_node) total_dist_generated++;
    if (read_node == write_node) {
      if (read_node != curr_node) generated_buffer_plan[epoch_nr][curr_node - 1][read_node - 1]++;
    } else {
      if (read_node != curr_node) generated_buffer_plan[epoch_nr][curr_node - 1][read_node - 1]++;
      if (write_node != curr_node) generated_buffer_plan[epoch_nr][curr_node - 1][write_node - 1]++;
      generated_buffer_plan[epoch_nr][read_node - 1][write_node - 1]++;
    }
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
  static void ReadWriteRowSingleNode(TxnRow read_vhandle, TxnRow write_vhandle);
  static void ReadAndSend(TxnRow read_vhandle, int dest_node, FutureValue <int32_t> &future, int future_origin_node);
  static void ReceiveAndWrite(TxnRow write_vhandle, FutureValue <int32_t> &future);
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
  INIT_ROUTINE_BRK(8192);

  for (int i = 0; i < kTotal; i++) dbk[i].k = read_keys[i];
  TxnIndexLookup<YCSBDistSlicerRouter, DistRMWState::ReadRowLookupCompletion, void>(
      nullptr,
      KeyParam<YcsbDist>(dbk, kTotal));
  for (int i = 0; i < kTotal; i++) dbk[i].k = write_keys[i];
//  state->nodes =
  TxnIndexLookup<YCSBDistSlicerRouter, DistRMWState::WriteRowLookupCompletion, void>(
      nullptr,
      KeyParam<YcsbDist>(dbk, kTotal));
}

//namespace ycsb_dist {
//__thread uint64_t ycsb_dist_waiting_key;
//}

void DistRMWTxn::ReadWriteRowSingleNode(TxnRow read_vhandle, TxnRow write_vhandle)
{
  auto dbv = read_vhandle.Read<YcsbDist::Value>();
  dbv.v.assign(Client::zero_data, 100);
  dbv.v.resize_junk(999);
  write_vhandle.Write(dbv);
}

void DistRMWTxn::ReadAndSend(TxnRow read_vhandle, int dest_node, FutureValue <int32_t> &future, int future_origin_node)
{
  auto dbv = read_vhandle.Read<YcsbDist::Value>();
  future.Subscribe(dest_node);
  future.SignalDistributed(0, future_origin_node); // send dummy dependency for now
                                                   // sending the actual data (2000 Bytes) stress the network too much
}

void DistRMWTxn::ReceiveAndWrite(Txn<DistRMWState>::TxnRow write_vhandle, FutureValue<int32_t> &future) {
  future.Wait();
  sql::YcsbValue dbv;
  dbv.v.assign(Client::zero_data, 100);
  dbv.v.resize_junk(999);
  write_vhandle.Write(dbv);
}

void DistRMWTxn::ReadRow(TxnRow vhandle)
{
  vhandle.Read<YcsbDist::Value>();
}

void DistRMWTxn::Run()
{
  auto &conf = util::Instance<NodeConfiguration>();
  int curr_node_id = conf.node_id();

  YcsbDist::Key read_dbk;
  YcsbDist::Key write_dbk;
  auto &locator = util::Instance<SliceLocator<YcsbDist>>();
  for (int i = 0; i < kTotal; i++) {
    read_dbk.k = read_keys[i];
    auto read_node = YCSBDistSlicerRouter::SliceToNodeId(locator.Locate(read_dbk));
    auto read_aff = YCSBDistSlicerRouter::SliceToCoreId(locator.Locate(read_dbk));
    write_dbk.k = write_keys[i];
    auto write_node = YCSBDistSlicerRouter::SliceToNodeId(locator.Locate(write_dbk));
    auto write_aff = YCSBDistSlicerRouter::SliceToCoreId(locator.Locate(write_dbk));
    if (read_node == write_node) {
      // read and write are one the same node, only one piece is required
      root->AttachRoutine(
          MakeContext(i), write_node,
          [](const auto &ctx) {
            auto &[state, index_handle, idx] = ctx;
            ReadWriteRowSingleNode(index_handle(state->read_rows[idx]), index_handle(state->rows[idx]));
          },
          write_aff);
    } else {
      root->AttachRoutine(
          MakeContext(i, write_node, curr_node_id), read_node,
          [](const auto &ctx) {
            auto &[state, index_handle, idx, dest_node, future_origin_node] = ctx;
            ReadAndSend(index_handle(state->read_rows[idx]), dest_node, state->future_vals[idx], future_origin_node);
          },
          read_aff);
      root->AttachRoutine(
          MakeContext(i), write_node,
          [](const auto &ctx) {
            auto &[state, index_handle, idx] = ctx;
            ReceiveAndWrite(index_handle(state->rows[idx]), state->future_vals[idx]);
          },
          write_aff, 1, read_node);
    }
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
}

size_t Client::g_table_size = 1 << 24;
double Client::g_theta = 0.00;
int Client::g_extra_read = 0;
int Client::g_contention_key = 0;
bool Client::g_dependency = false;
int Client::g_dist_factor = 0;

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
