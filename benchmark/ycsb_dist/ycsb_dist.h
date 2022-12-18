#ifndef YCSB_H
#define YCSB_H

#include "table_decl.h"
#include "epoch.h"
#include "slice.h"
#include "index.h"

#include "zipfian_random.h"

namespace ycsb_dist {

enum class TableType : int {
  YCSBBase = 200,
  Ycsb,
};

struct YcsbDist {
  static uint32_t HashKey(const felis::VarStrView &k) {
    auto x = (uint8_t *) k.data();
    return *(uint32_t *) x;
  }

  static constexpr auto kTable = TableType::Ycsb;
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 1 << 24, false);

  using IndexBackend = felis::HashtableIndex;
  using Key = sql::YcsbKey;
  using Value = sql::YcsbValue;
};

using RandRng = foedus::assorted::ZipfianRandom;

class Client : public felis::EpochClient {
  // Zipfian random generator
  RandRng rand;

  friend class LocalRMWTxn;
  static char zero_data[100];
 public:
  static double g_theta;
  static size_t g_table_size;
  static int g_extra_read;
  static int g_contention_key;
  static bool g_dependency;
  static double g_dist_factor;

  Client() noexcept;
  unsigned int LoadPercentage() final override { return 100; }
  felis::BaseTxn *CreateTxn(uint64_t serial_id) final override;

  template <typename T> T GenerateTransactionInput();

  friend class LocalRMWTxn;
  friend class DistRMWTxn;
};

class YCSBDistSlicerRouter {
public:
  static int SliceToNodeId(int16_t slice_id) {
    uint64_t u_slice_id = (uint16_t) slice_id;
    const uint64_t max_slice_id = 1 << 15;
    uint64_t nr_nodes = util::Instance<felis::NodeConfiguration>().nr_nodes();
    uint64_t slice_per_node = max_slice_id / nr_nodes;
    return (int) (u_slice_id / slice_per_node) + 1;
  }
  static int SliceToCoreId(int16_t slice_id) {
    uint64_t u_slice_id = (uint16_t) slice_id;
    const uint64_t max_slice_id = 1 << 15;
    uint64_t nr_nodes = util::Instance<felis::NodeConfiguration>().nr_nodes();
    uint64_t nr_threads_per_node = util::Instance<felis::NodeConfiguration>().g_nr_threads;
    uint64_t nr_threads = nr_nodes * nr_threads_per_node;
    uint64_t slice_per_thread = max_slice_id / nr_threads;
    return (int) (u_slice_id / slice_per_thread);
  }
};

class YcsbDistLoader : public go::Routine {
  std::atomic_bool done = false;
  int node_id;
 public:
  YcsbDistLoader() : node_id{util::Instance<felis::NodeConfiguration>().node_id()} { }
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
  template <typename FuncT>
  inline void DoOnSlice(YcsbDist::Key &key, int core_id, FuncT func) {
    auto slice_id = util::Instance<felis::SliceLocator<YcsbDist>>().Locate(key);
//    auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    if (YCSBDistSlicerRouter::SliceToNodeId(slice_id) == node_id
        && YCSBDistSlicerRouter::SliceToCoreId(slice_id) == core_id) {
      func(slice_id, core_id);
    }
  }
};

}

namespace felis {

using namespace ycsb_dist;

/**
 * Maps a YCSB key to a slice. Which is then used by the YCSBDistSliceRouter to map onto a node.
 * @param key
 * @return
 */
SHARD_TABLE(YcsbDist) {
  auto &key_val = key.k;
  // use the most significant 15 bits as the slice id
  return (int16_t) (key_val >> 9);
}

}

#endif
