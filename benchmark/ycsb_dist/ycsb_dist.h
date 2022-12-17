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
  static constexpr auto kIndexArgs = std::make_tuple(HashKey, 10000000, false);

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
};

class YcsbDistLoader : public go::Routine {
  std::atomic_bool done = false;
 public:
  YcsbDistLoader() {}
  void Run() override final;
  void Wait() { while (!done) sleep(1); }
};

}

namespace felis {

using namespace ycsb_dist;

SHARD_TABLE(YcsbDist) { return 0; }

}

#endif
