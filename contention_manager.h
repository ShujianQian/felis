#ifndef VHANDLE_BATCHAPPENDER_H
#define VHANDLE_BATCHAPPENDER_H

#include "vhandle.h"

namespace felis {

struct VersionBufferHandle {
  uint8_t *prealloc_ptr;
  long pos;

  void Append(VHandle *handle, uint64_t sid, uint64_t epoch_nr, bool is_ondemand_split);
  void FlushIntoNoLock(VHandle *handle, uint64_t epoch_nr, unsigned int end);
};

struct VersionBufferHead;

class ContentionManager {
  friend class VersionBufferHead;
  std::array<VersionBufferHead *, NodeConfiguration::kMaxNrThreads> buffer_heads;
  size_t est_split;

 public:
  ContentionManager();
  VersionBufferHandle GetOrInstall(VHandle *handle);
  void FinalizeFlush(uint64_t epoch_nr);
  void Reset();
  int GetRowContentionAffinity(VHandle *row) const;

  size_t estimated_splits() const { return est_split; }

  static size_t g_prealloc_count;

 private:
  static size_t BinPack(VHandle **knapsacks, unsigned int nr_knapsack, int label, size_t limit);
  static void PackLeftOver(VHandle **knapsacks, unsigned int nr_knapsack, int label);
};

}

namespace util {

template <> struct InstanceInit<felis::ContentionManager> {
  static constexpr bool kHasInstance = true;
  static inline felis::ContentionManager *instance;
  InstanceInit();
};

}

#endif
