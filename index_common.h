// -*- C++ -*-
#ifndef INDEX_COMMON_H
#define INDEX_COMMON_H

#include <memory>
#include <mutex>
#include <atomic>
#include <cstdlib>

#include "mem.h"
#include "log.h"
#include "util.h"

#include "vhandle.h"
#include "node_config.h"
#include "shipping.h"

namespace felis {

using util::ListNode;

class Checkpoint {
  static std::map<std::string, Checkpoint *> impl;
 public:
  static void RegisterCheckpointFormat(std::string fmt, Checkpoint *pimpl) { impl[fmt] = pimpl; }
  static Checkpoint *checkpoint_impl(std::string fmt) { return impl[fmt]; }
  virtual void Export() = 0;
};

// Relations is an index or a table. Both are associates between keys and
// rows.
//
// Since we are doing replay, we need two layers of indexing.
// First layer is indexing the keys.
// Second layer is the versioning layer. The versioning layers could be simply
// kept as a sorted array
class BaseRelation {
 public:
  static constexpr size_t kAutoIncrementZones = 1024;

 protected:
  int id;
  bool read_only;
  size_t key_len;
  std::atomic_uint64_t auto_increment_cnt[kAutoIncrementZones];
 public:
  BaseRelation() : id(-1), read_only(false) {}

  void set_id(int relation_id) { id = relation_id; }
  int relation_id() { return id; }

  void set_key_length(size_t l) { key_len = l; }
  size_t key_length() const { return key_len; }

  void set_read_only(bool v) { read_only = v; }
  bool is_read_only() const { return read_only; }

  // In a distributed environment, we may need to generate a AutoIncrement key
  // on one node and insert on another. In order to prevent conflict, we need to
  // attach our node id at th end of the key.
  uint64_t AutoIncrement(int zone = 0) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto ts = auto_increment_cnt[zone].fetch_add(1);
    return (ts << 8) | (conf.node_id() & 0x00FF);
  }

  uint64_t GetCurrentAutoIncrement(int zone = 0) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto ts = auto_increment_cnt[zone].load();
    return (ts << 8) | (conf.node_id() & 0x00FF);
  }

  void ResetAutoIncrement(int zone = 0, uint64_t ts = 0) {
    auto_increment_cnt[zone] = ts;
  }

};

class RelationManagerBase {
 public:
  RelationManagerBase() {}
};

template <class T>
class RelationManagerPolicy : public RelationManagerBase {
 protected:
 public:
  static constexpr int kMaxNrRelations = 1024;
  RelationManagerPolicy() {}

  T &GetRelationOrCreate(int fid) {
#ifndef NDEBUG
    abort_if(fid < 0 || fid >= kMaxNrRelations,
             "Cannot access {}, limit {}", fid, kMaxNrRelations);
#endif

    if (relations[fid].relation_id() == -1)
      relations[fid].set_id(fid);
    return relations[fid];
  }

  T &operator[](int fid) {
#ifndef NDEBUG
    abort_if(fid < 0 || fid >= kMaxNrRelations || relations[fid].relation_id() == -1,
             "Cannot access {}? Is it initialized? limit {}", fid, kMaxNrRelations);
#endif
    return relations[fid];
  }

  template <typename TableT> T &Get() { return (*this)[static_cast<int>(TableT::kTable)]; }

 protected:
  std::array<T, kMaxNrRelations> relations;
};

template <class IndexPolicy>
class RelationPolicy : public BaseRelation,
		       public IndexPolicy {
 public:
  VHandle *SearchOrCreate(const VarStr *k) {
    return this->SearchOrDefault(k, []() { return new VHandle(); });
  }
};

void InitVersion(felis::VHandle *, VarStr *);

}

#endif /* INDEX_COMMON_H */
