#ifndef MASSTREE_INDEX_IMPL_H
#define MASSTREE_INDEX_IMPL_H

#include <cstdio>
#include <atomic>

#include "index_common.h"
#include "util.h"
#include "log.h"
#include "vhandle.h"

class threadinfo;

namespace felis {

class RelationManager;
class MasstreeMap;
class MasstreeMapForwardScanIteratorImpl;

class MasstreeIndex {
 protected:
  MasstreeMap *map;
  static threadinfo *GetThreadInfo();
 public:
  static void ResetThreadInfo();

  struct Iterator {
    const VarStr *end_key;
    MasstreeMapForwardScanIteratorImpl *it;
    threadinfo *ti;
    VarStr cur_key;
    VHandle *vhandle;

    Iterator(MasstreeMapForwardScanIteratorImpl *scan_it,
             const VarStr *terminate_key);

    void Next() __attribute__((noinline));
    bool IsValid() const;

    const VarStr &key() const { return cur_key; }
    const VHandle *row() const { return vhandle; }
    VHandle *row() { return vhandle; }

   private:
    void Adapt();
  };
  void Initialize(threadinfo *ti);
 protected:

  struct {
    uint64_t add_cnt;
    uint64_t del_cnt;
  } nr_keys [NodeConfiguration::kMaxNrThreads]; // scalable counting

 public:

  struct SearchOrDefaultHandler {
    virtual VHandle *operator()() const = 0;
  };

  VHandle *SearchOrDefaultImpl(const VarStr *k, const SearchOrDefaultHandler &default_handler);

  template <typename Func>
  VHandle *SearchOrDefault(const VarStr *k, Func default_func) {
    struct SearchOrDefaultHandlerImpl : public SearchOrDefaultHandler {
      Func f;
      SearchOrDefaultHandlerImpl(Func f) : f(f) {}
      VHandle *operator()() const override final {
        return f();
      }
    };
    return SearchOrDefaultImpl(k, SearchOrDefaultHandlerImpl(default_func));
  }

  VHandle *Search(const VarStr *k, uint64_t sid = 0);
  VHandle *PriorityInsert(const VarStr *k, uint64_t sid);

  Iterator IndexSearchIterator(const VarStr *start, const VarStr *end = nullptr) __attribute__((noinline));

  size_t nr_unique_keys() const {
    size_t rs = 0;
    for (int i = 0; i < NodeConfiguration::g_nr_threads; i++) {
      rs += nr_keys[i].add_cnt - nr_keys[i].del_cnt;
    }
    return rs;
  }
  void ImmediateDelete(const VarStr *k);
  void FakeDelete(const VarStr *k) {
    // delete an object, this won't be checkpointed
    nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].del_cnt++;
  }
};

class Relation : public felis::RelationPolicy<MasstreeIndex> {};

class RelationManager : public RelationManagerPolicy<Relation> {
  threadinfo *ti;

  RelationManager();
  template <typename T> friend T &util::Instance() noexcept;
 public:
  threadinfo *GetThreadInfo() { return ti; }
};

}

#endif /* MASSTREE_INDEX_IMPL_H */
