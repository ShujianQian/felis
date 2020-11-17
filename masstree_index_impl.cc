#include "masstree_index_impl.h"

#include "masstree/build/config.h"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/masstree_print.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/kvthread.hh"
#include "masstree/timestamp.hh"
#include "masstree/masstree.hh"
#include "masstree/masstree_struct.hh"

volatile mrcu_epoch_type active_epoch;
volatile mrcu_epoch_type globalepoch = 1;

kvtimestamp_t initial_timestamp;
kvepoch_t global_log_epoch;

namespace felis {

struct MasstreeDollyParam : public Masstree::nodeparams<15, 15> {
  typedef VHandle* value_type;
  // typedef VHandlePrinter value_print_type;
  typedef threadinfo threadinfo_type;
};

class MasstreeMap : public Masstree::basic_table<MasstreeDollyParam> {};
class MasstreeMapForwardScanIteratorImpl : public MasstreeMap::forward_scan_iterator_impl {
 public:
  using MasstreeMap::forward_scan_iterator_impl::forward_scan_iterator_impl;

  static void *operator new(size_t sz) {
    return mem::AllocFromRoutine(sizeof(MasstreeMapForwardScanIteratorImpl));
  }

  static void operator delete(void *p) {
    // noop;
  }
};

MasstreeIndex::Iterator::Iterator(MasstreeMapForwardScanIteratorImpl *scan_it,
                                  const VarStr *terminate_key)
    : end_key(terminate_key), it(scan_it), ti(GetThreadInfo()), vhandle(nullptr)
{
  Adapt();
}

void MasstreeIndex::Iterator::Adapt()
{
  if (!it->terminated) {
    // wrap the iterator
    auto s = it->key.full_string();
    cur_key.len = s.length();
    cur_key.data = (const uint8_t *) s.data();
    vhandle = it->entry.value();
  }
}

void MasstreeIndex::Iterator::Next()
{
  it->next(*ti);
  Adapt();
}

bool MasstreeIndex::Iterator::IsValid() const
{
  if (end_key == nullptr)
    return !it->terminated;
  else
    return !it->terminated && !(*end_key < cur_key);
}

void MasstreeIndex::Initialize(threadinfo *ti)
{
  auto tree = new MasstreeMap();
  tree->initialize(*ti);
  map = tree;
}

VHandle *MasstreeIndex::SearchOrDefaultImpl(const VarStr *k,
                                            const SearchOrDefaultHandler &default_func)
{
  VHandle *result;
  // result = this->Search(k);
  // if (result) return result;
  auto ti = GetThreadInfo();
  typename MasstreeMap::cursor_type cursor(*map, k->data, k->len);
  bool found = cursor.find_insert(*ti);
  if (!found) {
    cursor.value() = default_func();
    // nr_keys[go::Scheduler::CurrentThreadPoolId() - 1].add_cnt++;
  }
  result = cursor.value();
  cursor.finish(1, *ti);
  assert(result != nullptr);
  return result;
}

VHandle *MasstreeIndex::Search(const VarStr *k, uint64_t sid)
{
  auto ti = GetThreadInfo();
  VHandle *result = nullptr;
  map->get(lcdf::Str(k->data, k->len), result, *ti, sid);
  return result;
}

VHandle *MasstreeIndex::PriorityInsert(const VarStr *k, uint64_t sid)
{
  auto ti = GetThreadInfo();
  typename MasstreeMap::cursor_type cursor(*map, k->data, k->len);
  bool found = cursor.find_locked(*ti);
  if (found || cursor.node()->read_sid > sid || cursor.node()->write_sid > sid)
    return nullptr; // mechanism C, D, E, F
  found = cursor.find_insert(*ti);
  assert(!found);
  cursor.value() = new VHandle();
  VHandle *result = cursor.value();
  cursor.finish(1, *ti);
  assert(result != nullptr);
  return result;
}

static __thread threadinfo *TLSThreadInfo;

threadinfo *MasstreeIndex::GetThreadInfo()
{
  if (TLSThreadInfo == nullptr)
    TLSThreadInfo = threadinfo::make(threadinfo::TI_PROCESS, go::Scheduler::CurrentThreadPoolId());
  return TLSThreadInfo;
}

void MasstreeIndex::ResetThreadInfo()
{
  TLSThreadInfo = nullptr;
}

MasstreeIndex::Iterator MasstreeIndex::IndexSearchIterator(const VarStr *start, const VarStr *end)
{
  auto p = map->find_iterator<MasstreeMapForwardScanIteratorImpl>(
      lcdf::Str(start->data, start->len), *GetThreadInfo());
  return Iterator(p, end);
}

void MasstreeIndex::ImmediateDelete(const VarStr *k)
{
  auto ti = GetThreadInfo();
  typename MasstreeMap::cursor_type cursor(*map, k->data, k->len);
  bool found = cursor.find_locked(*ti);
  if (found) {
    VHandle *phandle = cursor.value();
    cursor.value() = nullptr;
    asm volatile ("": : :"memory");
    delete phandle;
  }
  cursor.finish(-1, *ti);
}

RelationManager::RelationManager()
{
  // initialize all relations
  ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  for (int i = 0; i < kMaxNrRelations; i++) {
    relations[i].Initialize(ti);
  }
}

}
