//
// Created by Shujian Qian on 2022-01-28.
//

#ifndef FELIS__EXTRAVHANDLE_H_
#define FELIS__EXTRAVHANDLE_H_

#include <cstdint>
#include "mem.h"
#include "varstr.h"

namespace felis
{

class SortedArrayVHandle;
class DoublyLinkedListExtraVHandle
{
  public:
    struct Entry
    {
        Entry *next;
        Entry *prev;
        uint64_t version;
        uintptr_t object;
        int32_t this_coreid;
        static mem::ParallelSlabPool pool;

        static void InitPool()
        {
            pool = mem::ParallelSlabPool(mem::EntryPool, sizeof(Entry), 4);
            pool.Register();
        }

        static void Quiescence()
        { pool.Quiescence(); }

        Entry(uint64_t _version, uintptr_t _object, int _coreid) :
                next(nullptr),
                prev(nullptr),
                version(_version),
                object(_object),
                this_coreid(_coreid)
        {}

        static void *operator new(size_t nr_bytes)
        {
            return pool.Alloc();
        }

        static void operator delete(void *ptr)
        {
            Entry *phandle = (Entry *) ptr;
            pool.Free(ptr, phandle->this_coreid);
        }

    };
    static_assert(sizeof(Entry) <= 64,
                  "LinkedListExtraVHandle Entry is too large");

  private:
    uint16_t alloc_by_regionid;
    uint16_t this_coreid;
    uint32_t size;
    std::atomic<Entry *> head;
    std::atomic<Entry *> tail;
    uint64_t last_batch_version;
    uintptr_t last_batch_obj;

  public:
    static mem::ParallelSlabPool pool;

    static void InitPool()
    {
        pool = mem::ParallelSlabPool(mem::ExtraVhandlePool,
                                     sizeof(DoublyLinkedListExtraVHandle),
                                     4);
        pool.Register();
    }
    static void *operator new(size_t nr_bytes)
    {
        return pool.Alloc();
    }

    static void operator delete(void *ptr)
    {
        auto phandle = (DoublyLinkedListExtraVHandle *) ptr;
        pool.Free(ptr, phandle->this_coreid);
    }

    DoublyLinkedListExtraVHandle();
    DoublyLinkedListExtraVHandle(DoublyLinkedListExtraVHandle &&rhs) = delete;

    util::MCSSpinLock lock;
    static bool RequiresLock();
    bool AppendNewPriorityVersion(uint64_t sid);
    VarStr *ReadWithVersion(uint64_t sid,
                            uint64_t ver,
                            SortedArrayVHandle *handle);
    bool CheckReadBit(uint64_t sid, uint64_t ver, SortedArrayVHandle *handle, bool &is_in);
    bool IsExistingVersion(uint64_t min);
    uint64_t FindUnreadVersionLowerBound(uint64_t min);
    uint64_t FindFirstUnreadVersion(uint64_t min);
    bool WriteWithVersion(uint64_t sid, VarStr *obj);
    void GarbageCollect();
    uint64_t first_version();
    uint64_t last_version();
    bool WriteLastBatch(uint64_t sid, VarStr *obj);
};

} // namespace felis

#endif //FELIS__EXTRAVHANDLE_H_
