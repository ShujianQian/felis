/*!
 * \file extravhandle.h
 *
 * Created by Shujian Qian on 2022-01-28.
 */

#ifndef FELIS__EXTRAVHANDLE_H_
#define FELIS__EXTRAVHANDLE_H_

#include <cstdint>
#include "mem.h"
#include "varstr.h"

namespace felis
{

class SortedArrayVHandle;

/*!
 * \brief An implementation of the extraVHandle using doubly linked lists.
 *
 * This implementation of extraVHandle uses a doubly linked list data
 * structure in order to allow concurrent access by IPPTs and GC.
 *
 * Although the list of Entry's are organized as a doubly linked list, GC and
 * transactions will only traverse the list in one direction. In other words,
 * the list appears as a singly linked list to both the GC and the transactions.
 */
class DoublyLinkedListExtraVHandle
{
  public:
    /*!
     * \brief Entry node in the extraVHandle doubly linked list.
     */
    struct Entry
    {
        std::atomic<Entry *> next;
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
    std::atomic<uint64_t> max_exec_sid;

  public:
    static mem::ParallelSlabPool pool;
    // TODO: Ray
    //  Change this back to private and create API for accessing this obj.
    uintptr_t last_batch_obj;

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

    /*!
     * \brief Determine if this implementation of extraVHandle requires a lock.
     *
     * \return Whether this implementation of extraVHandle requires lock.
     */
    static bool RequiresLock();

    /*!
     * \brief Append an extraVHandle::Entry to the end of the list.
     *
     * If g_lock_insert is set, the lock is acquired before appending the
     * version. The extraVHandle::Entry object will be inserted in order. In
     * the case where the SID is reused due to TicToc, abort the append.
     * Otherwise, the append will always succeed.
     *
     * If g_hybrid_insert is set, the extraVHandle will first try to append
     * the extraVHandle::Entry using CAS. If the tail has a greater SID than
     * the Entry to be appended, it will try to acquire a lock and try to
     * insert the Entry in order. In the case where the SID is reused due to
     * TicToc, abort the append. Otherwise, the append will always succeed.
     *
     * Otherwise, the extraVHandle::Entry object will only be appended in a
     * lockless manner using CAS. In the case where appending will result in
     * the list to becomes out of order, the append is aborted.
     *
     * Pre-conditions:
     *     - The list of Entry objects is in order.
     *
     * Post-conditions:
     *     - The list of Entry objects is in order.
     *
     * \param sid The serial ID of the extraVHandle::Entry to be appended.
     * \return Whether the append succeeded or not.
     */
    bool AppendNewPriorityVersion(uint64_t sid);

    /*!
     * \brief Read the version from extraVHandle if it is closer than that
     * from the version array.
     *
     * If this implementation of extraVHandle requires a lock, the read will
     * be performed under synchronization of a lock. Otherwise the read will
     * be performed using atomic operations.
     *
     * If the version read is pending and later become ignore value, then
     * recursively call ReadWithVersion on the corresponding VHandle to get
     * the next closest version.
     *
     * In addition, this version atomically marks the read bit if g_read_bit
     * option is set.
     *
     * \param sid SID of the reading transaction.
     * \param ver The closest batch transaction version.
     * \param handle Pointer to this row's VHandle.
     * \return Pointer to version data if extraVHandle version is closer than
     * the VHandle version. Otherwise, nullptr.
     */
    VarStr *ReadWithVersion(uint64_t sid,
                            uint64_t ver,
                            SortedArrayVHandle *handle);

    /*!
     * \brief Checks whether the read bit is set for the version read by
     * transaction with version sid.
     *
     * This function is only used when g_read_bit option is set.
     *
     * \param sid SID of the reading transaction.
     * \param ver The closest batch transaction version.
     * \param handle Pointer to this row's VHandle.
     * \param is_in Set to true if closest version to SID is in extraVHandle.
     * \return True if the read bit is set.
     */
    bool CheckReadBit(uint64_t sid,
                      uint64_t ver,
                      SortedArrayVHandle *handle,
                      bool &is_in);

    /*!
     * \brief Checks whether a version SID exists in extraVHandle.
     *
     * \param sid The SID of the version we are looking for.
     * \return True if the version exists in the extraVHandle.
     */
    bool IsExistingVersion(uint64_t sid);

    /*!
     * \brief Find the lower bound of the last consecutive unread versions.
     *
     * Example:
     *     6r <- 11 <- 14r <- 16 <- 18
     *     return: 16
     *
     * \param min Search lower bound.
     * \return The SID of the version that is the lower bound of the last
     * consecutive unread version. If answer found is smaller than min, return
     * min. If all versions are read, return 0.
     */
    uint64_t FindUnreadVersionLowerBound(uint64_t min);

    /*!
     * \brief Find the first unread version in the extraVHandle.
     *
     * Look for the first unread version in the range of [min, core_progress).
     *
     * Pre-condition:
     *     - The list of version is in order.
     *
     * \param min Search lower bound.
     * \return The SID of the first unread version. If answer found is
     * smaller than min, return min. If all if no unread version can be
     * found in the range [min, core_progress), return 0.
     */
    uint64_t FindFirstUnreadVersion(uint64_t min);

    /*!
     * \brief Write the object to the entry with version SID.
     *
     * \param sid The SID of the writing transaction.
     * \param obj Pointer to the object to be written.
     * \return True if write succeeds.
     */
    bool WriteWithVersion(uint64_t sid, VarStr *obj);

    /*!
     * \brief Collect unused versions in the extraVHandle linked list.
     *
     * Collect all but one version. The largest version with an actual value
     * (i.e. not kIgnoreValue) is kept.
     */
    void GarbageCollect();

    /*!
     * \brief Get the first version in the extraVHandle linked list.
     *
     * \return The SID of the first version in the extraVHandle linked list.
     * If the linked list is currently empty, return UINT64_MAX.
     */
    inline uint64_t first_version()
    {
        Entry *p = head;
        if (p) {
            return p->version;
        }
        return UINT64_MAX;
    }

    /*!
     * \brief Get the last version in the extraVHandle linked list.
     *
     * \return The SID of the last version in the extraVHandle linked list.
     * If the linked list is currently empty, return 0;
     */
    inline uint64_t last_version()
    {
        Entry *p = tail;
        if (p) {
            return p->version;
        }
        return 0;
    }

    /*!
     * \brief Writes the last batch version in the extraVHandle linked list.
     *
     * Pre-condition:
     *     - The obj is never kIgnoreValue and should point to an actual object.
     *
     * \param sid The SID of the writing batch transaction.
     * \param obj The pointer to the object of the version.
     */
    void WriteLastBatch(uint64_t sid, VarStr *obj);
};

} // namespace felis

#endif //FELIS__EXTRAVHANDLE_H_
