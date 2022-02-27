/*!
 * \file extravhandle.cc
 *
 * Created by Shujian Qian on 2022-01-28.
 */

#include "extravhandle.h"

#include <forward_list>

#include "priority.h"

namespace felis
{

mem::ParallelSlabPool DoublyLinkedListExtraVHandle::pool;
mem::ParallelSlabPool DoublyLinkedListExtraVHandle::Entry::pool;

DoublyLinkedListExtraVHandle::DoublyLinkedListExtraVHandle()
        : head(nullptr),
          tail(nullptr),
          size(0),
          last_batch_obj(kIgnoreValue),
          last_batch_version(0)
{
    this_coreid = alloc_by_regionid = mem::ParallelPool::CurrentAffinity();
}

bool DoublyLinkedListExtraVHandle::RequiresLock()
{
    return PriorityTxnService::g_lock_insert
            || PriorityTxnService::g_hybrid_insert;
}

bool DoublyLinkedListExtraVHandle::AppendNewPriorityVersion(uint64_t sid)
{
    auto n = new Entry(sid,
                       kPendingValue,
                       mem::ParallelSlabPool::CurrentAffinity());

    if (PriorityTxnService::g_lock_insert) {
        // lock based insert
        util::MCSSpinLock::QNode q_node;
        lock.Lock(&q_node);

        Entry after_tail(LONG_MAX, kPendingValue, 0);
        after_tail.prev = tail;

        Entry *cur = &after_tail;
        while (cur && cur->prev && cur->prev->version > sid) {
            cur = cur->prev;
        }

        // TODO: Shujian: Understand what does it mean to have TicToc and why
        //  it reuses SID.

        // SID may be reused for TicToc, therefore, abort if seeing repeated
        // SID.
        if (cur->prev && cur->prev->version == sid) {
            delete n;
            lock.Unlock(&q_node);
            return false;
        }

        n->prev = cur->prev;
        n->next = (cur == &after_tail) ? nullptr : cur;
        cur->prev = n;
        if (n->prev) {
            n->prev->next = n;
        } else {
            head = n;
        }

        tail = after_tail.prev;

        lock.Unlock(&q_node);
        return true;
    } else if (PriorityTxnService::g_hybrid_insert) {
        // hybrid insert
        // Firstly, try to insert is atomically. If the atomic insert will
        // result in the list being out of order, then insert while holding a
        // lock.
        Entry *old_tail;
        do {
            old_tail = tail.load();
            if (old_tail && old_tail->version >= sid) {
                // if requires insert, do it with a lock
                util::MCSSpinLock::QNode q_node;
                lock.Lock(&q_node);

                Entry *cur = old_tail;

                while (cur && cur->prev && cur->prev->version > sid) {
                    cur = cur->prev;
                }

                // SID may be reused for TicToc, therefore, abort if seeing repeated
                // SID.
                if (cur->prev && cur->prev->version == sid) {
                    delete n;
                    lock.Unlock(&q_node);
                    return false;
                }

                n->prev = cur->prev;
                n->next = cur;
                cur->prev = n;
                if (n->prev) {
                    n->prev->next = n;
                } else {
                    head = n;
                }

                size++;
                lock.Unlock(&q_node);
                return true;
            }
            n->next = nullptr;
            n->prev = old_tail;
        } while (!tail.compare_exchange_strong(old_tail, n));

        old_tail->next = n;

        // TODO: Shujian: understand if this size is used, if so why is it not
        //  atomic
        size++;
        return true;
    } else {
        Entry *old_tail;
        do {
            old_tail = tail.load();
            if (old_tail && old_tail->version >= sid) {
                delete n;
                return false;
            }
            n->next = nullptr;
            n->prev = old_tail;
        } while (!tail.compare_exchange_strong(old_tail, n));

        if (old_tail) {
            // atomic write because GC could be loading this
            old_tail->next = n;
        } else {
            head = n;
        }

        // TODO: Shujian: understand if this size is used, if so why is it not
        //  atomic
        size++;
        return true;
    }
}

VarStr *DoublyLinkedListExtraVHandle::ReadWithVersion(uint64_t sid,
                                                      uint64_t ver,
                                                      SortedArrayVHandle
                                                      *handle)
{
    util::MCSSpinLock::QNode q_node;
    if (RequiresLock()) {
        lock.Lock(&q_node);
    }

    Entry *p = tail;

    // find the version that is closer than the version array version
    while (p && (p->version > ver) &&
            ((p->version >= sid) ||
                    (p->version < sid
                            && VHandleSyncService::IsIgnoreVal(p->object)))) {
        p = p->prev;
    }

    if (RequiresLock()) {
        lock.Unlock(&q_node);
    }

    if (!p) {
        return nullptr;
    }

    abort_if(p->version >= sid, "p->version >= sid, {} >= {}", p->version, sid);

    // TODO: Shujian: this is probably unnecessary
    auto extra_ver = p->version;
    if (extra_ver < ver) {
        return nullptr;
    }

    // pointer to the object pointer in the version
    volatile uintptr_t *obj_ptr_ptr = &(p->object);

    // mark read bit atomically using CAS
    if (PriorityTxnService::g_read_bit) {
        uintptr_t old_obj_ptr = *obj_ptr_ptr;
        uintptr_t new_obj_ptr = old_obj_ptr | kReadBitMask;
        while (!(old_obj_ptr & kReadBitMask)) {
            uintptr_t orig_obj_ptr = __sync_val_compare_and_swap(obj_ptr_ptr,
                                                                 old_obj_ptr,
                                                                 new_obj_ptr);
            if (orig_obj_ptr == old_obj_ptr) {
                // marking succeeded
                break;
            }
            old_obj_ptr = orig_obj_ptr;
            new_obj_ptr = old_obj_ptr | kReadBitMask;
        }
    }

    // wait for the object to be filled
    util::Impl<VHandleSyncService>().WaitForData(obj_ptr_ptr,
                                                 sid,
                                                 extra_ver,
                                                 (void *) this);

    // TODO: Shujian: This might be unnecessary.
    auto varstr_ptr = *obj_ptr_ptr & ~kReadBitMask;

    // TODO: Shujian: Potential optimization: Instead of recursively calling
    //  VHandle::ReadWithVersion, can we just redo
    //  extraVHandle::ReadWithVersion?
    // if the value turns out to be kIgnoreValue, recursively call
    // ReadWithVersion from VHandle with the closest version.
    if (VHandleSyncService::IsIgnoreVal(varstr_ptr))
        return handle->ReadWithVersion(extra_ver);

    return (VarStr *) varstr_ptr;
}

bool DoublyLinkedListExtraVHandle::CheckReadBit(uint64_t sid,
                                                uint64_t ver,
                                                SortedArrayVHandle *handle,
                                                bool &is_in)
{
    abort_if(!PriorityTxnService::g_read_bit, "ExtraVHandle CheckReadBit() is"
                                              " called when read bit is off");

    is_in = false;

    util::MCSSpinLock::QNode q_node;
    if (RequiresLock()) {
        lock.Lock(&q_node);
    }

    Entry *p = tail;
    while (p && ((p->version >= sid) || (p->version < sid
            && VHandleSyncService::IsIgnoreVal(p->object)))) {
        p = p->prev;
    }

    if (RequiresLock()) {
        lock.Unlock(&q_node);
    }

    if (!p)
        return false;

    auto extra_ver = p->version;
    if (extra_ver < ver) {
        return false;
    }

    is_in = true;
    auto obj_ptr = p->object;
    if (obj_ptr == kPendingValue) {
        // obj_ptr is not written to yet, so it cannot be read from before
        return false;
    }

    // TODO: Shujian: I'm not sure what this does.
    if (PriorityTxnService::g_last_version_patch && p == tail
            && (obj_ptr & kReadBitMask)) {
        uint64_t rts = ((uint64_t) (handle->GetRowRTS()) << 8) + 1;
        auto wts = sid;
        return wts <= rts;
    }

    return obj_ptr & kReadBitMask;
}

bool DoublyLinkedListExtraVHandle::IsExistingVersion(uint64_t sid)
{
    util::MCSSpinLock::QNode q_node;
    if (RequiresLock()) {
        lock.Lock(&q_node);
    }

    Entry *p = tail;
    while (p && p->version > sid) {
        p = p->prev;
    }

    if (RequiresLock()) {
        lock.Lock(&q_node);
    }

    if (!p || p->version > sid) {
        return false;
    }

    return true;
}

uint64_t DoublyLinkedListExtraVHandle::FindUnreadVersionLowerBound(uint64_t min)
{
    // TODO: Shujian: How come this doesn't need a lock?

    Entry after_tail(LONG_MAX, kPendingValue, 0);
    after_tail.prev = tail;

    Entry *cur = &after_tail;
    while (cur->prev && !(cur->prev->object & kReadBitMask)) {
        cur = cur->prev;
        if (cur->version <= min) {
            return min;
        }
    }

    if (cur == &after_tail) {
        return 0;
    }

    return cur->version;
}

uint64_t DoublyLinkedListExtraVHandle::FindFirstUnreadVersion(uint64_t min)
{
    util::MCSSpinLock::QNode q_node;
    if (RequiresLock()) {
        lock.Lock(&q_node);
    }

    Entry after_tail(LONG_MAX, kPendingValue, 0);
    after_tail.prev = tail;

    Entry *cur = &after_tail;
    uint64_t first_unread = UINT64_MAX;
    while (cur->prev && cur->prev->version >= min) {
        cur = cur->prev;
        if (!(cur->object & kReadBitMask)) {
            abort_if(first_unread < cur->version, "extraVHandle appears to be "
                                                  "out of order");
            first_unread = cur->version;
        }
    }

    if (cur->prev && cur->prev->version < min
            && !(cur->prev->object & kReadBitMask)) {
        if (RequiresLock()) {
            lock.Unlock(&q_node);
        }
        return min;
    }

    if (RequiresLock()) {
        lock.Unlock(&q_node);
    }

    if (first_unread == UINT64_MAX) {
        // no unread version found
        return 0;
    }

    int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
    uint64_t core_progress =
            util::Instance<PriorityTxnService>().GetProgress(core_id);
    if (first_unread >= core_progress) {
        // first unread version if after current core progress
        return 0;
    }

    return first_unread;
}

bool DoublyLinkedListExtraVHandle::WriteWithVersion(uint64_t sid, VarStr *obj)
{
    util::MCSSpinLock::QNode q_node;
    if (RequiresLock()) {
        lock.Lock(&q_node);
    }

    Entry *p = tail;
    while (p && p->version > sid) {
        p = p->prev;
    }

    if (!p || p->version != sid) {
        // writing to a non-existing sid
        logger->critical("Divergin outcomes! sid {}", sid);
        std::stringstream ss;
        Entry *cur = tail;
        while (cur) {
            ss << "{" << std::dec << p->version << "(hex" << std::hex
               << p->version << "), 0x" << std::hex << p->object << "}->";
            cur = cur->prev;
        }
        logger->critical("Extra Doubly Linked List (backwards): {}nullptr",
                         ss.str());
        if (RequiresLock()) {
            lock.Unlock(&q_node);
        }
        return false;
    }

    if (RequiresLock()) {
        lock.Unlock(&q_node);
    }

    volatile uintptr_t *obj_ptr_ptr = &p->object;
    util::Impl<VHandleSyncService>().OfferData(obj_ptr_ptr, (uintptr_t) obj);
    return true;
}

void DoublyLinkedListExtraVHandle::GarbageCollect()
{
    // TODO: Shujian: Currently not guarded by locks because GC and EPPTs run
    //  in different phases. Later on, will need to add locking in order to
    //  allow concurrent access between GC & IPPTs.

    // TODO: Shujian: Ideas
    //  1. use 2 vectors to keep the versions to be deleted
    //  2. access once from the tail to find the last version to keep and
    //  then deletes stuff from the head.
    //  The current implementation uses the first option.

    // GC starts from the head unlike PT accesses
    Entry *cur = head;
    std::forward_list<Entry *> ignores_to_collect;
    Entry *new_head = nullptr;
    uint32_t num_to_collect = 0; // new_head is nullptr in the first
    // iteration so num_to_collect = number of
    // ignores encountered
    while (cur) {
        if (!VHandleSyncService::IsIgnoreVal(cur->object)) {
            // update head first because it's used to determine the first
            // version in the extraVHandle linked list
            // this will ensure that the head pointer is always valid
            head = cur;

            // collects previous non-ignore value and ignores encountered
            delete new_head;
            new_head = cur;

            for (auto ignore_entry : ignores_to_collect) {
                delete ignore_entry;
            }

            // TODO: Shujian: should this size operation be atomic?
            size -= num_to_collect;

            ignores_to_collect.clear();
            num_to_collect = 1; // including the new_head that is collected in
            // the next iteration
            cur = cur->next;
        } else {
            // keep trying to find the first version to keep
            ignores_to_collect.push_front(cur);
            num_to_collect++;
            abort_if(cur == cur->next, "Fuck: loop in linked list");
            cur = cur->next;
        }
    }

    if (new_head) {
        // new head found, need to update head and new_head's prev
        head = new_head;
        new_head->prev = nullptr;
    } else {
        // no non-ignore version found, collect everything & update head and
        // tail
        for (auto ignore_entry : ignores_to_collect) {
            delete ignore_entry;
        }
        head = nullptr;
        tail = nullptr;

        // TODO: Shujian: should this size operation be atomic?
        this->size = 0;
    }
}

void DoublyLinkedListExtraVHandle::WriteLastBatch(uint64_t sid, VarStr *obj)
{
    volatile uintptr_t *obj_ptr_ptr = &this->last_batch_obj;
    util::Impl<VHandleSyncService>().OfferData(obj_ptr_ptr, (uintptr_t) obj);
    this->last_batch_version = sid;
}

} // namespace felis
