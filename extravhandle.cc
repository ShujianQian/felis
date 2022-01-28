//
// Created by Shujian Qian on 2022-01-28.
//

#include "extravhandle.h"

#include "priority.h"

namespace felis
{

DoublyLinkedListExtraVHandle::DoublyLinkedListExtraVHandle()
        : head(nullptr), tail(nullptr), size(0)
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
    return false;
}
VarStr *DoublyLinkedListExtraVHandle::ReadWithVersion(uint64_t sid,
                                                      uint64_t ver,
                                                      SortedArrayVHandle *handle)
{
    return nullptr;
}
bool DoublyLinkedListExtraVHandle::CheckReadBit(uint64_t sid,
                                                uint64_t ver,
                                                SortedArrayVHandle *handle,
                                                bool &is_in)
{
    return false;
}
bool DoublyLinkedListExtraVHandle::IsExistingVersion(uint64_t min)
{
    return false;
}
uint64_t DoublyLinkedListExtraVHandle::FindUnreadVersionLowerBound(uint64_t min)
{
    return 0;
}
uint64_t DoublyLinkedListExtraVHandle::FindFirstUnreadVersion(uint64_t min)
{
    return 0;
}
bool DoublyLinkedListExtraVHandle::WriteWithVersion(uint64_t sid, VarStr *obj)
{
    return false;
}
void DoublyLinkedListExtraVHandle::GarbageCollect()
{

}
uint64_t DoublyLinkedListExtraVHandle::first_version()
{
    return 0;
}
uint64_t DoublyLinkedListExtraVHandle::last_version()
{
    return 0;
}
bool DoublyLinkedListExtraVHandle::WriteLastBatch(uint64_t sid, VarStr *obj)
{
    return false;
}

} // namespace felis
