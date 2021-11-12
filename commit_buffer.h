#include <atomic>
#include "varstr.h"
#include "mem.h"

namespace felis {

// Commit buffer to deal with repeat updates within one transaction. Usually
// this is a per-transaction hashtable, however, in our case, we don't have a
// notion of commit, and we have to preserve this hashtable across multiple
// phases.
//
// So, our commit buffer is a per-epoch hashtable, it will be reset (in
// parallel) at the epoch boundary.

class VHandle;

/*!
 * \brief Commit buffer to deal with repeated updates within one transaction.
 */
class CommitBuffer {
public:
    /*!
     * \brief Commit buffer entry.
     */
    struct Entry {
        VHandle *vhandle;
        uint32_t short_sid;         ///< serialID inside the epoch.
        std::atomic_int32_t wcnt;
        union {
            std::atomic<Entry *> dup = nullptr;
            VarStr *value;
        } u;
        std::atomic<Entry *> next = nullptr;

        /*!
         * \brief Default constructor.
         * @param vhandle
         * @param sid
         */
        Entry(VHandle *vhandle, uint32_t sid) : vhandle(vhandle), short_sid(sid), wcnt(1)
        {}
    };

private:
    std::atomic<Entry *> *ref_hashtable;
    unsigned long ref_hashtable_size;
    std::atomic<Entry *> *dup_hashtable;
    unsigned long dup_hashtable_size;

    std::atomic_uint64_t clear_refcnt; // 0 means all clear

    std::array<mem::Brk *, mem::ParallelAllocationPolicy::kMaxNrPools> entbrks;

    void EnsureReady();

public:
    CommitBuffer();

    void Reset();

    void Clear(int core_id);

    bool AddRef(int core_id, VHandle *vhandle, uint64_t sid);

    Entry *LookupDuplicate(VHandle *vhandle, uint64_t sid);
};

using WriteSetDesc = CommitBuffer::Entry;

}
