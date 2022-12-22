#ifndef ROUTINE_SCHED_H
#define ROUTINE_SCHED_H

#include "piece.h"
#include "util/objects.h"
#include "util/linklist.h"
#include "node_config.h"

namespace felis {

struct PriorityQueueValue : public util::GenericListNode<PriorityQueueValue> {
  PieceRoutine *routine;
  BasePieceCollection::ExecutionRoutine *state;
};

/** state of a waiting ExecutionRoutine (preempted) */
struct WaitState {
  BasePieceCollection::ExecutionRoutine *state;  /*!< go::Routine of the caller of preempt */
  uint64_t sched_key;  /*!< scheduling key of the caller routine of the preempt */
  uint64_t preempt_ts;  /*!< never used */
  uint64_t preempt_key;  /*!< used to order the waiting routines, allows backoff */
  uint64_t preempt_times;  /*!< number of times that this routine is preempted */
};

// for use ordering WaitStates in a heap
// as this performs a > b instead of a < b, this functions as a min-heap
static bool Greater(const WaitState &a, const WaitState &b) {
  if (a.preempt_key > b.preempt_key)
    return true;
  if (a.preempt_key < b.preempt_key)
    return false;
  //edge case, overlap, use original sched key to tie-break
  return a.sched_key > b.sched_key;
}

struct PriorityQueueHashEntry : public util::GenericListNode<PriorityQueueHashEntry> {
  util::GenericListNode<PriorityQueueValue> values;  /*!< list of Pieces with the same priority as in key */
  uint64_t key;  /*!< sched_key of the collection of PieceRoutines */
};

static constexpr size_t kPriorityQueuePoolElementSize =
    std::max(sizeof(PriorityQueueValue), sizeof(PriorityQueueHashEntry));

static_assert(kPriorityQueuePoolElementSize < 64);

// Interface for priority scheduling
class PrioritySchedulingPolicy {
 protected:
  size_t len = 0;
 public:
  virtual ~PrioritySchedulingPolicy() {}

  bool empty() { return len == 0; }

  // Before we try to schedule from this scheduling policy, should we double
  // check the zero queue?
  virtual bool ShouldRetryBeforePick(std::atomic_ulong *zq_start, std::atomic_ulong *zq_end,
                                     std::atomic_uint *pq_start, std::atomic_uint *pq_end) {
    return false;
  }
  // Would you pick this key according to the current situation?
  virtual bool ShouldPickWaiting(const WaitState &ws) = 0;
  // Pick a value without consuming it.
  virtual PriorityQueueValue *Pick() = 0;
  // Consume (delete) the value from the scheduling structure.
  virtual void Consume(PriorityQueueValue *value) = 0;

  virtual void IngestPending(PriorityQueueHashEntry *hent, PriorityQueueValue *value) = 0;

  virtual void Reset() {}
};

// For scheduling transactions during execution
class EpochExecutionDispatchService : public PromiseRoutineDispatchService {
  template <typename T> friend T &util::Instance() noexcept;
  EpochExecutionDispatchService();

  using ExecutionRoutine = BasePieceCollection::ExecutionRoutine;

  struct CompleteCounter {
    ulong completed;
    CompleteCounter() : completed(0) {}
  };
 public:
  static unsigned int Hash(uint64_t key) { return key >> 8; }
  static constexpr int kOutOfOrderWindow = 25;
  static constexpr int keyThreshold = 17000;
  static constexpr uint64_t max_backoff = 40;

  using PriorityQueueHashHeader = util::GenericListNode<PriorityQueueHashEntry>;
 private:
  // This is not a normal priority queue because lots of priorities are
  // duplicates! Therefore, we use a hashtable to deduplicate them.
  struct PriorityQueue {
    PrioritySchedulingPolicy *sched_pol;  /*!< This is where the actual priority queue locates */
    PriorityQueueHashHeader *ht; /*!< Hashtable. First item is a sentinel */
    struct {
      PieceRoutine **q;
      std::atomic_uint start;
      std::atomic_uint end;
    } pending; /*!< Pending inserts into the heap and the hashtable */

    struct {
      // Min-heap
      WaitState states[kOutOfOrderWindow];
      uint32_t unique_preempts; /*!< number of successful preempts, statistics tracking */
      uint32_t len;  /*!< number of waiting ExecutionRoutines */
    } waiting;  /*!< a priority queue of waiting ExecutionRoutines */

    mem::Brk brk; /*!< memory allocator for hashtables entries and queue values */
  };

  /** A none wrapping buffer of PieceRoutine ptrs */
  struct ZeroQueue {
    PieceRoutine **q;
    std::atomic_ulong end;
    std::atomic_ulong start;
  };

  struct State {
    uint64_t current_sched_key;
    uint64_t ts;  /*!< Monotonic incrementing timestamp, but never used. */
    CompleteCounter complete_counter;  /*!< Local counter of completed piece routines. */

    static constexpr int kSleeping = 0;
    static constexpr int kRunning = 1;
    static constexpr int kDeciding = -1;
    std::atomic_int running;

    State() : current_sched_key(0), ts(0), running(kSleeping) {}
  };

  struct Queue {
    PriorityQueue pq;
    ZeroQueue zq;
    util::SpinLock lock;
    State state;
  };
 public:
  static size_t g_max_item;
 private:

  static const size_t kHashTableSize;
  static constexpr size_t kMaxNrThreads = NodeConfiguration::kMaxNrThreads;

  std::array<Queue *, kMaxNrThreads> queues;
  std::atomic_ulong tot_bubbles;

 private:
  /**
   * Adds a PieceRoutine into the hashtable and the actual priority queue inside the sched_pol.
   * @param q
   * @param r
   * @param state
   */
  void AddToPriorityQueue(PriorityQueue &q, PieceRoutine *&r,
                          BasePieceCollection::ExecutionRoutine *state = nullptr);
  /**
   * Process the PieceRoutines that are added to the pending buffer of the PQ but not the hash table.
   * @param q
   */
  void ProcessPending(PriorityQueue &q);

 public:
  /**
   * Add a number of PieceRoutines to the per core scheduler queue.
   * @param core_id
   * @param routines
   * @param nr_routines
   */
  void Add(int core_id, PieceRoutine **routines, size_t nr_routines) final override;
  void AddBubble() final override;
  bool Peek(int core_id, DispatchPeekListener &should_pop) final override;
  /**
   * Pushes a ExecutionRoutine onto the waiting queue of the scheduler if possible
   * @param core_id
   * @param state
   * @param sid
   * @param ver
   * @return
   */
  bool Preempt(int core_id, BasePieceCollection::ExecutionRoutine *state, uint64_t sid, uint64_t ver) final override;
  void Reset() final override;
  void Complete(int core_id) final override;
  int TraceDependency(uint64_t key) final override;
  bool IsRunning(int core_id) final override {
    auto &s = queues[core_id]->state;
    int running = -1;
    while ((running = s.running.load(std::memory_order_seq_cst)) == State::kDeciding)
      _mm_pause();
    return running == State::kRunning;
  }
  bool IsReady(int core_id) final override;
};

}

#endif /* SCHED_H */
