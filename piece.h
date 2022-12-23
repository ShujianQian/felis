#ifndef PIECE_H
#define PIECE_H

#include <tuple>
#include <atomic>

#include "gopp/gopp.h"
#include "varstr.h"

namespace felis {

class BasePieceCollection;
class PieceRoutine;

// Performance: It seems critical to keep this struct one cache line!

/**
 * Basic runnable piece. A piece consists of a static function that takes
 */
struct PieceRoutine {
  uint8_t *capture_data;
  uint32_t capture_len;
  /**
   * The level of child routine that this routine is. Not used.
   */
  uint8_t level;
  /**
   * The destination node that this routine should run on. 0 means same as source node.
   */
  uint8_t node_id;
  uint64_t sched_key; // Optional. 0 for unset. For scheduling only.
  uint64_t affinity; // Which core to run on. -1 means not specified. >= nr_threads means random.

  void (*callback)(PieceRoutine *);

  size_t NodeSize() const;
  uint8_t *EncodeNode(uint8_t *p);

  size_t DecodeNode(uint8_t *p, size_t len);

  /**
   * Shujian: I believe this is never used.
   */
  BasePieceCollection *next;

  /**
   * Shujian: I believe this is never used.
   */
  uint8_t fv_signals;
  uint8_t future_source_node_id;
  uint8_t __padding__[14];

  static PieceRoutine *CreateFromCapture(size_t capture_len);
  static PieceRoutine *CreateFromPacket(uint8_t *p, size_t packet_len);
  PieceRoutine() = delete;

  static constexpr size_t kUpdateBatchCounter = std::numeric_limits<uint64_t>::max() - (1ULL << 56);
};

static_assert(sizeof(PieceRoutine) == CACHE_LINE_SIZE);

class EpochClient;

class BasePieceCollection {
 public:
  static constexpr size_t kInlineLimit = 6;
 protected:
  friend struct PieceRoutine;
  int nr_handlers;
  int limit;
  PieceRoutine **extra_handlers;

  PieceRoutine *inline_handlers[kInlineLimit];
 public:
  static size_t g_nr_threads;
  static constexpr int kMaxHandlersLimit = 32 + kInlineLimit;

  class ExecutionRoutine : public go::Routine {
   public:
    ExecutionRoutine() {
      set_reuse(true);
    }
    static void *operator new(std::size_t size) {
      return BasePieceCollection::Alloc(util::Align(size, CACHE_LINE_SIZE));
    }
    static void operator delete(void *ptr) {}
    void Run() final override;
    void AddToReadyQueue(go::Scheduler::Queue *q, bool next_ready = false) final override;

    bool Preempt(uint64_t sid, uint64_t ver);
  };

  static_assert(sizeof(ExecutionRoutine) <= CACHE_LINE_SIZE);

  BasePieceCollection(int limit = kMaxHandlersLimit);
  /**
   * Transport all routines in this PieceCollection.
   */
  void Complete();
  void Add(PieceRoutine *child);
  void AssignSchedulingKey(uint64_t key);
  /**
   * Sets the affinity (which core should this txn run on) to all children PieceRoutines in this PieceCollection.
   * @param affinity
   */
  void AssignAffinity(uint64_t affinity);

  static void *operator new(std::size_t size);
  static void *Alloc(size_t size);
  /**
   * Adds a number of PieceRoutines to the per core scheduler queue through EpochExecutionDispatchService::Add.
   * @param routines
   * @param nr_routines
   * @param core_id
   */
  static void QueueRoutine(PieceRoutine **routines, size_t nr_routines, int core_id);
  /**
   * For each core on the node, if there's no ExecutionRoutine running to execute the PieceRoutines in that core's
   * scheduler queues, create one on that core.
   */
  static void FlushScheduler();

  size_t nr_routines() const { return nr_handlers; }
  PieceRoutine *&routine(int idx) {
    if (idx < kInlineLimit)
      return inline_handlers[idx];
    else
      return extra_handlers[idx - kInlineLimit];
  }
  PieceRoutine *&last() {
    return routine(nr_routines() - 1);
  }
};

static_assert(sizeof(BasePieceCollection) % CACHE_LINE_SIZE == 0, "BasePromise is not cache line aligned");

class BaseFutureValue;

class PromiseRoutineTransportService {
 public:
  /**
   * Max depth of child routines that a PieceRoutine can have.
   */
  static constexpr size_t kPromiseMaxLevels = 16;

  virtual void TransportPromiseRoutine(PieceRoutine *routine) = 0;
  virtual void TransportFutureValue(BaseFutureValue *val) {}
  virtual void TransportDistributedFutureValue(BaseFutureValue *val, int origin_node) {}
  /**
   * Performs sending and receiving of PieceRoutines and FutureValues
   * @param core
   * @return Should continue IO?
   */
  virtual bool PeriodicIO(int core) { return false; }
  virtual void PrefetchInbound() {};
  virtual void FinishCompletion(int level) {}
  virtual uint8_t GetNumberOfNodes() { return 0; }
};

class PromiseRoutineDispatchService {
 public:

  class DispatchPeekListener {
   public:
    virtual bool operator()(PieceRoutine *, BasePieceCollection::ExecutionRoutine *) = 0;
  };

  template <typename F>
  class GenericDispatchPeekListener : public DispatchPeekListener {
    F func;
   public:
    GenericDispatchPeekListener(F f) : func(f) {}
    bool operator()(PieceRoutine *r, BasePieceCollection::ExecutionRoutine *state) final override {
      return func(r, state);
    }
  };

  virtual void Add(int core_id, PieceRoutine **r, size_t nr_routines) = 0;
  virtual void AddBubble() = 0;
  virtual bool Preempt(int core_id, BasePieceCollection::ExecutionRoutine *state, uint64_t sid, uint64_t ver) = 0;
  virtual bool Peek(int core_id, DispatchPeekListener &should_pop) = 0;
  virtual void Reset() = 0;
  virtual void Complete(int core_id) = 0;
  virtual bool IsRunning(int core_id) = 0;
  virtual bool IsReady(int core_id) = 0;

  // For debugging
  virtual int TraceDependency(uint64_t) { return -1; }
};

class PromiseAllocationService {
 public:
  virtual void *Alloc(size_t size) = 0;
  virtual void Reset() = 0;
};

}

#endif
