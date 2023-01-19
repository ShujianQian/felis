//
// Created by Shujian Qian on 2022-12-03.
//

#ifndef FELIS__CORO_SCHED_H
#define FELIS__CORO_SCHED_H

#include <list>

#include "routine_sched.h"
#include "util/objects.h"
#include "piece.h"
#include "coroutine.h"

namespace felis {

class CoroSched;

extern __thread CoroSched *coro_sched;  /*!< used to conveniently access this core's scheduler */

class CoroSched {
public:
  struct CoroStack {
    struct coroutine coroutine;  /*!< coroutine struct containing the function pointer and the saved context */
    struct coro_shared_stack stack;  /*!< keeps the info about the size of the stack and pointers to the stack */
    uint64_t core_id;  /*!< where this CoroStack belongs to, used to add to the core's ready queue */
    uint64_t sched_key;  /*!< the scheduling key of the waiting coroutine */
    uint64_t preempt_key;  /*!< the sched key with backoff of the waiting coroutine */
    uint64_t preempt_times;  /*!< number of preempt called, used to calculated linear backoff */
    PieceRoutine *running_piece;
    static bool MinHeapCompare(CoroStack *a,  CoroStack *b);  /*!< compare function to build heap */
  };

private:
  /** concurrent linked list to store detached coroutines that turned ready */
  struct ReadyQueue {
    struct ListNode {
      CoroStack *coro;  /*!< pointer to the detached coroutine itself */
      ListNode *next;
    };

    /** brk style memory pre-allocated for ListNode, simplifies ListNode allocation and eliminates the ABA problem */
    struct ConcurrentBrk {
      uint8_t *base_ptr;
      size_t size;
      std::atomic<uint8_t *> curr_ptr;  /*!< current top of the brk */

      ConcurrentBrk() = delete;
      explicit ConcurrentBrk(size_t size);  /*!< allocates memory of given size and lock in memory */
      void *Alloc(size_t alloc_size);
      void Reset();  /*!< resets the top pointer of the brk */
    };

    static constexpr size_t kListNodeBrkSize = 16 * 1024 * 1024;  /*!< size of the brk to pre-allocate 16MB */

    std::atomic<ListNode *> list_head = nullptr;
    ConcurrentBrk list_node_brk;

    ReadyQueue();
    void Reset();
    bool IsEmpty();
    void Add(CoroStack *coro);  /*!< atomically add coroutine the ready queue */
    CoroStack *Pop();  /*!< atomically pops the ready queue */
  };

public:
  /* Settings */
  static bool g_use_coro_sched;  /*!< if set use the coroutine scheduler instead of the normal ExecutionRoutine one */
  static bool g_use_signal_future;  /*!< if set use signal mechanism for future waits */
  static size_t g_ooo_buffer_size;  /*!< size of the out of order execution window */
  static uint64_t g_preempt_step; /*!< backoff step for preempted pieces */
  static uint64_t g_max_backoff;  /*!< max number of backoff steps for preempted pieces */

private:
  static constexpr size_t kMaxNrCoroutine = 40000;  /*!< number of coroutines allocated at initialization */
  static constexpr size_t kCoroutineStackSize = 32 * 1024;  /*!< min size of the coroutines' stack */
  static constexpr uint64_t kPeriodicIOInterval = 0x3F;  /*!< PeriodicIO event trigger interval */

  uint64_t core_id;  /*!< id of the core where this scheduler belongs to */
  bool is_running;  /*!< used to check whether StartCoroExec has overlapping calls */
  std::list<CoroStack *> free_corostack_list;  /*!< list of pre-allocated but unused coroutines */
  CoroStack **ooo_buffer;  /*!< out of order buffer, kept as a min heap */
  size_t ooo_buffer_len = 0;  /*!< number of ooo_buffer entry used */
  CoroStack *paused_coro = nullptr;  /*!< if set, means a coroutine paused itself to execute a ready queue piece */
  EpochExecutionDispatchService &svc;  /*!< reference to the dispatch service where we get our new routines */
                                       /*!< **Caution**: this only works with the EpochExecutionDispatchService */
  PromiseRoutineTransportService &transport;  /*!< reference to the transport service where we do periodic IOs */
  uint64_t periodic_counter = 0;  /*!< counter for triggering periodic events */
  ReadyQueue ready_queue;  /*!< list of detached coroutines that became ready using the signal mechanism */
  uint64_t num_detached_coros = 0;  /*!< keeps track of number of detached coroutines */

public:
  static void Init();  /*!< Initializes CoroSched on a core. Must be called once before executing */
  static CoroSched *GetCoroSchedForCore(int core_id);  /*!< get a core's responsible coro_sched */

  CoroSched() = delete;
  explicit CoroSched(uint64_t core_id);  /*!< Pre-allocates the coroutines */

  /* Scheduler Calls */
  void StartCoroExec();  /*!< The entry point of the Coroutine Scheduler. To be called by the ExecutionRoutine */
  bool WaitForVHandleVal();  /*!< calls when waiting for a vhandle value and tries to preempt */
  bool WaitForFutureValue(BaseFutureValue *future);  /*!< calls when waiting for a future */
  void AddToReadyQueue(CoroStack *coro);  /*!< adds a coroutine previously attached somewhere else back */

  /* Debug */
  void DumpStatus(bool halt = true);

private:
  static void WorkerFunction();  /*!< worker function executed by the coroutines */

  /* Utility Functions */
  CoroStack *GetCoroStack();  /*!< util to get a free coroutine from the free list */
  void ReturnCoroStack(CoroStack *cs);  /*!< util to return a coroutine to the free list */
  void Reset();  /*!< resets the coroutine scheduler before exiting */

  /* Scheduler Calls - to be called within the coroutines */
  PieceRoutine *GetNewPiece();  /*!< try to get a new piece to run, scheduler may shut caller coroutine down */
  void ExitExecutionRoutine();  /*!< returns the current coroutine and switch back to the main routine to exit */
  void ShutdownAndSwitchTo(CoroStack *coro);  /*!< shutdown myself and switch to a different coroutine */
  void SwitchTo(CoroStack *coro);  /*!< switch to a selected coroutine that was preempted */
  void StartNewCoroutine();  /*!< switch to run a brand new coroutine */
  void ShutdownAndStartNew();  /*!< shutdown myself and switch to a new coroutine */

};

}

#endif //FELIS__CORO_SCHED_H
