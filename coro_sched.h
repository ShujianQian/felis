//
// Created by Shujian Qian on 2022-12-03.
//

#ifndef FELIS__CORO_SCHED_H
#define FELIS__CORO_SCHED_H

#include <list>

#include "routine_sched.h"
#include "util/objects.h"
#include "piece.h"
#include "txn_cc.h"
#include "coroutine.h"

namespace felis {

class CoroSched;

extern __thread CoroSched *coro_sched;

class CoroSched {
public:
  struct CoroStack {
    struct coroutine coroutine;  /*!< coroutine struct containing the function pointer and the saved context */
    struct coro_shared_stack stack;  /*!< keeps the info about the size of the stack and pointers to the stack */
    uint64_t core_id;  /*!< where this CoroStack belongs to, used to add to the core's ready queue */
    uint64_t sched_key;  /*!< the scheduling key of the waiting coroutine */
    uint64_t preempt_key;  /*!< the sched key with backoff of the waiting coroutine */
    static bool MinHeapCompare(CoroStack *a,  CoroStack *b);  /*!< compare function to build heap */
  };

  static constexpr size_t kMaxNrCoroutine = 1000;  /*!< number of coroutines allocated at initialization */
  static constexpr size_t kCoroutineStackSize = 8192;  /*!< min size of the coroutines' stack */
  static constexpr size_t kOOOBufferSize = 25;  /*!< size of the out of order execution window */
  static bool g_use_coro_sched;  /*!< if set use the coroutine scheduler instead of the normal ExecutionRoutine one */

  uint64_t core_id;  /*!< id of the core where this scheduler belongs to */
  std::atomic<uint16_t> tag;  /*!< used to reduce the chance of the ABA problem */
  bool is_running;  /*!< used to check whether StartCoroExec has overlapping calls */
  std::list<CoroStack *> free_corostack_list;  /*!< list of pre-allocated but unused coroutines */
  CoroStack *ooo_buffer[kOOOBufferSize];  /*!< out of order buffer, kept as a min heap */
  size_t ooo_bufer_len = 0;  /*!< number of ooo_buffer entry used */
  EpochExecutionDispatchService &svc;  /*!< reference to the dispatch service where we get our new routines */
                                       /*!< **Caution**: this only works with the EpochExecutionDispatchService */
  PromiseRoutineTransportService &transport;  /*!< reference to the transport service where we do periodic IOs */

  CoroSched() = delete;
  explicit CoroSched(uint64_t core_id);  /*!< Pre-allocates the coroutines */

  static void Init();  /*!< Initializes CoroSched on a core. Must be called once before executing */
  void StartCoroExec();  /*!< The entry point of the Coroutine Scheduler. To be called by the ExecutionRoutine */

private:
  static void WorkerFunction();  /*!< worker function executed by the coroutines */

  /* Utility Functions */
  CoroStack *GetCoroStack();  /*!< util to get a free coroutine from the free list */
  void ReturnCoroStack(CoroStack *cs);  /*!< util to return a coroutine to the free list */

  /* Scheduler Calls - to be called within the coroutines */
  PieceRoutine *GetNewPiece();  /*!< try to get a new piece to run, scheduler may shut caller coroutine down */
  bool WaitForVHandleVal();  /*!< calls when waiting for a vhandle value and tries to preempt */
  bool WaitForFutureValue(FutureValue<uint32_t> *future);  /*!< calls when waiting for a future */
  void ExitExecutionRoutine();  /*!< returns the current coroutine and switch back to the main routine to exit */
  void ShutdownAndSwitchTo(CoroStack *coro);  /*!< shutdown myself and switch to a different coroutine */

};

}

#endif //FELIS__CORO_SCHED_H
