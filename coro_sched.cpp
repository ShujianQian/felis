//
// Created by Shujian Qian on 2022-12-03.
//

#include "coro_sched.h"

#include "gopp/gopp.h"
#include "log.h"

void felis::CoroSched::Init() {
}

void felis::CoroSched::StartCoroExec() {
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  auto &transport = util::Impl<PromiseRoutineTransportService>();

  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;

  cs_trace("CoroSched::StartCoroExec called on core {}", core_id);

  trace(TRACE_EXEC_ROUTINE "new ExecutionRoutine up and running on {}", core_id);

  PieceRoutine *next_r;
  bool give_up = false;
  // BasePromise::ExecutionRoutine *next_state = nullptr;
  go::Scheduler *sched = go::GetSchedulerFromPool(go::Scheduler::CurrentThreadPoolId());

  auto should_pop = PromiseRoutineDispatchService::GenericDispatchPeekListener(
      [&next_r, &give_up, sched]
          (PieceRoutine *r, BasePieceCollection::ExecutionRoutine *state) -> bool {
        if (state != nullptr) {
          if (state->is_detached()) {
            trace(TRACE_EXEC_ROUTINE "Wakeup Coroutine {}", (void *) state);
            state->Init();
            sched->WakeUp(state);
          } else {
            trace(TRACE_EXEC_ROUTINE "Found a sleeping Coroutine, but it's already awaken.");
          }
          give_up = true;
          return false;
        }
        give_up = false;
        next_r = r;
        // next_state = state;
        return true;
      });


  unsigned long cnt = 0x01F;

  do {
    while (svc.Peek(core_id, should_pop)) {
      // Periodic flush
      cnt++;
      if ((cnt & 0x01F) == 0) {
        transport.PeriodicIO(core_id);
      }

      auto rt = next_r;
      if (rt->sched_key != 0)
        debug(TRACE_EXEC_ROUTINE "Run {} sid {}", (void *) rt, rt->sched_key);

      rt->callback(rt);
      svc.Complete(core_id);
    }
  } while (!give_up && svc.IsReady(core_id) && transport.PeriodicIO(core_id));

  trace(TRACE_EXEC_ROUTINE "Coroutine Exit on core {} give up {}", core_id, give_up);
}
