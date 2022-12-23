//
// Created by Shujian Qian on 2022-12-03.
//

#include "coro_sched.h"

#include "gopp/gopp.h"
#include "log.h"
#include "opts.h"

bool felis::CoroSched::g_use_coro_sched = false;
__thread felis::CoroSched *felis::coro_sched = nullptr;

void felis::CoroSched::Init()
{
  auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  logger->info("Initializing CoroSched on core {}", core_id);
  coro_thread_init(nullptr);
  coro_sched = new CoroSched(core_id);
}

void felis::CoroSched::StartCoroExec() {
  // in this new scheduler, only one ExecutionRoutine should run at a time
  abort_if(is_running, "StartCoroExec is called before a previous call returns");
  is_running = true;

  // let's just grab a coroutine and start running
  CoroStack *cs = GetCoroStack();
  auto &co = cs->coroutine;
  auto &stack = cs->stack;
  coro_reset_coroutine(&co);
  coro_resume(&co);

  // when the main coroutine resume execution, it's time to exit
  is_running = false;
}

void felis::CoroSched::WorkerFunction()
{
  auto core_id = (int) coro_sched->core_id;
  auto &svc = coro_sched->svc;
  PieceRoutine *routine;
  while ((routine = coro_sched->GetNewPiece())) {
    routine->callback(routine);
    svc.Complete(core_id);
  }
  abort();  // unreachable
}
felis::CoroSched::CoroStack *felis::CoroSched::GetCoroStack()
{
  abort_if(free_corostack_list.empty(), "All CoroStack used up.");
  auto cs = free_corostack_list.front();
  free_corostack_list.pop_front();
  return cs;
}
void felis::CoroSched::ReturnCoroStack(felis::CoroSched::CoroStack *cs)
{
  free_corostack_list.push_front(cs);
}

felis::PieceRoutine *felis::CoroSched::GetNewPiece()
{
  auto &zq = svc.queues[core_id]->zq;
  auto &q = svc.queues[core_id]->pq;
  auto &state = svc.queues[core_id]->state;
  auto &sched_pol = *static_cast<ConservativePriorityScheduler *>(q.sched_pol);

retry_after_periodicIO:

  state.running = EpochExecutionDispatchService::State::kDeciding;

  // 1. try to run something from the zero queue
  uint64_t zstart = zq.start;
  if (zstart < zq.end) {
    state.running = EpochExecutionDispatchService::State::kRunning;
    PieceRoutine *routine = zq.q[zstart];
    zq.start = zstart + 1;
    return routine;
  }

  // if the CallTxnWorker on this core has not finished
  // exit the ExecutionRoutine and let the CallTxnWorker finish first
  if (!svc.IsReady((int) core_id)) {
    state.running = EpochExecutionDispatchService::State::kSleeping;
    transport.PeriodicIO((int) core_id);  // exiting soon, might as well do a periodic IO

    assert(free_corostack_list.size() == kMaxNrCoroutine - 1);
    auto cs = (CoroStack *) coro_get_co();
    ExitExecutionRoutine();
    abort();  // unreachable
  }

  if (sched_pol.empty()
      && ooo_bufer_len == 0
      && q.pending.end == q.pending.start) {
    state.running = EpochExecutionDispatchService::State::kSleeping;
  } else {
    state.running = EpochExecutionDispatchService::State::kRunning;
  }

  // Process the pending buffer of the priority queue
  svc.ProcessPending(q);

  // 2. try to run something from the OOO buffer
  auto waiting_coro = ooo_buffer[0];
  if (ooo_bufer_len > 0
      && (ooo_bufer_len == kOOOBufferSize
          || (sched_pol.empty() || sched_pol.q[0].key > waiting_coro->preempt_key))) {
    std::pop_heap(ooo_buffer, ooo_buffer + ooo_bufer_len, CoroStack::MinHeapCompare);
    ooo_bufer_len--;
    ShutdownAndSwitchTo(waiting_coro);
  }

  // 3. try to run something from the priority queue
  if (!sched_pol.empty()) {
    auto node = sched_pol.Pick();
    PieceRoutine *routine = node->routine;
    sched_pol.Consume(node);
    return routine;
  }

  // 4. nothing to do till now, update the completion counter then
  auto &local_comp = state.complete_counter;
  auto num_local_completed = local_comp.completed;
  local_comp.completed = 0;
  auto global_comp = EpochClient::g_workload_client->completion_object();

  if (num_local_completed > 0) {
    global_comp->Complete(num_local_completed);
  }

  // 5. do periodic IO, if there could be anything new, retry everything
  bool should_retry_after_periodicIO = transport.PeriodicIO((int) core_id);
  if (should_retry_after_periodicIO) {
    goto retry_after_periodicIO;
  }

  // there's truely nothing to do, exit ExecutionRoutine
  assert(free_corostack_list.size() == kMaxNrCoroutine - 1);
  ExitExecutionRoutine();

  abort();  // unreachable
}

bool felis::CoroSched::WaitForVHandleVal() {
  return false;
}

bool felis::CoroSched::WaitForFutureValue(FutureValue<uint32_t> *future) {
  return false;
}

void felis::CoroSched::ExitExecutionRoutine()
{
  auto cs = (CoroStack *) coro_get_co();
  ReturnCoroStack(cs);
  coro_yield_to(coro_get_main_co());
}

void felis::CoroSched::ShutdownAndSwitchTo(felis::CoroSched::CoroStack *coro) {
  auto cs = (CoroStack *) coro_get_co();
  ReturnCoroStack(cs);
  coro_yield_to((coroutine *) coro);
}

felis::CoroSched::CoroSched(uint64_t core_id)
  :
  core_id{core_id},
  is_running{false},
  svc{dynamic_cast<EpochExecutionDispatchService&>(util::Impl<PromiseRoutineDispatchService>())},
  transport{util::Impl<PromiseRoutineTransportService>()}
{
  for (int i = 0; i < kMaxNrCoroutine; i++) {
    auto coro_stack = (CoroStack *) malloc(sizeof(CoroStack));
    coro_allocate_shared_stack(&coro_stack->stack, kCoroutineStackSize, Options::kNoHugePage, true);
    coro_stack->core_id = core_id;
    coro_stack->coroutine.is_finished = true;
    coro_reuse_coroutine(&coro_stack->coroutine, coro_get_co(), &coro_stack->stack, WorkerFunction, nullptr);
    free_corostack_list.push_front(coro_stack);
  }
}

bool felis::CoroSched::CoroStack::MinHeapCompare(felis::CoroSched::CoroStack *a, felis::CoroSched::CoroStack *b) {
  if (a->preempt_key > b->preempt_key) return true;
  if (a->preempt_key < b->preempt_key) return false;
  return a->sched_key > b->sched_key;
}
