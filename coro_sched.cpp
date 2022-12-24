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

  cs_trace("CoroSched on core {} it starting to run.", core_id);
  // let's just grab a coroutine and start running
  StartNewCoroutine();

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
  assert(cs->coroutine.fptr == WorkerFunction);
  return cs;
}
void felis::CoroSched::ReturnCoroStack(felis::CoroSched::CoroStack *cs)
{
  assert(cs->coroutine.fptr == WorkerFunction);
  free_corostack_list.push_front(cs);
}

felis::PieceRoutine *felis::CoroSched::GetNewPiece()
{
  auto &zq = svc.queues[core_id]->zq;
  auto &q = svc.queues[core_id]->pq;
  auto &state = svc.queues[core_id]->state;
  auto &priority_queue = *static_cast<ConservativePriorityScheduler *>(q.sched_pol);

retry_after_periodicIO:

  state.running = EpochExecutionDispatchService::State::kDeciding;

  // 1. try to run something from the zero queue
  uint64_t zstart = zq.start;
  if (zstart < zq.end) {
    state.running = EpochExecutionDispatchService::State::kRunning;
    PieceRoutine *routine = zq.q[zstart];
    zq.start = zstart + 1;
    ((CoroStack *) coro_get_co())->sched_key = routine->sched_key;
    return routine;
  }

  // if the CallTxnWorker on this core has not finished
  // exit the ExecutionRoutine and let the CallTxnWorker finish first
  if (!svc.IsReady((int) core_id)) {
    state.running = EpochExecutionDispatchService::State::kSleeping;
    // CAVEAT: cannot call PeriodicIO before the CallTxnsWorker sets the finished flag
    //         setting the local buffer plan needs to be synchronized before updating buffer plan from other nodes

    assert(free_corostack_list.size() == kMaxNrCoroutine - 1);
    auto cs = (CoroStack *) coro_get_co();
    ExitExecutionRoutine();
    abort();  // unreachable
  }

  if (priority_queue.empty()
      && ooo_buffer_len == 0
      && q.pending.end == q.pending.start) {
    state.running = EpochExecutionDispatchService::State::kSleeping;
  } else {
    state.running = EpochExecutionDispatchService::State::kRunning;
  }

  // Process the pending buffer of the priority queue
  svc.ProcessPending(q);

  periodic_counter++;
  if ((periodic_counter & kPeriodicIOInterval) == 0) {
    transport.PeriodicIO(core_id);
  }

  // 2. try to run something from the OOO buffer
  auto waiting_coro = ooo_buffer[0];
  if (ooo_buffer_len > 0
      && (ooo_buffer_len == kOOOBufferSize
          || (priority_queue.empty() || priority_queue.q[0].key > waiting_coro->preempt_key))) {
    std::pop_heap(ooo_buffer, ooo_buffer + ooo_buffer_len, CoroStack::MinHeapCompare);
    ooo_buffer_len--;
    ShutdownAndSwitchTo(waiting_coro);
    abort();  // unreachable
  }

  // 3. try to run something from the priority queue
  if (!priority_queue.empty()) {
    auto node = priority_queue.Pick();
    PieceRoutine *routine = node->routine;
    priority_queue.Consume(node);
    ((CoroStack *) coro_get_co())->sched_key = routine->sched_key;
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
  auto &zq = svc.queues[core_id]->zq;
  auto &q = svc.queues[core_id]->pq;
  auto &state = svc.queues[core_id]->state;
  auto &priority_queue = *static_cast<ConservativePriorityScheduler *>(q.sched_pol);

  svc.ProcessPending(q);

  periodic_counter++;
  if ((periodic_counter & kPeriodicIOInterval) == 0) {
    transport.PeriodicIO(core_id);
  }

  CoroStack &me = *((CoroStack *) coro_get_co());
  // preemption is not allowed for the zero queue routines
  if (me.sched_key == 0) {
    return false;
  }

  abort_if(ooo_buffer_len == kOOOBufferSize, "OOO Window is full");

  CoroStack *&wait_state = ooo_buffer[ooo_buffer_len];
  if (wait_state != nullptr) {
    if (wait_state->sched_key == me.sched_key) {
      me.preempt_times++;
    } else {
      wait_state->preempt_times = 1;
      q.waiting.unique_preempts++;
    }
  } else {
    me.preempt_times = 1;
  }

  wait_state = &me;
  me.preempt_key = me.sched_key + kPreemptKeyThreshold * std::min(me.preempt_times, kMaxBackoff);

  // if there's nothing else to switch to, do not preempt
  if (ooo_buffer_len == 0  // ooo_buffer is empty
      && (priority_queue.empty() || priority_queue.q[0].key > me.preempt_key)  // no new piece in priority queue
      && zq.start >= zq.end) {  // zero queue is empty
    return false;
  }

  // add myself to the ooo buffer
  ooo_buffer_len++;
  std::push_heap(ooo_buffer, ooo_buffer + ooo_buffer_len, CoroStack::MinHeapCompare);

  // now find what to execute next

  // 1. check if there's something to run in the zero queue
  uint64_t zstart = zq.start;
  if (zstart < zq.end) {
    // preempt to a new coroutine to execute the zero queue piece
    cs_trace("core {} starting a new coroutine to run a zero queue piece", core_id);
    StartNewCoroutine();
    return true;
  }

  // 2. check whether we should switch to a different waiting coroutine
  CoroStack *candidate = ooo_buffer[0];
  if (ooo_buffer_len > 0
      && (ooo_buffer_len == kOOOBufferSize
          || (priority_queue.empty() || priority_queue.q[0].key > candidate->preempt_key))) {
    std::pop_heap(ooo_buffer, ooo_buffer + ooo_buffer_len, CoroStack::MinHeapCompare);
    ooo_buffer_len--;
    if (candidate == &me) {
      return false;
    } else {
      SwitchTo(candidate);
      return true;
    }
  }

  // 3. finally run new PieceRoutines on the priority queue
  if (!priority_queue.empty()) {
    cs_trace("core {} starting a new coroutine to run a priority queue piece", core_id);
    StartNewCoroutine();
    return true;
  }

  abort();  // unreachable
}

bool felis::CoroSched::WaitForFutureValue(FutureValue<uint32_t> *future) {
  return false;
}

void felis::CoroSched::ExitExecutionRoutine()
{
  auto cs = (CoroStack *) coro_get_co();
  ReturnCoroStack(cs);
  cs_trace("core {} is ExitExecutionRoutine {} from {}", core_id, (void *) coro_get_main_co(), (void *) coro_get_co());
  assert(cs->coroutine.fptr == WorkerFunction);
  coro_yield_to(coro_get_main_co());
}

void felis::CoroSched::ShutdownAndSwitchTo(felis::CoroSched::CoroStack *coro) {
  auto cs = (CoroStack *) coro_get_co();
  ReturnCoroStack(cs);
  cs_trace("core {} is ShutdownAndSwitchTo {} from {}", core_id, (void *) coro, (void *) coro_get_co());
  assert(coro->coroutine.fptr == WorkerFunction);
  coro_yield_to((coroutine *) coro);
}

void felis::CoroSched::StartNewCoroutine() {
  CoroStack *cs = GetCoroStack();
  auto &co = cs->coroutine;
  auto &stack = cs->stack;
  coro_reset_coroutine(&co);
  cs_trace("core {} is StartNewCoroutine {} from {}", core_id, (void *) &co, (void *) coro_get_co());
  assert(co.fptr == WorkerFunction);
  coro_resume(&co);
}

void felis::CoroSched::SwitchTo(felis::CoroSched::CoroStack *coro) {
  cs_trace("core {} is SwitchTo {} from {}", core_id, (void *) &coro, (void *) coro_get_co());
  assert(coro->coroutine.fptr == WorkerFunction);
  coro_yield_to((coroutine *) coro);
}

felis::CoroSched::CoroSched(uint64_t core_id)
  :
  core_id{core_id},
  is_running{false},
  svc{dynamic_cast<EpochExecutionDispatchService&>(util::Impl<PromiseRoutineDispatchService>())},
  transport{util::Impl<PromiseRoutineTransportService>()}
{
  std::fill(ooo_buffer, ooo_buffer + kOOOBufferSize, nullptr);
  for (int i = 0; i < kMaxNrCoroutine; i++) {
    auto coro_stack = (CoroStack *) malloc(sizeof(CoroStack));
    coro_allocate_shared_stack(&coro_stack->stack, kCoroutineStackSize, Options::kNoHugePage, true);
    coro_stack->core_id = core_id;
    coro_stack->coroutine.is_finished = true;
    coro_reuse_coroutine(&coro_stack->coroutine, coro_get_co(), &coro_stack->stack, WorkerFunction, nullptr);
    assert(coro_stack->coroutine.fptr == WorkerFunction);
    free_corostack_list.push_front(coro_stack);
  }
}

bool felis::CoroSched::CoroStack::MinHeapCompare(felis::CoroSched::CoroStack *a, felis::CoroSched::CoroStack *b) {
  if (a->preempt_key > b->preempt_key) return true;
  if (a->preempt_key < b->preempt_key) return false;
  return a->sched_key > b->sched_key;
}
