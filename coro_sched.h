//
// Created by Shujian Qian on 2022-12-03.
//

#ifndef FELIS__CORO_SCHED_H
#define FELIS__CORO_SCHED_H

#include <list>

#include "routine_sched.h"
#include "util/objects.h"
#include "coroutine.h"

namespace felis {

class CoroSched {
public:

  static __thread std::list<struct coroutine> free_coro_list;
  static __thread std::list<struct coro_shared_stack> free_stack_list;

  /**
   * Initializes CoroSched on a core. Must be called once before executing.
   */
  static void Init();

  /**
   * The entry point of the Coroutine Scheduler. To be called by the ExecutionRoutine.
   */
  static void StartCoroExec();

};

}

#endif //FELIS__CORO_SCHED_H
