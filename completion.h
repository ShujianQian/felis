#ifndef _COMPLETION_H
#define _COMPLETION_H

#include <atomic>
#include "log.h"

namespace felis {

/**
 * Utility class that keeps track of a counter of completed jobs.
 * @tparam T Callable that implements PreComplete function to be called each time before counter is decremented, and the callable it self is called when the completion counter reaches zero.
 */
template <typename T>
class CompletionObject {
  std::atomic_long comp_count;
  T callback;
 public:
  CompletionObject(unsigned long count, T callback)
      : comp_count(count), callback(callback) {
    logger->info("Completion object at {}", (void *) this);
  }

  void Complete(long dec = 1) {
    callback.PreComplete();
    auto cnt = comp_count.fetch_sub(dec) - dec;
    if (cnt < 0) {
      logger->error("Completion handler isn't enough! left = {}, completed = {}", cnt + dec, dec);
      std::abort();
    }
    callback(cnt);
  }

  void operator()() {
    Complete();
  }

  void Increment(unsigned long inc) {
    comp_count.fetch_add(inc);
  }

  unsigned long left_over() const {
    return comp_count.load();
  }
};


}

#endif
