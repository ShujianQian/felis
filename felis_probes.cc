#include "felis_probes.h"

#ifdef PROBE_ENBALED

// PROBE(felis, wait_jiffies, long jiffies, uint64_t sid, uint64_t ver);
// PROBE(felis, local_index_cache, void *p);

// PROBE(felis, linklist_search_write, int cnt, size_t size);
// PROBE(felis, linklist_search_read, int cnt, size_t size);

#else

#include <cstdlib>
#include <iostream>

static struct {
  agg::Agg<agg::LogHistogram<16>> read_ll_skip, write_ll_skip;
  agg::Agg<agg::Histogram<256, 0, 1500>> tot_wait;
  agg::Agg<agg::Average> tot_wait_avg;
  agg::Agg<agg::Average> tot_local_cache;
} global;

static thread_local struct {
  AGG(read_ll_skip);
  AGG(write_ll_skip);
  AGG(tot_wait_avg);
  AGG(tot_wait);
  AGG(tot_local_cache);

  int last_rel_id;
} statcnt;

PROBE(felis, wait_jiffies, long jiffies, uint64_t sid, uint64_t ver) {
  if (sid % 2 == 0 && ver % 2 == 1) {
    statcnt.tot_wait << jiffies;
    statcnt.tot_wait_avg << jiffies;
  }
}

PROBE(felis, local_index_cache, void *p) {
  int c = p ? 1 : 0;
  statcnt.tot_local_cache << c;
}

PROBE(felis, index_get, int id, const void *key, uint64_t sid) {
  statcnt.last_rel_id = id;
}

PROBE(felis, linklist_search_read, int cnt, size_t size) {
  statcnt.read_ll_skip << cnt;
}

PROBE(felis, linklist_search_write, int cnt, size_t size) {
  statcnt.write_ll_skip << cnt;
}

AT_EXIT() {
#if 0
  std::cout << "wait_jiffies: " << std::endl << global.tot_wait() << std::endl
            << "on avg: " << global.tot_wait_avg() << std::endl;

  std::cout << "local index cache: " << std::endl << global.tot_local_cache() << std::endl;

  std::cout << "read_ll_skip" << std::endl << global.read_ll_skip() << std::endl;
  std::cout << "write_ll_skip" << std::endl << global.write_ll_skip() << std::endl;
#endif
}

#endif
