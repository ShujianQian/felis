#ifndef BENCHMARKS_BST_BENCHMARK_H_
#define BENCHMARKS_BST_BENCHMARK_H_

#include <stdlib.h>

void init_bst(int seed);

void flush_cache(void);

int naive_bst(void);

void coroutine_bst_init(void);

int coroutine_bst(void);

#endif // BENCHMARKS_BST_BENCHMARK_H_