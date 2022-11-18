//
// Created by shujianqian on 14/11/22.
//

#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <sched.h>

#include "x86intrin.h"

#include "bst_benchmark.h"

#define MS 2200000
#define US 2200
#define TIME_EXEC(res, unit, workload)          \
	do {                                    \
		uint64_t start = __rdtsc();     \
		(workload);                     \
		uint64_t end = __rdtsc();       \
		(res) = (end - start) / (unit); \
	} while (0)

int main(int argc, char **argv)
{
	uint64_t exec_time;
	cpu_set_t cpu_mask;

	CPU_ZERO(&cpu_mask);
	CPU_SET(0, &cpu_mask);
	sched_setaffinity(0, sizeof(cpu_mask), &cpu_mask);

	TIME_EXEC(exec_time, MS, init_bst(0));
	fprintf(stdout, "init_bst: %lu ms\n", exec_time);

	TIME_EXEC(exec_time, MS, flush_cache());
	fprintf(stdout, "flush_cache: %lu ms\n", exec_time);

	coroutine_bst_init();
	uint64_t naive_total_exec_time = 0;
	uint64_t coroutine_total_exec_time = 0;
	for (int i = 0; i < 10; i++) {
		int found_count;
		flush_cache();
		TIME_EXEC(exec_time, US, found_count = naive_bst());
		naive_total_exec_time += exec_time;
		fprintf(stdout, "naive_bst: %lu us\n", exec_time);
		fprintf(stdout, "\tnum_found: %d \n", found_count);
		flush_cache();
		TIME_EXEC(exec_time, US, found_count = coroutine_bst());
		coroutine_total_exec_time += exec_time;
		fprintf(stdout, "coroutine_bst: %lu us\n", exec_time);
		fprintf(stdout, "\tnum_found: %d \n", found_count);
	}
	fprintf(stdout,
		"--------------------------------------------------------------------------------\n");
	fprintf(stdout, "naive_bst: %lu us\n", naive_total_exec_time / 10);
	fprintf(stdout, "coroutine_bst: %lu us\n",
		coroutine_total_exec_time / 10);

	return 0;
}