#include "bst_benchmark.h"

#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#include "x86intrin.h"

#include "coroutine.h"

#define K_L3_CACHE_SIZE (16384 * 1024)
#define K_CACHELIN_SIZE 64
#define K_NUM_NODES (K_L3_CACHE_SIZE / sizeof(int64_t) * 64)
//#define K_NUM_NODES (SIZE_MAX / 2)
// #define K_NUM_NODES 128 * 1024
#define K_SEARCH 1000000
#define K_BST_STRIDE 10
#define K_BST_MAX (K_NUM_NODES * K_BST_STRIDE)
#define K_NUM_COROUTINE 6

static int64_t cache[K_L3_CACHE_SIZE / sizeof(int64_t)];
static int64_t bst[K_NUM_NODES];
static int64_t search[K_SEARCH];

void init_bst(int seed)
{
	srand(seed);
	for (size_t i = 0; i < K_NUM_NODES; i++) {
		bst[i] = i * K_BST_STRIDE;
	}
	for (size_t i = 0; i < K_SEARCH; i++) {
		search[i] = rand() % K_BST_MAX;
	}
}

void flush_cache()
{
	for (int i = 0; i < K_L3_CACHE_SIZE / sizeof(int64_t);
	     i += K_CACHELIN_SIZE / sizeof(int64_t)) {
		cache[i] = rand();
	}
}

int naive_bst()
{
	int found_count = 0;
	for (int i = 0; i < K_SEARCH; i++) {
		int64_t key = search[i];
		int64_t *p_lo = bst;
		int64_t *p_hi = bst + sizeof(bst) / sizeof(int64_t);
		int64_t found;
		while (p_hi - p_lo > 1) {
			int64_t *p_mid = p_lo + (p_hi - p_lo) / 2;
			if (*p_mid < key) {
				p_lo = p_mid;
			} else {
				p_hi = p_mid;
			}
		}
		if (*p_lo == key) {
			found = *p_lo;
		} else if (*p_hi == key) {
			found = *p_hi;
		} else {
			found = -1;
		}
		if (found != -1) {
			found_count++;
		}
		assert(((key % K_BST_STRIDE == 0) && (found == key)) ||
		       ((key % K_BST_STRIDE != 0) && (found == -1)));
	}
	return found_count;
}

struct coroutine_bst_args {
	int64_t key;
	int64_t *bst;
	int64_t found;
	bool logged;
};

static struct coroutine coroutines[K_NUM_COROUTINE];
static struct coro_shared_stack *stacks[K_NUM_COROUTINE];
static struct coroutine_bst_args coroutine_bst_args[K_NUM_COROUTINE];

void coroutine_bst_init()
{
	coro_thread_init(NULL);
	for (int i = 0; i < K_NUM_COROUTINE; i++) {
		coroutines[i].is_finished = true;
		stacks[i] = coro_create_shared_stack(4096, true, true);
		coroutine_bst_args[i].bst = bst;
		coroutine_bst_args[i].logged = true;
	}
}

struct coroutine_bst_scheduler_states {
	int i, idx, finished_count, found_count;
} coroutine_bst_scheduler_states;

static void coroutine_bst_worker(void);

static void coroutine_bst_scheduler(void)
{
again:
	coroutine_bst_scheduler_states.idx++;
	if (coroutine_bst_scheduler_states.idx >= K_NUM_COROUTINE) {
		coroutine_bst_scheduler_states.idx = 0;
	}

	if (coroutines[coroutine_bst_scheduler_states.idx].is_finished) {
		if (!coroutine_bst_args[coroutine_bst_scheduler_states.idx]
			     .logged) {
			coroutine_bst_args[coroutine_bst_scheduler_states.idx]
				.logged = true;
			if (coroutine_bst_args[coroutine_bst_scheduler_states.idx]
				    .found != -1) {
				coroutine_bst_scheduler_states.found_count++;
			}
		}
		if (coroutine_bst_scheduler_states.i < K_SEARCH) {
			coroutine_bst_scheduler_states.i++;
			coroutine_bst_args[coroutine_bst_scheduler_states.idx]
				.key = search[coroutine_bst_scheduler_states.i];
			coroutine_bst_args[coroutine_bst_scheduler_states.idx]
				.logged = false;
			coro_reuse_coroutine(
				&coroutines[coroutine_bst_scheduler_states.idx],
				coro_get_co(),
				stacks[coroutine_bst_scheduler_states.idx],
				coroutine_bst_worker,
				&coroutine_bst_args
					[coroutine_bst_scheduler_states.idx]);
			coro_resume(
				&coroutines[coroutine_bst_scheduler_states.idx]);
		} else {
			coroutine_bst_scheduler_states.finished_count++;
			if (coroutine_bst_scheduler_states.finished_count ==
			    K_NUM_COROUTINE)
				coro_yield_to(coro_get_main_co());
			goto again;
		}
	} else {
		coro_resume(&coroutines[coroutine_bst_scheduler_states.idx]);
	}
}

static void coroutine_bst_worker(void)
{
	struct coroutine_bst_args *args =
		(struct coroutine_bst_args *)coro_get_args();
	int64_t key = args->key;
	int64_t *p_lo = args->bst;
	int64_t *p_hi = args->bst + sizeof(bst) / sizeof(int64_t);
	int64_t depth = 0;
	while (p_hi - p_lo > 1) {
		int64_t *p_mid = p_lo + (p_hi - p_lo) / 2;
		// the first few nodes is very like to reside in L1 cache
		if (depth >= 3) {
			_mm_prefetch(p_mid, 0);
			coroutine_bst_scheduler();
		}
		depth++;
		if (*p_mid < key) {
			p_lo = p_mid;
		} else {
			p_hi = p_mid;
		}
	}
	if (*p_lo == key) {
		args->found = *p_lo;
	} else if (*p_hi == key) {
		args->found = *p_hi;
	} else {
		args->found = -1;
	}
	assert(((key % K_BST_STRIDE == 0) && (args->found == key)) ||
	       ((key % K_BST_STRIDE != 0) && (args->found == -1)));

	coro_mark_finish();
	coroutine_bst_scheduler();
}

int coroutine_bst()
{
	coroutine_bst_scheduler_states.i = 0;
	coroutine_bst_scheduler_states.idx = 0;
	coroutine_bst_scheduler_states.finished_count = 0;
	coroutine_bst_scheduler_states.found_count = 0;

	coroutine_bst_scheduler();

	return coroutine_bst_scheduler_states.found_count;
}