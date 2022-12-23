//
// Created by Shujian Qian on 2022-08-01
//

#ifndef FELIS__COROUTINE_H_
#define FELIS__COROUTINE_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// currently only support x86-64
#ifdef __x86_64__
#else
#error "Platform not supported."
#endif

#define CORO_RET_ADDR_IDX 4
#define CORO_SP_IDX 5
#define CORO_BP_IDX 7
#define CORO_FPUCW_MXCSR_IDX 8

struct coroutine;

struct coro_private_stack {
	void *ptr;
	size_t size;
	size_t valid_size;
	size_t max_cp_size;
	size_t cp_to_save;
	size_t cp_to_restore;
};

struct coro_shared_stack {
	void *ptr;
	void *real_ptr;
	void *aligned_highptr;
	void *aligned_ret_ptr;
	size_t size;
	size_t real_size;
	size_t aligned_valid_size;
	size_t aligned_limit;
	struct coroutine *owner;
	bool guard_page_enabled;
};

/**
 * Creates a new shared stack of size and may have a read-only guard page for detection of stack overflow.
 *
 * \param size Minimum size of the shared stack.
 * \param enable_guard_page Whether to add a read only page protection.
 */
struct coro_shared_stack *coro_create_shared_stack(size_t size,
						   bool enable_guard_page, bool lock);

/**
 * Creates a new shared stack of size size_t and sets the coro_shared_stack struct with the newly created stack.
 *
 * @param stack
 * @param size
 * @param enable_guard_page
 */
void coro_allocate_shared_stack(struct coro_shared_stack *stack, size_t size, bool enable_guard_page, bool lock);

/**
 * Destroy the shared sstack.
 *
 * Note: All coroutines that shares the sstack must be destroyed before the shared sstack can be destroyed.
 *
 * \param sstack
 */
void coro_destroy_shared_stack(struct coro_shared_stack *sstack);

/**
 * Coroutine function type.
 */
typedef void (*coro_func_t)(void);

struct coroutine {
#ifdef CORO_SAVE_FPUCW_MXCSR
	void *reg[9];
#else
	void *reg[8];
#endif
	struct coroutine *main_co;
	void *args;
	bool is_finished;
	coro_func_t fptr;
	struct coro_shared_stack *shared_stack;
};

extern __thread struct coroutine *coro_glbl_tls_current_co;
//extern __thread struct coroutine *coro_glbl_tls_yield_co;
extern __thread struct coroutine *coro_glbl_tls_main_co;

/**
 * Initializes the per thread coroutine environment in the current thread.
 *
 * \param exception_func
 */
void coro_thread_init(coro_func_t exception_func);

struct coroutine *coro_create(struct coroutine *main_co,
			      struct coro_shared_stack *shared_stack,
			      coro_func_t coro_func, void *args);

void coro_reuse_coroutine(struct coroutine *coro, struct coroutine *main_co,
			  struct coro_shared_stack *shared_stack,
			  coro_func_t coro_func, void *args);

/**
 * Resets the coroutine using it's previous function pointer and stack pointer
 */
void coro_reset_coroutine(struct coroutine *coro);

/**
 * Returns the pointer to the current non-main coroutine. The caller of the function must be a non-main coroutine.
 */
#define coro_get_co() (coro_glbl_tls_current_co)

#define coro_get_main_co() (coro_glbl_tls_main_co)

/**
 * Equivalent to (coro_get_co()->args). The caller of this function must be a non-main coroutine.
 * \return
 */
#define coro_get_args() (coro_glbl_tls_current_co->args)

/**
 *
 */
void coro_yield();

/**
 *
 */
void coro_yield_to(struct coroutine *to_co);

/**
 *
 */
#define coro_exit()                                \
	do {                                       \
		coro_get_co()->is_finished = true; \
		coro_yield();                      \
	} while (0);

#define coro_mark_finish()                         \
	do {                                       \
		coro_get_co()->is_finished = true; \
	} while (0)

/**
 *
 */
void coro_destroy(struct coroutine *co);

/**
 *
 */
bool coro_is_finished(struct coroutine *co);

void coro_switch(struct coroutine *from_co, struct coroutine *to_co);

void coro_resume(struct coroutine *to_co);

#ifdef __cplusplus
};
#endif

#endif //FELIS__COROUTINE_H_
