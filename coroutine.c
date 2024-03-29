//
// Created by Shujian Qian on 2022-08-01
//

#include "coroutine.h"

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/mman.h>

#define is_empty(...) (sizeof((char[]){#__VA_ARGS__}) == 1)

#define abort_if_c(cond, ...)            \
  if (__builtin_expect(!!(cond), 0)) {   \
    if (!is_empty(__VA_ARGS__)) {        \
      fprintf(stderr, ""__VA_ARGS__);    \
      fflush(stderr);                    \
    }                                    \
  }

void coro_default_exception_func(void);

__thread struct coroutine *coro_glbl_tls_current_co = NULL;
__thread struct coroutine *coro_glbl_tls_yield_co;
__thread struct coroutine *coro_glbl_tls_main_co = NULL;
static __thread coro_func_t coro_glbl_tls_fptr = coro_default_exception_func;
#ifdef CORO_SAVE_FPUCW_MSCSR
static __thread void *coro_glbl_tls_fpucw_mxcsr[1];
#endif

void coro_thread_init(coro_func_t exception_func)
{
	if ((void *)exception_func != NULL)
		coro_glbl_tls_fptr = exception_func;

	if (coro_glbl_tls_main_co == NULL) {
		struct coroutine *co =
			(struct coroutine *)malloc(sizeof(struct coroutine));
		memset(co, 0, sizeof(struct coroutine));
		coro_glbl_tls_current_co = co;
		coro_glbl_tls_main_co = co;
	}
}

struct coroutine *coro_create(struct coroutine *main_co,
			      struct coro_shared_stack *shared_stack,
			      coro_func_t coro_func, void *args)
{
	struct coroutine *co =
		(struct coroutine *)malloc(sizeof(struct coroutine));
	memset(co, 0, sizeof(struct coroutine));

	if (main_co != NULL) {
		// this is a non-main coroutine
		abort_if_c(shared_stack != NULL);
		co->shared_stack = shared_stack;
		co->reg[CORO_RET_ADDR_IDX] = (void *)coro_func;
		co->reg[CORO_SP_IDX] = co->shared_stack->aligned_ret_ptr;
#ifdef CORO_SAVE_FPUCW_MXCSR
		co->reg[CORO_FPUCW_MXCSR_IDX] = coro_glbl_tls_fpucw_mxcsr[0];
#endif
		co->main_co = main_co;
		co->args = args;
		co->fptr = coro_func;
	} else {
		// this is the main coroutine
		co->main_co = NULL;
		co->args = args;
		co->fptr = coro_func;
		co->shared_stack = NULL;
	}
	return co;
}

void coro_reuse_coroutine(struct coroutine *coro, struct coroutine *main_co,
			  struct coro_shared_stack *shared_stack,
			  coro_func_t coro_func, void *args)
{
	abort_if_c(main_co != NULL);
	abort_if_c(coro->is_finished);
	abort_if_c(shared_stack != NULL);
	coro->main_co = main_co;
	coro->args = args;
	coro->is_finished = false;
	coro->fptr = coro_func;
	coro->shared_stack = shared_stack;
	coro->reg[CORO_RET_ADDR_IDX] = (void *)coro_func;
	coro->reg[CORO_SP_IDX] = coro->shared_stack->aligned_ret_ptr;
}

void coro_reset_coroutine(struct coroutine *coro)
{
  abort_if_c(coro != NULL);
  abort_if_c(coro->shared_stack != NULL);
  abort_if_c(coro->shared_stack->aligned_ret_ptr != NULL);
  abort_if_c(coro->fptr != NULL);
  coro->reg[0] = 0;
  coro->reg[1] = 0;
  coro->reg[2] = 0;
  coro->reg[3] = 0;
  coro->reg[4] = coro->fptr;
  coro->reg[5] = coro->shared_stack->aligned_ret_ptr;
  coro->reg[6] = 0;
  coro->reg[7] = 0;
}

struct coro_shared_stack *coro_create_shared_stack(size_t size,
						   bool enable_guard_page, bool lock)
{
	struct coro_shared_stack *sstack = (struct coro_shared_stack *)malloc(
		sizeof(struct coro_shared_stack));
	memset(sstack, 0, sizeof(*sstack));

    coro_allocate_shared_stack(sstack, size, enable_guard_page, lock);

	return sstack;
}

void coro_allocate_shared_stack(struct coro_shared_stack *stack, size_t size, bool enable_guard_page, bool lock)
{
  if (size == 0)
    size = 1 << 22;
  if (size < 4096)
    size = 4096;
  abort_if_c(size >= 4096);

  size_t page_size = 0;
  if (enable_guard_page) {
    long signed_page_size = sysconf(_SC_PAGESIZE);
    abort_if_c(signed_page_size > 0);
    abort_if_c(((signed_page_size - 1) & signed_page_size) == 0);
    page_size = (size_t)((unsigned long)signed_page_size);
    abort_if_c(page_size == (unsigned long)page_size);
    // check overflow
    abort_if_c((page_size << 1) >> 1 == page_size);

    if (size < page_size) {
      size = page_size * 2;
    } else {
      size_t aligned_size;
      if ((size & (page_size - 1)) != 0) {
        // align sstack size to multiple of pagesize
        aligned_size = (size & (~(page_size - 1)));
        abort_if_c(aligned_size + page_size * 2 > aligned_size);
        aligned_size += page_size * 2;
        abort_if_c(aligned_size / page_size == size / page_size + 2);
      } else {
        aligned_size = size;
        abort_if_c(aligned_size + page_size > aligned_size);
        aligned_size += page_size; // protection page
        abort_if_c(aligned_size / page_size == size / page_size + 1);
      }
      size = aligned_size;
      abort_if_c(size / page_size > 1);
      abort_if_c((size & (page_size - 1)) == 0);
    }
  }

  if (enable_guard_page) {
    stack->guard_page_enabled = true;
    stack->real_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // create a read-only guard page at the end
    abort_if_c(stack->real_ptr);
    abort_if_c(mprotect(stack->real_ptr, page_size, PROT_READ) == 0);
    stack->real_size = size;
    stack->ptr = (void *)(((uintptr_t)stack->real_ptr) + page_size);
    stack->size = size - page_size;
    if (lock) {
      abort_if_c(mlock(stack->real_ptr, stack->real_size) < 0, "Failed to lock coroutine stack in memory");
    }
  } else {
    stack->size = size;
    stack->ptr = malloc(size);
    abort_if_c(stack->ptr);
    if (lock) {
      abort_if_c(mlock(stack->ptr, stack->size) < 0, "Failed to lock coroutine stack in memory");
    }
  }

  stack->owner = NULL;
  uintptr_t aligned_ptr = (uintptr_t)(stack->size - sizeof(void *) * 2 +
      (uintptr_t)stack->ptr);
  aligned_ptr = (aligned_ptr >> 4) << 4; // aligned to 16 bytes
  stack->aligned_highptr = (void *)aligned_ptr;
  stack->aligned_ret_ptr = (void *)(aligned_ptr - sizeof(void *));
  *((void **)(stack->aligned_ret_ptr)) =
      (void *)(coro_default_exception_func);
  stack->aligned_limit =
      stack->size - sizeof(void *) * 2 - 16; // max cost of alignment
}

void coro_destroy_shared_stack(struct coro_shared_stack *sstack)
{
	abort_if_c(sstack != NULL);
	abort_if_c(sstack->ptr != NULL);
	if (sstack->guard_page_enabled) {
		munmap(sstack->real_ptr, sstack->real_size);
		sstack->real_ptr = NULL;
		sstack->ptr = NULL;
	} else {
		free(sstack->ptr);
		sstack->ptr = NULL;
	}
	free(sstack);
}

void coro_default_exception_func()
{
  fprintf(stderr, "A coroutine illegally called return.");
  fflush(stderr);
  abort();
}

void coro_resume(struct coroutine *to_co)
{
	struct coroutine *current_co = coro_glbl_tls_current_co;
	coro_glbl_tls_current_co = to_co;
	coro_switch(current_co, to_co);
	coro_glbl_tls_current_co = current_co;
}

void coro_yield_to(struct coroutine *to_co)
{
	struct coroutine *current_co = coro_glbl_tls_current_co;
	coro_glbl_tls_current_co = to_co;
	coro_switch(current_co, to_co);
	coro_glbl_tls_current_co = current_co;
}

void coro_yield()
{
	struct coroutine *current_co = coro_glbl_tls_current_co;
	//    struct coroutine *to_co = current_co->main_co;
	struct coroutine *to_co = coro_glbl_tls_main_co;
	coro_glbl_tls_current_co = to_co;
	coro_switch(current_co, to_co);
	coro_glbl_tls_current_co = current_co;
}