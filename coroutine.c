//
// Created by Shujian Qian on 2022-08-01
//

#include "coroutine.h"

#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>

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
    if ((void *) exception_func != NULL)
        coro_glbl_tls_fptr = exception_func;

    if (coro_glbl_tls_main_co == NULL) {
        struct coroutine *co = (struct coroutine *) malloc(sizeof(struct coroutine));
        memset(co, 0, sizeof(struct coroutine));
        coro_glbl_tls_current_co = co;
        coro_glbl_tls_main_co = co;
    }
}

struct coroutine *coro_create(
        struct coroutine *main_co, struct coro_shared_stack *shared_stack,
        coro_func_t coro_func, void *args
)
{
    struct coroutine *co = (struct coroutine *) malloc(sizeof(struct coroutine));
    memset(co, 0, sizeof(struct coroutine));

    if (main_co != NULL) {
        // this is a non-main coroutine
        assert(shared_stack != NULL);
        co->shared_stack = shared_stack;
        co->reg[CORO_RET_ADDR_IDX] = (void *) coro_func;
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

struct coro_shared_stack *coro_create_shared_stack(size_t size, bool enable_guard_page)
{
    if (size == 0)
        size = 1 << 22;
    if (size < 4096)
        size = 4096;
    assert(size >= 4096);

    size_t page_size = 0;
    if (enable_guard_page) {
        long signed_page_size = sysconf(_SC_PAGESIZE);
        assert(signed_page_size > 0);
        assert(((signed_page_size - 1) & signed_page_size) == 0);
        page_size = (size_t) ((unsigned long) signed_page_size);
        assert(page_size == (unsigned long) page_size);
        // check overflow
        assert((page_size << 1) >> 1 == page_size);

        if (size < page_size) {
            size = page_size * 2;
        } else {
            size_t aligned_size;
            if ((size & (page_size - 1)) != 0) {
                // align sstack size to multiple of pagesize
                aligned_size = (size & (~(page_size - 1)));
                assert(aligned_size + page_size * 2 > aligned_size);
                aligned_size += page_size * 2;
                assert(aligned_size / page_size == size / page_size + 2);
            } else {
                aligned_size = size;
                assert(aligned_size + page_size > aligned_size);
                aligned_size += page_size; // protection page
                assert(aligned_size / page_size == size / page_size + 1);
            }
            size = aligned_size;
            assert(size / page_size > 1);
            assert((size & (page_size - 1)) == 0);
        }
    }

    struct coro_shared_stack *sstack = (struct coro_shared_stack *) malloc(sizeof(struct coro_shared_stack));
    memset(sstack, 0, sizeof(*sstack));

    if (enable_guard_page) {
        sstack->guard_page_enabled = true;
        sstack->real_ptr = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        sstack->real_size = size;
        sstack->ptr = (void *) (((uintptr_t)sstack->real_ptr) + page_size);
        sstack->size = size - page_size;
    } else {
        sstack->size = size;
        sstack->ptr  = malloc(size);
    }

    sstack->owner = NULL;
    uintptr_t aligned_ptr = (uintptr_t)(sstack->size - sizeof(void *) * 2 + (uintptr_t) sstack->ptr);
    aligned_ptr = (aligned_ptr >> 4) << 4;  // aligned to 16 bytes
    sstack->aligned_highptr = (void *) aligned_ptr;
    sstack->aligned_ret_ptr = (void *) (aligned_ptr - sizeof(void *));
    *((void **) (sstack->aligned_ret_ptr)) = (void *)(coro_default_exception_func);
    sstack->aligned_limit = sstack->size - sizeof(void *) * 2 - 16;  // max cost of alignment

    return sstack;
}

void coro_destroy_shared_stack(struct coro_shared_stack *sstack)
{
    assert(sstack != NULL);
    assert(sstack->ptr != NULL);
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
    return;
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