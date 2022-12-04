//
// Created by Shujian Qian on 2022-08-08
//

#include <gtest/gtest.h>
#include <iostream>

#include "coroutine.h"

TEST(CoroutineSwitchTest, Test)
{
    coro_thread_init(nullptr);
    EXPECT_EQ(true, true);
}

TEST(CoroutineSharedStackCreateTest, Test)
{
    coro_shared_stack *p = coro_create_shared_stack(4096, true, true);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(p->size, 4096);
    EXPECT_EQ(p->real_size, 8192);

    coro_destroy_shared_stack(p);
    EXPECT_EQ(true, true);
}

void coro_test_func()
{
    std::cerr << "hello" << std::endl;
    coro_yield();
    std::cerr << "hello again" << std::endl;
    coro_exit();
}

TEST(CoroutineSwitchYieldTest, Test)
{
    coro_thread_init(nullptr);
    coro_shared_stack *p = coro_create_shared_stack(4096, true, true);
    coroutine * co = coro_create(coro_get_co(), p, coro_test_func, nullptr);
    coro_resume(co);
    std::cerr << "bye!" << std::endl;
    coro_resume(co);
    coro_destroy_shared_stack(p);
    EXPECT_EQ(true, true);
}

void coro_test_func2()
{
    void *x_ptr = coro_get_args();
    intptr_t &x = *((intptr_t *) x_ptr);

    for (int i = 0; i < 100; i++) {
        x ++;
        coro_yield();
    }
    coro_exit();
}

TEST(CoroutineMultipleSwitchTest, Test)
{
    coro_thread_init(nullptr);
    intptr_t x = 0;
    coro_shared_stack *p = coro_create_shared_stack(4096, true, true);
    coroutine * co = coro_create(coro_get_co(), p, coro_test_func2, (void *) &x);
    coro_resume(co);
    for (int i = 0; i < 100; i++)
        coro_resume(co);
    coro_destroy_shared_stack(p);
    EXPECT_EQ(x, 100);
    EXPECT_EQ(co->is_finished, true);
}

void coro_test_func3()
{
    void *co_ptr = coro_get_args();
    struct coroutine *&co = *((coroutine **) co_ptr);

    for (int i = 0; i < 100000; i++) {
        coro_resume(co);
    }
    coro_exit();
}

TEST(CoroutineSymmetricSwitchTest, Test)
{
    coro_thread_init(nullptr);
    coro_shared_stack *p1 = coro_create_shared_stack(4096, true, true);
    coro_shared_stack *p2 = coro_create_shared_stack(4096, true, true);
    coroutine *co1, *co2;
    co1 = coro_create(coro_get_co(), p1, coro_test_func3, (void *) &co2);
    co2 = coro_create(coro_get_co(), p2, coro_test_func3, (void *) &co1);
    coro_resume(co1);
    EXPECT_EQ(co1->is_finished, true);
    coro_destroy_shared_stack(p1);
    coro_resume(co2);
    EXPECT_EQ(co2->is_finished, true);
    coro_destroy_shared_stack(p2);
}


