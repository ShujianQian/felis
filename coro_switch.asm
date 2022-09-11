.global coro_switch

// #if defined(__APPLE)
// #else
.type coro_switch, @function
// #endif

coro_switch:
    mov     (%rsp),     %rcx             // ret addr
    lea     0x8(%rsp),  %rdx             // rsp
    mov     %r12,       (%rdi)
    mov     %r13,       0x8(%rdi)
    mov     %r14,       0x10(%rdi)
    mov     %r15,       0x18(%rdi)
    mov     %rcx,       0x20(%rdi)      // ret addr
    mov     %rdx,       0x28(%rdi)      // rsp
    mov     %rbx,       0x30(%rdi)
    mov     %rbp,       0x38(%rdi)

    mov     (%rsi),     %r12
    mov     0x8(%rsi),  %r13
    mov     0x10(%rsi), %r14
    mov     0x18(%rsi), %r15
    mov     0x20(%rsi), %rax            // ret addr
    mov     0x28(%rsi), %rcx            // rsp
    mov     0x30(%rsi), %rbx
    mov     0x38(%rsi), %rbp

    mov     %rcx,       %rsp
    jmp     *%rax