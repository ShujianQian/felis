// -*- c++ -*-

#ifndef UTIL_H
#define UTIL_H

#include <string>
#include <cassert>
#include <atomic>
#include <memory>
#include <unistd.h>
#include <sched.h>
#include <pthread.h>
#include <sys/types.h>

#include <optional>

#include "gopp/gopp.h"

#define CACHE_ALIGNED __attribute__((aligned(CACHE_LINE_SIZE)))
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define __XCONCAT2(a, b) a ## b
#define CACHE_PADOUT                                                    \
  char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHE_LINE_SIZE)))

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif

#ifndef container_of
#define container_of(ptr, type, member) ({			\
      (type *)((char *)ptr - offsetof(type, member)); })
#endif

namespace util {

// padded, aligned primitives
template <typename T, bool Pedantic = true>
class CacheAligned {
 public:

  template <class... Args>
  CacheAligned(Args &&... args)
      : elem(std::forward<Args>(args)...)
  {
    if (Pedantic)
      assert(((uintptr_t)this % CACHE_LINE_SIZE) == 0);
  }

  T elem;
  CACHE_PADOUT;

  // syntactic sugar- can treat like a pointer
  inline T & operator*() { return elem; }
  inline const T & operator*() const { return elem; }
  inline T * operator->() { return &elem; }
  inline const T * operator->() const { return &elem; }

 private:
  inline void
  __cl_asserter() const
  {
    static_assert((sizeof(*this) % CACHE_LINE_SIZE) == 0, "xx");
  }
} CACHE_ALIGNED;

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class FastRandom {
 public:
  FastRandom(unsigned long seed)
      : seed(0)
  {
    set_seed0(seed);
  }

  unsigned long next() {
    return ((unsigned long) next(32) << 32) + next(32);
  }

  uint32_t next_u32() {
    return next(32);
  }

  uint16_t next_u16() {
    return next(16);
  }

  /** [0.0, 1.0) */
  double next_uniform() {
    return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
  }

  char next_char() {
    return next(8) % 256;
  }

  char next_readable_char() {
    static const char readables[] = "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_char();
    return s;
  }

  std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_readable_char();
    return s;
  }

  unsigned long get_seed() {
    return seed;
  }

  void set_seed(unsigned long seed) {
    this->seed = seed;
  }

 private:
  void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long) (seed >> (48 - bits));
  }

  unsigned long seed;
};

template <typename T>
class XORRandom {
  T state;
 public:
  XORRandom(T state) : state(state) {}
  T Next() {
    auto x = state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    state = x;
    return x;
  }

  T NextRange(T begin, T end) {
    return Next() % (end - begin) + begin;
  }
};

class XORRandom32 : public XORRandom<uint32_t> {
 public:
  XORRandom32() : XORRandom<uint32_t>(0x25F16D1D) {}
};

class XORRandom64 : public XORRandom<uint64_t> {
 public:
  XORRandom64() : XORRandom<uint64_t>(0x2545F4914F6CDD1D) {}
};

// link list headers. STL is too slow
template <typename T>
struct GenericListNode {
  GenericListNode<T> *prev, *next;

  void InsertAfter(GenericListNode<T> *parent) {
    prev = parent;
    next = parent->next;
    parent->next->prev = this;
    parent->next = this;
  }

  void Remove() {
    prev->next = next;
    next->prev = prev;
    prev = next = nullptr; // detached
  }

  bool is_detached() {
    return prev == next && next == nullptr;
  }

  void Initialize() {
    prev = next = this;
  }

  bool empty() const { return next == this; }

  T *object() { return static_cast<T *>(this); }
};

using ListNode = GenericListNode<void>;

// Type Safe Embeded LinkListNode. E is a enum, serves as a variable name.
template <typename T, int E> struct TypedListNode;

template <typename T>
struct TypedListNodeWrapper {
  template <int E>
  static TypedListNode<T, E> *ToListNode(T *obj) {
    return obj;
  }
};

template <typename T, int E>
struct TypedListNode : public TypedListNodeWrapper<T>,
                       public GenericListNode<TypedListNode<T, E>> {
  T *object() { return static_cast<T *>(this); }
};

// Locks

class SpinLock {
  std::atomic_bool lock = false;
 public:
  void Lock() {
    bool locked = false;
    while (!lock.compare_exchange_strong(locked, true)) {
      locked = false;
      _mm_pause();
    }
  }
  void Acquire() { Lock(); }

  bool TryLock() {
    bool locked = false;
    return lock.compare_exchange_strong(locked, true);
  }

  void Unlock() {
    lock.store(false);
  }
  void Release() { Unlock(); }
};

class MCSSpinLock {
 public:
  struct QNode {
    std::atomic_bool done = false;
    std::atomic<QNode *> next = nullptr;
  };
 private:
  std::atomic<QNode *> tail = nullptr;
 public:
  bool IsLocked() { return tail.load() != nullptr; }
  bool TryLock(QNode *qnode) {
    QNode *old = nullptr;
    return tail.compare_exchange_strong(old, qnode);
  }

  void Lock(QNode *qnode) {
    auto last = tail.exchange(qnode);
    if (last) {
      last->next = qnode;
      while (!qnode->done) _mm_pause();
    }
  }
  void Acquire(QNode *qnode) { Lock(qnode); }

  void Unlock(QNode *qnode) {
    if (!qnode->next) {
      auto owner = qnode;
      if (tail.compare_exchange_strong(owner, nullptr)) return;
      while (!qnode->next.load()) _mm_pause();
    }
    qnode->next.load()->done = true;
  }
  void Release(QNode *qnode) { Unlock(qnode); }
};

template <typename T>
class Guard {
  T &t;
 public:
  Guard(T &t) : t(t) { t.Acquire(); }
  ~Guard() { t.Release(); }
};

template <class... M>
class MixIn : public M... {};

// instance of a global object. So that we don't need the ugly extern.
template <class O> O &Instance() noexcept;

template <class O>
struct InstanceInit {
  static constexpr bool kHasInstance = false;
  InstanceInit() {
    Instance<O>();
  }
};

template <class O> O &Instance() noexcept
{
  if constexpr (InstanceInit<O>::kHasInstance) {
    return *(InstanceInit<O>::instance);
  } else {
    static O o;
    return o;
  }
}

// Prefetch a lot of pointers
template <typename Iterator>
static inline void Prefetch(Iterator begin, Iterator end)
{
  for (auto p = begin; p != end; ++p) {
    __builtin_prefetch(*p);
  }
}

static inline void Prefetch(std::initializer_list<void *> arg)
{
  Prefetch(arg.begin(), arg.end());
}

// Interface implementation. The real implementation usually is in iface.cc
template <class IFace> IFace &Impl() noexcept;

#define IMPL(IFace, ImplClass) \
  template<> IFace &Impl() noexcept { return Instance<ImplClass>(); }

template <typename T, typename ...Args>
class BaseFactory {
 public:
  // Don't use std::function. Complation speed is very slow.
  struct Callable { virtual T *operator()(Args...) = 0; };
  template <typename F> struct GenericCallable : public Callable {
    F f;
    GenericCallable(F f) : f(f) {}
    T *operator()(Args... args) override final {
      return f(args...);
    }
  };
  typedef std::vector<Callable *> Table;
 protected:
  static Table table;
  template <typename F>
  static void AddToTable(F f) {
    table.push_back(new GenericCallable<F>(f));
  }
 public:
  static T *Create(int n, Args... args) {
    return (*table[n])(args...);
  }
};

template <typename T, typename ...Args>
typename BaseFactory<T, Args...>::Table BaseFactory<T, Args...>::table;

template <typename T, int LastEnum, typename ...Args>
class Factory : public Factory<T, LastEnum - 1, Args...> {
  typedef Factory<T, LastEnum - 1, Args...> Super;
 public:
  static void Initialize() {
    Super::Initialize();
    Super::AddToTable([](Args... args) {
        return Factory<T, LastEnum - 1, Args...>::Construct(args...);
      });
  }
  static T *Construct(Args ...args);
};

template <typename T, typename ...Args>
class Factory<T, 0, Args...> : public BaseFactory<T, Args...> {
 public:
  static void Initialize() {}
  static T *Construct(Args ...args);
};

// CPU pinning
static inline void PinToCPU(std::vector<int> cpus)
{
  // linux only
  cpu_set_t set;
  CPU_ZERO(&set);
  for (auto cpu : cpus) {
    CPU_SET(cpu % sysconf(_SC_NPROCESSORS_CONF), &set);
  }
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);
  pthread_yield();
}

static inline void PinToCPU(int cpu) { PinToCPU(std::vector<int>{cpu}); }

static inline size_t Align(size_t x, size_t a = 16)
{
  return a * ((x - 1) / a + 1);
}

// Typesafe get from a variadic arguments
template <int Index, typename U, typename ...Args>
struct GetArg : public GetArg<Index - 1, Args...> {
  GetArg(const U &first, const Args&... rest) : GetArg<Index - 1, Args...>(rest...) {}
};

template <typename U, typename ...Args>
struct GetArg<0, U, Args...> {
  U value;
  GetArg(const U &value, const Args&... drop) : value(value) {}
};

template <typename ValueType> using Optional = std::optional<ValueType>;
template <typename ValueType> using Ref = std::reference_wrapper<ValueType>;
template <typename ValueType> using OwnPtr = std::unique_ptr<ValueType>;


static inline uint64_t *FastLowerBound(uint64_t *start, uint64_t *end, uint64_t value)
{
  unsigned int len = end - start;
  if (len == 0) return start;

  unsigned int maxstep = 1 << (31 - __builtin_clz(len));
  unsigned int ret = (maxstep < len && start[maxstep] <= value) * (len - maxstep);

  __builtin_prefetch(end - 16);

  // printf("len %u middle %u ret %u\n", len, middle, ret);
  for (auto x = 11; x >= 0; x--) {
    auto stepping = 1 << x;
    if (stepping < maxstep) ret += (start[ret + stepping] <= value) * stepping;
  }
  return start[ret] <= value ? start + ret + 1 : start + ret;
}

}

#endif /* UTIL_H */
