#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>

#include <syscall.h>

#include "util/arch.h"
#include "os.h"

namespace util {

Cpu::Cpu()
{
  auto s = new cpu_set_t;
  CPU_ZERO(s);
  os_cpuset = s;
  nr_processors = sysconf(_SC_NPROCESSORS_CONF);
}

Cpu::~Cpu() {}

void Cpu::set_affinity(int cpu)
{
  if (cpu >= nr_processors) {
    fprintf(stderr, "Cannot set processor affinity %d, total number of processors %lu\n",
            cpu, nr_processors);
  }
  CPU_SET(cpu, (cpu_set_t *) os_cpuset);
}

void Cpu::Pin()
{
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), (cpu_set_t *) os_cpuset);
  pthread_yield();
}

OSMemory::OSMemory()
        : mem_map_desc(-1)
{}

size_t OSMemory::AlignLength(size_t length)
{
    if (length >= 2 << 20) {
        // align to 2MB
        length = util::Align(length, 2 << 20);
    } else {
        // align to 4KB
        length = util::Align(length, 4 << 10);
    }
    return length;
}

void *OSMemory::Alloc(size_t length, int numa_node, bool on_demand)
{
    // private anonymous mapping
    int flags = MAP_ANONYMOUS | MAP_PRIVATE;

    // read write protection
    int prot = PROT_READ | PROT_WRITE;
    length = AlignLength(length);

    // allocate huge page if size >= 2MB
    if (length >= 2 << 20) flags |= MAP_HUGETLB;

    void *mem = mmap(nullptr, length, prot, flags, (int) mem_map_desc, 0);
    if (mem == MAP_FAILED)
        return nullptr;

    // bind memory to numa_node if specified
    if (numa_node != -1) BindMemory(mem, length, numa_node);

    // lock page
    if (!on_demand) LockMemory(mem, length);

    return mem;
}

void OSMemory::Free(void *p, size_t length)
{
  length = AlignLength(length);
  munmap(p, length);
}

void OSMemory::LockMemory(void *p, size_t length)
{
    // lock memory pointed to by p of size length in RAM
    if (mlock(p, length) < 0) {
        fprintf(stderr, "WARNING: mlock() failed\n");
        perror("mlock");
        std::abort();
    }
}

void OSMemory::BindMemory(void *p, size_t length, int numa_node)
{
    int nodemask = 1 << numa_node;

    // Sets the NUMA memory policy
    // MPOL_BIND        - Strict policy that restricts memory allocation to the nodes specified in node mask.
    // nodemask         - pointer to a bit mask containing up to maxnode bits
    // maxnode          - Number of bits used in the nodemask. Size of nodemask will be rounded up to the next multiple
    //                    of sizeof(unsigned long)
    // MPOL_MF_STRICT   - The kernel will attempt to move all the existing pages in the memory range so that they follow
    //                    the policy.
    if (syscall(__NR_mbind, p, length, 2 /* MPOL_BIND */, &nodemask, sizeof(unsigned long) * 8,
                1 << 0 /* MPOL_MF_STRICT */) < 0) {
        fprintf(stderr, "Fail to mbind on address %p length %lu numa_node %d\n", p, length, numa_node);
        std::abort();
    }
}

OSMemory OSMemory::g_default;

}
