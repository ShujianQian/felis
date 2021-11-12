#ifndef UTIL_OS_H
#define UTIL_OS_H

#include <cstddef>
#include <cstdint>

namespace util {

class Cpu {
  void *os_cpuset; // OS specific
  size_t nr_processors;
 public:
  Cpu();
  ~Cpu();

  void set_affinity(int cpu);
  void Pin();

  size_t get_nr_processors() const { return nr_processors; }

  // TODO: maybe add NUMA information here?
};

/*!
 * \brief Custom memory allocator.
 */
class OSMemory {
    intptr_t mem_map_desc;    ///< mmap file descriptor. (-1 for anonymous mapping)

    /*!
     * \brief Align size of memory to 2MB or 4KB depending on size.
     * @param length    - Size of memory to align.
     * @return          - Aligned size.
     */
    static size_t AlignLength(size_t length);

public:
    /*!
     * \brief Default constructor.
     */
    OSMemory();
    // TODO: constructor if we want to write to NVM backed file?

    /*!
     * \brief Allocate memory in the NUMA region specified to numa_node.
     * @param length        - Size of memory to allocate.
     * @param numa_node     - NUMA node to bind memory to.
     * @param on_demand     - Whether memory can bw swapped out on demand.
     * @return              - Pointer to the allocated memory. nullptr if allocation failed.
     */
    void *Alloc(size_t length, int numa_node = -1, bool on_demand = false);

    /*!
     * \brief Free the allocated memory.
     * @param p         - Pointer to the allocated memory.
     * @param length    - Size of the allocated memory.
     */
    void Free(void *p, size_t length);

    /*!
     * \brief Bind memory using NUMA policy
     * @param p             - Pointer to memory to bind.
     * @param length        - Size of the memory.
     * @param numa_node     - NUMA node to bind to.
     */
    static void BindMemory(void *p, size_t length, int numa_node);

    /*!
     * \brief Lock virtual memory into RAM.
     * @param p         - Pointer to the memory block.
     * @param length    - Size of the memory block.
     */
    static void LockMemory(void *p, size_t length);

    static OSMemory g_default;    ///< Globally accessible singleton.
};

}

#endif
