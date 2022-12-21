#ifndef NODE_CONFIG_H
#define NODE_CONFIG_H

#include <string>
#include <array>
#include <atomic>
#include <bitset>
#include "util/objects.h"
#include "util/types.h"
#include "util/locks.h"
#include "log.h"
#include "piece.h"

namespace go {
class OutputChannel;
}

namespace felis {

class NodeServerRoutine;
class NodeServerThreadRoutine;
struct PieceRoutine;

class LocalDispatcherImpl;

static constexpr size_t kMaxNrNode = 254;

class TransportBatcher {
 public:
  // Thread local information
  class LocalMetadata {
    friend class TransportBatcher;
    // For each level, we had been accumulating increment to global counters.
    std::array<unsigned long, kMaxNrNode> delta; // for each destinations

    LocalMetadata() {}

    void Init(int nr_nodes) { Reset(nr_nodes); }
    void Reset(int nr_nodes) {
      std::fill(delta.begin(), delta.begin() + nr_nodes, 0);
    }
   public:
    /**
     * Increment the counter for corresponding destination node.
     * @param dst
     */
    void AddRoute(int dst) { delta[dst - 1]++; }
  };
 private:
  static constexpr auto kMaxLevels = PromiseRoutineTransportService::kPromiseMaxLevels;

  // Given a level, how many pieces we should see for each destination node?
  std::array<std::array<std::atomic_ulong, kMaxNrNode>, kMaxLevels> counters;
  std::array<LocalMetadata *, 32> thread_local_data;
  friend class NodeConfiguration;
 public:
  TransportBatcher() {}

  void Init(int nr_nodes, int nr_cores);
  void Reset(int nr_nodes, int nr_cores);
  LocalMetadata &GetLocalData(int level, int core) { return thread_local_data[core][level]; }
  unsigned long Merge(int level, LocalMetadata &local, int node);
};

class LocalTransport : public PromiseRoutineTransportService {
  LocalDispatcherImpl *lb;
 public:
  LocalTransport();
  ~LocalTransport();
  LocalTransport(const LocalTransport &rhs) = delete;

  /**
   * Transport a PieceRoutine locally. Used by TcpNodeTransport to transport local PieceRoutines.
   * Add the PieceRoutine to the LocalDispatcher's queue and potentially flush and submit the PieceRoutines to the
   * scheduler's queue.
   * @param routine
   */
  void TransportPromiseRoutine(PieceRoutine *routine) final override;
  /**
   * Flush the PieceRoutines from the LocalDispatcher's queues to the scheduler's queues and spawn ExecutionRoutines to
   * execute those PieceRoutines (and poll from network).
   */
  void Flush();
  bool TryFlushForCore(int core_id);
};

class IncomingTraffic {
 protected:
  static constexpr int kTotalStates = 3;
  std::atomic_ulong state = kTotalStates - 1;
  int src_node_id = 0;
 public:
  enum class Status {
    PollMappingTable, PollRoutines, EndOfPhase,
  };
  void AdvanceStatus() {
    auto old_state = state.fetch_add(1);
    logger->info("{} {} Incoming traffic status changed {} -> {}",
                 (void *) this, src_node_id, old_state % kTotalStates, (old_state + 1) % kTotalStates);
  }
  Status current_status() const {
    static constexpr Status all_status[] = {
      Status::PollMappingTable, Status::PollRoutines, Status::EndOfPhase,
    };
    return all_status[state.load() % kTotalStates];
  }
};

class OutgoingTraffic {
 protected:
  int dst_node;
 public:
  virtual void WriteToNetwork(void *data, size_t cnt) = 0;
  virtual void DoFlush(bool async) = 0;
};

class NodeConfiguration {
  NodeConfiguration();

  template <typename T> friend struct util::InstanceInit;
  int id;
 public:
  /**
   * How many threads are available on a machine.
   */
  static size_t g_nr_threads;
  static constexpr size_t kMaxNrThreads = 32;
  static bool g_data_migration;

  struct NodePeerConfig {
    std::string host;
    uint16_t port;
  };

  struct NodeConfig {
    int id;
    std::string name;
    NodePeerConfig worker_peer;
    NodePeerConfig index_shipper_peer;
    NodePeerConfig row_shipper_peer;
  };

  int node_id() const { return id; }
  void SetupNodeName(std::string name);

  const NodeConfig &config(int idx = -1) const {
    if (idx == -1) idx = id;
    abort_if(!all_config[idx],
             "configuration for node {} does not exist!", idx);
    return all_config[idx].value();
  }

  /**
   * Clears local and global batch counters.
   */
  void ResetBufferPlan();
  /**
   * Given a PieceCollection, update the counter for each of the PieceRoutine in the PieceColleciton.
   * @param root
   * @param cnts
   */
  void CollectBufferPlan(BasePieceCollection *root, unsigned long *cnts);
  /**
   * Flush the per-core counter after updating all PieceRoutines on this core
   * @param per_core_cnts   This core's buffer plan counter.
   * @return                Whether this core flushed counter to the network.
   */
  bool FlushBufferPlan(unsigned long *per_core_cnts);
  /**
   * Broadcasts start phase message to all other nodes.
   */
  void SendStartPhase();
  /**
   * Advances the state of each IncomingTraffic
   */
  void ContinueInboundPhase();
  void CloseAndShutdown();

  // node id starts from 1
  int nr_nodes() const { return max_node_id; }

  std::atomic_ulong *local_buffer_plan_counters() const {
    return local_batch->counters;
  };

  std::array<util::Optional<NodeConfig>, kMaxNrNode> all_configurations() const {
    return all_config;
  }

  TransportBatcher &batcher() { return transport_batcher; }

  /**
   * Calculates the index of the counter a PieceRoutine corresponds to inside the counter array.
   * @param level
   * @param src_node
   * @param dst_node
   * @return
   */
  size_t BatchBufferIndex(int level, int src_node, int dst_node);
  std::atomic_ulong &TotalBatchCounter(int idx) { return total_batch_counters[idx]; }

  void RegisterOutgoing(int idx, OutgoingTraffic *t) {
    outgoing[idx] = t;
  }
  void RegisterIncoming(int idx, IncomingTraffic *t) {
    incoming[idx] = t;
  }

  int UpdateBatchCountersFromReceiver(unsigned long *data);
  size_t CalculateIncomingFromNode(int src);

 private:
  std::array<util::Optional<NodeConfig>, kMaxNrNode> all_config;
  size_t max_node_id;
  std::array<OutgoingTraffic *, kMaxNrNode> outgoing;
  std::array<IncomingTraffic *, kMaxNrNode> incoming;

  TransportBatcher transport_batcher;

  // The BufferRootPromise is going to run an analysis on the root promise to
  // keep track of how many handlers needs to be sent.
  //
  // The counters should be in the format of
  // channel_batch_counters[level][src][dst], where src and dst are the node
  // number - 1.
  std::atomic_ulong *total_batch_counters;
  struct LocalBatch {
    unsigned long magic;
    unsigned long node_id;
    std::atomic_ulong counters[];
  } *local_batch;
  std::atomic_ulong local_batch_completed;
 private:

  /**
   * Update the counter of PieceRoutines given a PieceRoutine to run.
   * @param routine
   * @param cnts
   * @param level
   * @param src_node
   */
  void CollectBufferPlanImpl(PieceRoutine *routine, unsigned long *cnts, int level, int src, int future_src_node = 0);
};

template <typename T>
class Flushable {
 protected:
 private:
  T *self() { return (T *) this; }

 public:

  std::tuple<bool, bool> TryFlushForThread(int i) {
    if (!self()->TryLock(i)) return std::make_tuple(false, false);
    auto [start, end] = self()->GetFlushRange(i);
    self()->UpdateFlushStart(i, end);
    return std::make_tuple(true, self()->PushRelease(i, start, end));
  }

  void Flush() {
    std::bitset<NodeConfiguration::kMaxNrThreads + 1> flushed;
    bool need_do_flush = false;
    // Also flush the main go-routine
    auto nr_threads = NodeConfiguration::g_nr_threads + 1;

    while (flushed.count() < nr_threads) {
      int i = 0;
      for (auto i = 0; i < nr_threads; i++) {
        if (flushed[i]) continue;
        auto [success, did_flush] = TryFlushForThread(i);
        if (success) {
          if (did_flush) need_do_flush = true;
          flushed.set(i);
        }
      }
    }
    if (need_do_flush)
      self()->DoFlush();
  }
};

class LocalDispatcherImpl : public Flushable<LocalDispatcherImpl> {
  static constexpr size_t kBufferSize = 16383;
  struct Queue {
    // Putting these per-core task buffer simply because it's too large and we
    // can't put them on the stack!
    struct {
      std::array<PieceRoutine *, kBufferSize> routines;
      size_t nr;
    } task_buffer[NodeConfiguration::kMaxNrThreads];

    std::array<PieceRoutine *, kBufferSize> routines;
    std::atomic_uint append_start = 0;
    unsigned int flusher_start = 0;
    std::atomic_bool need_scan = false;
    util::SpinLock lock;
  };

  std::array<Queue *, NodeConfiguration::kMaxNrThreads + 1> queues;
  std::atomic_ulong dice;
  int idx;

 public:
  LocalDispatcherImpl(int idx);
  /**
   * Add a PieceRoutine to the local dispatcher's queue. Potentially flush the dispatcher to submit to the scheduler.
   * @param routine
   */
  void QueueRoutine(PieceRoutine *routine);

  std::tuple<uint, uint> GetFlushRange(int tid) {
    return {
      queues[tid]->flusher_start,
      queues[tid]->append_start.load(std::memory_order_acquire),
    };
  }
  void UpdateFlushStart(int tid, unsigned int flush_start) {
    queues[tid]->flusher_start = flush_start;
  }

  /**
   * Submit the PieceRoutines from the LocalDispatcher's Queues to the scheduler's queues (under
   * EpochExecutionDispatchService)
   * @param tid
   * @param start
   * @param end
   * @return
   */
  bool PushRelease(int tid, unsigned int start, unsigned int end);
  /**
   * If there's no ExecutionRoutine running to run the PieceRoutines in the local scheduler's queues, create one on that
   * core.
   */
  void DoFlush();

  bool TryLock(int i) {
    return queues[i]->lock.TryLock();
  }
  void Unlock(int i) {
    queues[i]->lock.Unlock();
  }

 private:
  /**
   * Submit the PieceRoutines from the LocalDispatcher's queues to the scheduler's queues
   * @param thread
   * @param start
   * @param end
   */
  void FlushOnCore(int thread, unsigned int start, unsigned int end);
  /**
   * Submits a number of PieceRoutines and add them to the core's scheduling queue.
   * @param routines
   * @param start
   * @param end
   * @param thread
   */
  void SubmitOnCore(PieceRoutine **routines, unsigned int start, unsigned int end, int thread);
};

}

namespace util {

template <>
struct InstanceInit<felis::NodeConfiguration> {
  static constexpr bool kHasInstance = true;
  static inline felis::NodeConfiguration *instance;
  InstanceInit() {
    instance = new felis::NodeConfiguration();
  }
};

}

#endif /* NODE_CONFIG_H */
