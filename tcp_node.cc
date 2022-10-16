#include "tcp_node.h"

#include "shipping.h"
#include "slice.h"
#include "console.h"
#include "txn.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "epoch.h"
#include "log.h"
#include <iostream>
#include "txn_cc.h"

namespace felis {
namespace tcp {

class SendChannel : public Flushable<SendChannel>, public OutgoingTraffic {
  go::TcpOutputChannel *out;
  go::BufferChannel *flusher_channel;

  struct Channel {
    uint8_t *mem;
    unsigned int flusher_start;
    std::atomic_uint append_start;
    std::atomic_bool lock;
    std::atomic_bool dirty;
  };

  // We need to create a per-thread, long-running flusher go::Routine.
  class FlusherRoutine : public go::Routine {
    go::BufferChannel *flusher_channel;
    go::TcpOutputChannel *out;
   public:
    FlusherRoutine(go::BufferChannel *chn, go::TcpOutputChannel *out)
        : flusher_channel(chn), out(out) {
      // set_urgent(true);
    }
    void Run() final override;
  };

  util::CacheAligned<Channel> channels[NodeConfiguration::kMaxNrThreads + 1];

 public:
  static constexpr size_t kPerThreadBuffer = 16 << 10;
  SendChannel(go::TcpSocket *sock, int dst_node);
  void *Alloc(size_t sz);
  void Finish(size_t sz);
  long PendingFlush(int core_id);

  std::tuple<unsigned int, unsigned int> GetFlushRange(int tid) {
    return {
      channels[tid].flusher_start,
      channels[tid].append_start.load(std::memory_order_acquire),
    };
  }
  void UpdateFlushStart(int tid, unsigned int flush_start) {
    channels[tid].flusher_start = flush_start;
  }
  bool PushRelease(int thr, unsigned int start, unsigned int end);
  void DoFlush(bool async = false) final override;
  bool TryLock(int i) {
    bool locked = false;
    return channels[i].lock.compare_exchange_strong(locked, true);
  }
  void Unlock(int i) {
    channels[i].lock.store(false);
  }

  void WriteToNetwork(void *data, size_t cnt) final override {
    out->Write(data, cnt);
    out->Flush(true);
  }
};

SendChannel::SendChannel(go::TcpSocket *sock, int dst_node)
    : out(sock->output_channel())
{
  this->dst_node = dst_node;
  auto buffer =
      (uint8_t *) malloc((NodeConfiguration::g_nr_threads + 1) * kPerThreadBuffer);
  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    auto &chn = channels[i];
    chn.mem = buffer + i * kPerThreadBuffer;
    chn.append_start = 0;
    chn.flusher_start = 0;
    chn.lock = false;
    chn.dirty = false;
  }
  flusher_channel = new go::BufferChannel(512);
  go::GetSchedulerFromPool(0)->WakeUp(new FlusherRoutine(flusher_channel, out));
}

void *SendChannel::Alloc(size_t sz)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  abort_if(tid < 0, "Have to call this within a go-routine");

  auto &chn = channels[tid];
retry:
  auto end = chn.append_start.load(std::memory_order_relaxed);
  if (end + sz >= kPerThreadBuffer) {
    while (!TryLock(tid)) _mm_pause();
    auto start = chn.flusher_start;
    chn.flusher_start = 0;
    chn.append_start.store(0, std::memory_order_release);
    PushRelease(tid, start, end);
    goto retry;
  }
  auto ptr = chn.mem + end;
  return ptr;
}

void SendChannel::Finish(size_t sz)
{
  int tid = go::Scheduler::CurrentThreadPoolId();
  auto &chn = channels[tid];
  chn.append_start.store(chn.append_start.load(std::memory_order_relaxed) + sz,
                          std::memory_order_release);
}

bool SendChannel::PushRelease(int tid, unsigned int start, unsigned int end)
{
  
  auto mem = channels[tid].mem;
  if (end - start > 0) {
    void *buf = alloca(end - start);
    memcpy(buf, mem + start, end - start);
    channels[tid].dirty.store(true, std::memory_order_release);
    Unlock(tid);
    WriteToNetwork(buf, end - start);
    return true;
  } else {
    Unlock(tid);
    return channels[tid].dirty.load();
  }
}

void SendChannel::DoFlush(bool async)
{
  if (async) {
    out->Flush(true);
    return;
  }

  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto &chn = channels[core_id];

  uint8_t signal = 0;
  logger->info("SendChannel signaling flusher {}", dst_node);
  flusher_channel->Write(&signal, 1);

  for (int i = 0; i <= NodeConfiguration::g_nr_threads; i++) {
    channels[i].dirty = false;
  }
}

void SendChannel::FlusherRoutine::Run()
{
  while (true) {
    uint8_t signal = 0;
    flusher_channel->Read(&signal, 1);
    out->Flush();
    logger->info("FlusherRoutine done flushing SendChannel, next round");
  }
}

long SendChannel::PendingFlush(int core_id)
{
  // return channels[core_id]->flusher_cnt;
  return 0;
}

class NodeRowShipmentReceiverRoutine : public go::Routine {
  std::string host;
  unsigned short port;
 public:
  NodeRowShipmentReceiverRoutine(std::string host, unsigned short port) : host(host), port(port) {}

  void Run() final override;
};

void NodeRowShipmentReceiverRoutine::Run()
{
  go::TcpSocket *server = new go::TcpSocket(8192, 1024);
  logger->info("Row Shipment receiving thread listening on {}:{}", host, port);
  server->Bind(host, port);
  server->Listen();

  while (true) {
    auto *client_sock = server->Accept(8192, 1024);
    auto receiver = new RowShipmentReceiver(client_sock);
    go::Scheduler::Current()->WakeUp(receiver);
  }
}

class ReceiverChannel : public IncomingTraffic {
  friend class felis::TcpNodeTransport;
  static constexpr auto kMaxMappingTableBuffer = 1024;
  go::TcpInputChannel *in;
  // We don't use the tcp socket lock, we use our own lock
  std::atomic_bool lock;
  felis::TcpNodeTransport *transport;
  std::atomic_long nr_left;
  bool warned_during_poll = false;
 public:
  ReceiverChannel(go::TcpSocket *sock, felis::TcpNodeTransport *transport)
      : IncomingTraffic(), in(sock->input_channel()), transport(transport) {
    sock->OmitReadLock();
    sock->OmitWriteLock();
    lock = false;
    nr_left = 0;
  }

  size_t Poll(PieceRoutine **routines, size_t cnt);
 private:
  bool TryLock() {
    bool old = false;
    return lock.compare_exchange_strong(old, true);
  }
  void Unlock() {
    lock = false;
  }
  void Reset() {
    long expect = 0;
    if (!nr_left.compare_exchange_strong(expect, EpochClient::kMaxPiecesPerPhase)) {
      logger->info("Reset() failed, nr_left is {}", expect);
      std::abort();
    }
    warned_during_poll = false;
  }
  size_t PollRoutines(PieceRoutine **routines, size_t cnt);
  bool PollMappingTable();
  void Complete(size_t n);
};

void ReceiverChannel::Complete(size_t n)
{
  if (n == 0) return;
  int64_t left = nr_left.fetch_sub(n) - n;
  abort_if(left < 0, "left {} < 0!", left);
  if (left == 0) {
    logger->info("{} Complete() last n={}", (void *) this, n);
    AdvanceStatus();
    abort_if(current_status() != Status::SyncForEnd, "Bogus current state! {}", (int) current_status());
  }
}

size_t ReceiverChannel::Poll(PieceRoutine **routines, size_t cnt)
{
  bool keep_polling = false;
  size_t nr = 0;

  if (current_status() == Status::EndOfPhase
      || !TryLock())
    return 0;

  in->BeginPeek();
  do {
    auto s = current_status();
    switch (s) {
      case Status::PollMappingTable:
        keep_polling = PollMappingTable();
        break;
      case Status::PollRoutines:
        nr = PollRoutines(routines, cnt);
        keep_polling = false;
        break;
      case Status::SyncForEnd:
      case Status::EndOfPhase:
        break;
    }
  } while (keep_polling);
  in->EndPeek();

  Unlock();
  return nr;
}

size_t ReceiverChannel::PollRoutines(PieceRoutine **routines, size_t cnt)
{
  auto &svc = util::Impl<PromiseRoutineDispatchService>();
  int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t header;
  size_t i = 0; //counter for pieces processed
  size_t routines_processed = 0; //count for how many routines we process
  size_t futures_processed = 0; //count how many futures we processed
  while (i < cnt) { //only process so long as piece buffer has space
    if (in->Peek(&header, 8) < 8) //stop early if buffer empty
      break;

    if (((header >> 56) & 0xFF) == 0xFF) {
      /*
      if (!warned_during_poll) {
        logger->info("WARNING: Next phase comming up from {} nr_left {} i {}...",
                     src_node_id, nr_left, i);
        warned_during_poll = true;
      }
      */
      break;
    } else if (header == PieceRoutine::kUpdateBatchCounter) {
      auto &conf = util::Instance<NodeConfiguration>();
      constexpr auto max_level = PromiseRoutineTransportService::kPromiseMaxLevels;
      auto nr_nodes = conf.nr_nodes();
      auto buffer_size = 8 + max_level * nr_nodes * nr_nodes * sizeof(ulong);
      auto buflen = 8 + buffer_size;
      auto buf = (uint8_t *) alloca(buflen);

      if (in->Peek(buf, buflen) < buflen) {
        break;
      }

      src_node_id = util::Instance<NodeConfiguration>().
                    UpdateBatchCountersFromReceiver((unsigned long *) (buf + 8));
      in->Skip(buflen);

      transport->OnCounterReceived();
      //TODO: put the magic number somewhere consistent
    } else if ((header & 0xFFFF000000000000) == ((uint64_t)2<<55) ){ // Lets use the upper 2 bytes of the header as a flag
      
      header -= ((uint64_t)2<<55);
      auto buflen = 8 + header;
      auto buf = (uint8_t *) alloca(buflen);
      if (in->Peek(buf, buflen) < buflen) {
        break;
      }
      uint64_t epoch_nr;
      uint64_t offset;
      int node_id;
      int affinity;
      memcpy(&epoch_nr, buf+8, 8);
      memcpy(&offset, buf+16, 8);
      memcpy(&node_id, buf+24, 4);
      memcpy(&affinity, buf+28, 4);
      // Get the pointer to the local future value object
      BaseFutureValue* localFuture = (BaseFutureValue *) util::Instance<EpochManager>().ptr(epoch_nr,node_id,offset);

      ((FutureValue<int32_t> *)localFuture)->DecodeFrom(buf+32); //TODO, don't assume type, decode from should be a virtual function
      localFuture->SignalRemote(affinity); //signal, pass the affinity since we need an extra trigger piece

      //logger->info("recieved {}{}{} with affinity {} aka {}",epoch_nr, offset, node_id, affinity, (uint64_t) localFuture);

      ///**
      //zero queue signalling entry creation. 
      auto signal_routine = PieceRoutine::CreateFromCapture(0);
      signal_routine->remote_flag = 1; //simply having this non-zero indicates a fake signalling piece
      signal_routine->affinity = affinity;
      signal_routine->fv_signals = 1; //just to tell this is a remote piece

      //capture_data is actually just the identifier for our FV object
      signal_routine->sched_key = 0;
      signal_routine->capture_data = (uint8_t *) localFuture; // this is a hack, we're just using the future value pointer as a unique identifier
      routines[i++] = signal_routine;
      //**/

      futures_processed++;
      in->Skip(buflen);
      //by counting futures alongside pieces, we consider them both tasks
      //because of this, we need to count every time we recieve a Future
      //TODO: modify to allow batching at end of loop
      svc.Complete(core_id);

    } else {
      abort_if(header % 8 != 0, "header isn't aligned {}", header);
      auto buflen = 8 + header;
      auto buf = (uint8_t *) alloca(buflen);
      if (in->Peek(buf, buflen) < buflen)
        break;
      routines[i++] = PieceRoutine::CreateFromPacket(buf + 8, header);
      routines_processed++;
      in->Skip(buflen);
    }
  }
  // Counter for incoming futures is piggybacked alongside the counter for incoming pieces 
  Complete(routines_processed + futures_processed); 
  //logger->info("recieved {} pieces and {} futures",i,future_processed);
  return i;
}

bool ReceiverChannel::PollMappingTable()
{
  uint64_t header;
  if (in->Peek(&header, 8) < 8)
    return false;
  abort_if(((header >> 56) & 0xFF) != 0xFF,
           "header isn't right for mappingtable update 0x{:x}", header);
  unsigned int nr_ops = header & std::numeric_limits<int32_t>::max();
  auto len = 4 + 4 * nr_ops;
  auto buflen = 8 + len;
  auto buf = (uint8_t *) alloca(buflen);
  abort_if(buflen > kMaxMappingTableBuffer,
           "MappingTable request is {}, larger than maximum {}",
           buflen, kMaxMappingTableBuffer);

  if (in->Peek(buf, buflen) < buflen)
    return false;

  Reset();
  auto data = (uint32_t *) (buf + 8);
  src_node_id = data[0];
  util::Instance<SliceMappingTable>()
      .UpdateSliceMappingTablesFromReceiver(nr_ops, data + 1);

  logger->info("Mapping table from {} applied, buflen {}", src_node_id, buflen);
  AdvanceStatus();

  in->Skip(buflen);

  return true;
}

class NodeServerRoutine : public go::Routine {
  friend class felis::TcpNodeTransport;
  felis::TcpNodeTransport *transport;
 public:
  NodeServerRoutine(felis::TcpNodeTransport *transport) : transport(transport) {}
  virtual void Run() final;
};

void NodeServerRoutine::Run()
{
  auto &console = util::Instance<Console>();

  auto server_sock = new go::TcpSocket(1024, 1024);
  auto &conf = util::Instance<NodeConfiguration>();
  auto &node_conf = conf.config();

  auto nr_nodes = conf.nr_nodes();

  // Reuse addr just for debugging
  int enable = 1;
  setsockopt(server_sock->fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

  abort_if(!server_sock->Bind("0.0.0.0", node_conf.worker_peer.port),
           "Cannot bind peer address");
  abort_if(!server_sock->Listen(kMaxNrNode),
           "Cannot listen");
  console.WaitForServerStatus(Console::ServerStatus::Connecting);
  // Now if anybody else tries to connect to us, it should be in the listener
  // queue. We are safe to call connect at this point. It shouldn't lead to
  // deadlock.
  for (auto &config: conf.all_configurations()) {
    if (!config) continue;
    util::Instance<SliceMappingTable>().InitNode(config->id);
    if (config->id == conf.node_id()) {
      util::Instance<SliceMappingTable>().AddEntry(100 + config->id, IndexOwner, config->id);
      continue;
    }

    logger->info("Connecting worker peer on node {}", config->id);
    go::TcpSocket *remote_sock = new go::TcpSocket(1024, 512 << 20);
    auto &peer = config->worker_peer;
    bool rs = remote_sock->Connect(peer.host, peer.port);
    abort_if(!rs, "Cannot connect to {}:{}", peer.host, peer.port);
    transport->outgoing_socks[config->id] = remote_sock;
    transport->outgoing_channels[config->id] = new SendChannel(remote_sock, config->id);
    conf.RegisterOutgoing(config->id, transport->outgoing_channels[config->id]);
  }

  // Now we can begining to accept. Each client sock is a source for our Promise.
  // 0 is reserved for local source.
  //
  // The sources are different from nodes, and their orders are certainly
  // different from nodes too.
  for (size_t i = 1; i < nr_nodes; i++) {
    auto *client_sock = server_sock->Accept(128 << 20, 1024);
    if (client_sock == nullptr) continue;

    logger->info("New worker peer connection");
    transport->incoming_socks[i - 1] = client_sock;

    auto chn = new ReceiverChannel(client_sock, transport);
    logger->info("Incoming connection {}", (void *) chn);
    transport->incoming_connection[i - 1] = chn;
    conf.RegisterIncoming(i - 1, chn);
  }
  console.UpdateServerStatus(Console::ServerStatus::Running);
}

}

TcpNodeTransport::TcpNodeTransport()
{
  if (NodeConfiguration::g_data_migration) {
    auto &peer = node_config().config().row_shipper_peer;
    go::GetSchedulerFromPool(node_config().g_nr_threads + 1)->WakeUp(
        new tcp::NodeRowShipmentReceiverRoutine(peer.host, peer.port));
  }
  logger->info("Starting node server with id {}", node_config().node_id());
  serv = new tcp::NodeServerRoutine(this);
  go::GetSchedulerFromPool(0)->WakeUp(serv);
}

void TcpNodeTransport::OnCounterReceived()
{
  auto &conf = util::Instance<NodeConfiguration>();
  if (counters.fetch_add(1) + 2 == conf.nr_nodes()) {
    for (int i = 0; i < conf.nr_nodes() - 1; i++) {
      auto r = incoming_connection[i];
      auto s = conf.CalculateIncomingFromNode(r->src_node_id);
      logger->info("Counter stablized: {} src {} expecting {} pieces", (void *) r, r->src_node_id, s);
      r->Complete(EpochClient::kMaxPiecesPerPhase - s);
    }
    counters = 0;
  }
}

void TcpNodeTransport::TransportPromiseRoutine(PieceRoutine *routine)
{
  auto &conf = node_config();
  auto src_node = conf.node_id();
  auto dst_node = routine->node_id == 0 ? src_node : routine->node_id;
  int level = routine->level;

  auto &meta = conf.batcher().GetLocalData(level, go::Scheduler::CurrentThreadPoolId() - 1);

  if (src_node != dst_node) {
    auto out = outgoing_channels.at(dst_node);
    uint64_t buffer_size = routine->NodeSize();
    auto *buffer = (uint8_t *) out->Alloc(8 + buffer_size);

    memcpy(buffer, &buffer_size, 8);
    routine->EncodeNode(buffer + 8);
    out->Finish(8 + buffer_size);
  } else {
    ltp.TransportPromiseRoutine(routine);
  }
  meta.AddRoute(dst_node);
}

void TcpNodeTransport::TransportFutureValue(BaseFutureValue *val)
{
  auto &conf = node_config();

  for (uint8_t i = 0; i < val->nr_subscribed_nodes(); i++) {
    auto node = val->subscribed_node(i);
    if (node == conf.node_id()) {
      val->Signal((int) val->subscribed_node_affinity(i));
      //auto flush_v = ltp.TryFlushForCore((int) val->subscribed_node_affinity(i)); //we need to flush on other cores
      //logger->info("zqLocal success: {}, target {}",flush_v, (int) val->subscribed_node_affinity(i));
      continue;
    }

    auto out = outgoing_channels.at(node);
    // Fill in the correct buffer_size
    size_t buffer_size = ((FutureValue<int32_t> *)val)->EncodeSize(); //TODO remove cast here as well
    buffer_size += 3 * sizeof(uint64_t); //header, epoch, offset
    buffer_size += 2 * sizeof(int); //node id, affinity
    //the buffer is 32 bytes, plus the size of the encoded future value, which for now is always 4 bytes, for 36 total


    uint8_t *buffer = (uint8_t *) out->Alloc(buffer_size);

    uint64_t header = buffer_size - 8 + ((uint64_t)2<<55); // 2^56 is our flag

    GenericEpochObject<BaseFutureValue> epoch_info = val->ConvertToEpochObject();

    memcpy(buffer,&header,8);
    memcpy(buffer+8,&(epoch_info.epoch_nr),8);
    memcpy(buffer+16,&(epoch_info.offset),8);
    memcpy(buffer+24,&(epoch_info.node_id),4);
    int temp = (int) val->subscribed_node_affinity(i);
    memcpy(buffer+28, &temp, 4); // we also tack on the affinity at the destination

    //logger->info("sent {}{}{} with aff {}",epoch_info.epoch_nr, epoch_info.offset, epoch_info.node_id, (int) val->subscribed_node_affinity(i), (uint64_t)val);

    ((FutureValue<int32_t> *)val)->EncodeTo(buffer+32); //TODO remove cast

    out->Finish(buffer_size);
  }
}

void TcpNodeTransport::FinishCompletion(int level)
{
  auto &conf = node_config();
  auto src_node = conf.node_id();
  auto core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  auto &meta = conf.batcher().GetLocalData(level, core_id);

  // Optimistically flush the scheduler so that we can start earlier.
  ltp.Flush();

  for (auto dst_node = 1; dst_node <= conf.nr_nodes(); dst_node++) {
    if (dst_node == src_node) continue;

    auto idx = conf.BatchBufferIndex(level, src_node, dst_node);
    auto target_cnt = conf.TotalBatchCounter(idx).load();
    auto cnt = conf.batcher().Merge(level, meta, dst_node);
    auto chn = outgoing_channels.at(dst_node);
    if (cnt == target_cnt && cnt > 0) {
      // Flush channels to this route
      if (dst_node != src_node) {
        chn->Flush();
      }
    }
  }
}

bool TcpNodeTransport::PeriodicIO(int core)
{
  auto &conf = node_config();

  bool cont_io = false;
  for (int i = 1; i <= conf.nr_nodes(); i++) {
    if (i == conf.node_id()) continue;
    auto chn = outgoing_channels.at(i);
    if (core == -1) {
      chn->Flush();
    } else {

      auto [success, did_flush] = chn->TryFlushForThread(core + 1);

      // We need to flush no matter what.
      chn->DoFlush(true);
    }
  }

  for (int i = 0; i < conf.nr_nodes() - 1; i++) {
    auto recv = incoming_connection.at(i);
    if (recv->current_status() == IncomingTraffic::Status::EndOfPhase) {
      recv->ResetSelf(core);
      continue;
    }
    //logger->info("beginning poll on core {}",core);
    cont_io = true;
    PieceRoutine *routines[128];
    auto nr_recv = recv->Poll(routines, 128);
    if (nr_recv > 0) {
      //logger->info("poll found result on core {}",core);
      // We do not need to flush, because we are adding pieces to ourself!
      //util::Impl<PromiseRoutineDispatchService>().Add(core, routines, nr_recv);
      //logger->info("allocating space");
      // per-core buffers

      ///**
      //logger->info("affinity route pre core: {}", core);
      PieceRoutine *routine_buffer[16][128]; //TODO, allocate dynamically to number of threads without causing segfault
      int buffer_size[16] = {0};

      //logger->info("got space");
      for(int j = 0; j < nr_recv; j++){
        auto aff = routines[j]->affinity;
        //logger->info("aff {}",aff);
        if(aff < NodeConfiguration::g_nr_threads){ //add to affinity correct buffer if possible
          routine_buffer[aff][buffer_size[aff]] = routines[j];
          buffer_size[aff]++;
        }else{ //else, put in ours
          routine_buffer[core][buffer_size[core]] = routines[j];
          buffer_size[core]++;
        }
      }

      //logger->info("sending");
      //send out on cores
      for(int j = 0; j < NodeConfiguration::g_nr_threads; j++){
        if(buffer_size[j]>0){ //only bother if we've got anything - enable once we're passing signals through here?
          util::Impl<PromiseRoutineDispatchService>().Add(j, routine_buffer[j], buffer_size[j]);
          buffer_size[j] = 0;
          //if(j == core) continue;
          //auto flush_v = ltp.TryFlushForCore(j); //we need to flush on other cores //actually we don't
          //if(!flush_v)
            //logger->info("flush failed, self:target {}:{}", core,j);
        }else{
          //logger->info("skipped, self:target {}:{}", core,j); //this will be spammed if passing signal routines
        }
      }
      //logger->info("affinity route post core: {}", core);
      //**/

      
      //bad way of directing with affinity, way too much locking, and no idea if this could risk a deadlock
      /**
      logger->info("affinity route pre core: {}", core);
      for(int j = 0; j < nr_recv; j++){
        if(routines[j]->affinity < NodeConfiguration::g_nr_threads){
          util::Impl<PromiseRoutineDispatchService>().Add(routines[j]->affinity, routines + j, 1); //thanks auto pointer-arithmatic
        }else{
          util::Impl<PromiseRoutineDispatchService>().Add(core, routines + j, 1); //this never happens when pinning to warehouse
        }
      }
      logger->info("affinity route post core: {}", core);
      **/
      
    }
    if(recv->current_status() == IncomingTraffic::Status::SyncForEnd){
      recv->SyncBarrier(core);
    }


    //logger->info("ending poll on core {}",core);
  }

  // We constantly flush the issuing buffer as well. This is because the core
  // needs to poll from this in case it has some pieces it needs.
  //
  // We don't need to do a full flush, just like above, as long as every core is
  // flushing periodically, we are free from deadlock.

  // ltp.Flush();
  ltp.TryFlushForCore(core);

  return cont_io;
}

void TcpNodeTransport::PrefetchInbound()
{
  auto &conf = node_config();
  for (int i = 0; i < conf.nr_nodes() - 1; i++) {
    auto recv = incoming_connection.at(i);
    if (!recv->TryLock()) continue;
    recv->in->OpportunisticReadFromNetwork();
    recv->Unlock();
  }
}

}
