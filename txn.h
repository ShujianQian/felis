// -*- mode: c++ -*-
#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <functional>
#include <sys/types.h>

#include "sqltypes.h"
#include "csum.h"
#include "mem.h"
#include "log.h"

#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "gopp/barrier.h"

namespace dolly {

typedef sql::VarStr VarStr;

class Relation;
class Txn;
class Epoch;

struct TxnKey {
  int16_t fid; // table id
  uint8_t len;
  uint8_t data[];
} __attribute__((packed));

struct TxnQueue {
  Txn *t;
  TxnQueue *next, *prev;

  void Init() {
    prev = next = this;
  }

  void Add(TxnQueue *parent) {
    prev = parent;
    next = parent->next;

    next->prev = this;
    prev->next = this;
  }

  void Detach() {
    next->prev = prev;
    prev->next = next;
    prev = next = nullptr;
  }

  bool is_empty() const { return next == this && prev == this; }
};


// #define VALIDATE_TXN 1
// #define VALIDATE_TXN_KEY 1

class Txn : public go::Routine {
#ifdef CALVIN_REPLAY
  uint8_t *read_set_keys;
  uint32_t sz_read_set_key_buf;
#endif
  uint8_t *keys;
  uint16_t sz_key_buf;
  unsigned int key_crc;
  uint64_t sid;
  bool is_setup;

  go::WaitBarrier *barrier;
  int *count;
  int count_max;
  Epoch *epoch;

  TxnQueue node;
  TxnQueue *reuse_q;

 private:
  static const size_t kTxnBrkSize = 64 << 10;
  static const size_t kPoolCap = 1 << 20;

  static mem::Pool *pools; // for txn brk allocator
 public:
  static void InitPools();

 public:
  uint8_t type;

  struct FinishCounter {
    int count = 0;
    int max = std::numeric_limits<int>::max();
  } *fcnt;

  Txn() : key_crc(INITIAL_CRC32_VALUE), is_setup(true) {
    set_reuse(true);
  }

#ifdef CALVIN_REPLAY
  void InitializeReadSet(go::TcpInputChannel *channel, uint32_t read_key_pkt_size, Epoch *epoch);
#endif
  void Initialize(go::TcpInputChannel *channel, uint16_t key_pkt_len, Epoch *epoch);
  void SetupReExec();

  unsigned int key_checksum() const { return key_crc; }

  void set_serializable_id(uint64_t id) { sid = id; }
  uint64_t serializable_id() const { return sid; }

  void set_wait_barrier(go::WaitBarrier *b) { barrier = b; }
  void set_counter(FinishCounter *cnt) { fcnt = cnt; }
  void set_epoch(Epoch *e) { epoch = e; }
  void set_reuse_queue(TxnQueue *reuse_queue) { reuse_q = reuse_queue; }

  void PushToReuseQueue() {
    node.t = this;
    node.Add(reuse_q->prev);
  }

  virtual void RunTxn() = 0;
  virtual int CoreAffinity() const = 0;

  uint8_t *key_buffer() const { return keys; }
  uint16_t key_buffer_size() const { return sz_key_buf; }
 protected:
  virtual void Run();
 private:
  void GenericSetupReExec(uint8_t *key_buffer, size_t key_len,
                          std::function<bool (Relation *, const VarStr *, uint64_t)> callback);
};

class TxnValidator {
  unsigned int key_crc, value_crc;
  size_t value_size;
  static std::atomic<unsigned long> tot_validated;
  uint8_t *keys_ptr;
  bool is_valid;
 public:
  TxnValidator() :
      key_crc(INITIAL_CRC32_VALUE), value_crc(INITIAL_CRC32_VALUE),
      value_size(0), keys_ptr(nullptr), is_valid(true) {}

  void set_keys_ptr(uint8_t *kptr) { keys_ptr = kptr; }
  void CaptureWrite(const Txn &tx, int fid, const VarStr *k, VarStr *obj);
  void Validate(const Txn &tx);

  static void DebugVarStr(const char *prefix, const VarStr *s);
};

class TxnIOReader : public go::Routine {
  go::TcpSocket *sock;
  go::WaitBarrier *barrier;
  Epoch *epoch;
  Txn::FinishCounter fcnt;
  TxnQueue *reuse_q;

 public:
  TxnIOReader(go::TcpSocket *s, go::WaitBarrier *bar, Epoch *e, TxnQueue *q)
      : sock(s), barrier(bar), epoch(e), reuse_q(q) {
    set_reuse(true);
  }

  Txn::FinishCounter *finish_counter() { return &fcnt; }

 protected:
  virtual void Run();
};


class TxnRunner : public go::Routine {
  TxnQueue *queue;
  bool collect_garbage;
 public:
  TxnRunner(TxnQueue *q) : queue(q), collect_garbage(false) {}
  void set_collect_garbage(bool v) { collect_garbage = v; }
  virtual void Run();
};

}

#endif /* TXN_H */