// -*- mode: c++ -*-

#ifndef ENTITY_H
#define ENTITY_H

#include "mem.h"
#include "shipping.h"

// Entity is the minimum item we would like to send.
// An IndexEntity is the index of a row, and a RowEntity is a row.
// Each Entity needs to have a ShippingHandle, in order for us to send it.

namespace felis {

class VHandle;
class VarStr;

class RowEntity final {
  friend class RowShipmentReceiver;
  friend class SortedArrayVHandle;

  int alloc_coreid;
  int rel_id;
  int slice;
  VarStr *k;
  VHandle *handle_ptr;
  ObjectShippingHandle<RowEntity> shandle;
  VarStr *v;
 public:
  RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id);
  RowEntity() : RowEntity(-1, nullptr, nullptr, -1) {}
  ~RowEntity() {}
  RowEntity(const RowEntity &rhs) = delete; // C++17 has guaranteed copy-elision! :)

  ShippingHandle *shipping_handle() { return &shandle; }
  bool ShouldSkip();
  int EncodeIOVec(struct iovec *vec, int max_nr_vec);
  uint64_t encoded_len;

  void DecodeIOVec(struct iovec *vec);
  void Prepare(void *prepared_buf) { this->k = (VarStr *) prepared_buf; }

  int get_rel_id() const { return rel_id; }
  int slice_id() const { return slice; }

  static void *operator new(size_t s) {
    return pool.Alloc();
  }

  static void operator delete(void *p) {
    auto ent = (RowEntity *) p;
    pool.Free(p, ent->alloc_coreid);
  }

  static void InitPool();
  static mem::ParallelSlabPool pool;
  static void Quiescence() { pool.Quiescence(); };
};

}
#endif /* ENTITY_H */
