#include <unistd.h>
#include "entity.h"
#include "index.h"
#include "literals.h"
#include "vhandle.h"

namespace felis {

void RowEntity::DecodeIOVec(struct iovec *vec)
{
  auto p = (uint8_t *) vec->iov_base;

  memcpy(&rel_id, p, 4);
  memcpy(&slice, p + 4, 4);

  auto key_size = *((uint16_t *)(p + 8));
  assert(key_size > 0);

  auto pp = (uint8_t *) k;

  k = VarStr::FromPtr(pp, key_size);
  memcpy((uint8_t *) k->data(), p + 10, key_size);
  pp += util::Align(VarStr::NewSize(key_size), 8);

  auto value_size = vec->iov_len - 10 - key_size;
  assert(value_size > 0);
  v = VarStr::FromPtr(pp, value_size);
  memcpy((uint8_t *) v->data(), p + 10 + key_size, value_size);
}

int RowEntity::EncodeIOVec(struct iovec *vec, int max_nr_vec)
{
  if (max_nr_vec < 4)
    return 0;

  vec[0].iov_len = 4;
  vec[0].iov_base = &rel_id;
  vec[1].iov_len = 4;
  vec[1].iov_base = &slice;
  vec[2].iov_len = 2;
  auto klen = k->length();
  vec[2].iov_base = &klen;
  vec[3].iov_len = klen;
  vec[3].iov_base = (void *) k->data();

  VarStr *v = handle_ptr->ReadExactVersion(handle_ptr->latest_version.load());
  vec[4].iov_len = v->length();
  vec[4].iov_base = (void *) v->data();
  encoded_len = 10 + klen + v->length();

  shipping_handle()->PrepareSend();

  return 5;
}

bool RowEntity::ShouldSkip()
{
  return (handle_ptr->size == 0) || (handle_ptr->versions[handle_ptr->capacity + handle_ptr->latest_version.load()] == kPendingValue);
}

RowEntity::RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id)
    : alloc_coreid(mem::ParallelPool::CurrentAffinity()),
      rel_id(rel_id), k(k), handle_ptr(handle), shandle(this), slice(slice_id)
{
  // TODO: in data replay, after row_entity exists, reset(this) somewhere else
  // if (handle_ptr)
  //   handle_ptr->row_entity.reset(this);
}

mem::ParallelSlabPool RowEntity::pool;

void RowEntity::InitPool()
{
  pool = mem::ParallelSlabPool(mem::EntityPool, sizeof(RowEntity), 4);
  pool.Register();
}

}
