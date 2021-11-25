// -*- mode: c++ -*-

#ifndef TXN_CC_H
#define TXN_CC_H

#include "slice.h"
#include "sqltypes.h"
#include "epoch.h"
#include "txn.h"
#include "contention_manager.h"
#include "piece_cc.h"

namespace felis {

// C++ api layer.
// TODO: some of the BaseTxn should be here.

template <typename Table>
class KeyParam {
 public:
  static constexpr int kRelationId = static_cast<int>(Table::kTable);
  using TableType = Table;
 protected:
  const typename Table::Key *start;
  int len;
 public:
  KeyParam(const typename Table::Key &k)
      : start(&k), len(1) {}
  KeyParam(const typename Table::Key *start, int len)
      : start(start), len(len) {}

  int EncodeToSliceId(int idx) {
    return util::Instance<SliceLocator<TableType>>().Locate(start[idx]);
  }

  int size() const { return len; }
  const typename Table::Key &operator[](int idx) { return start[idx]; }
};

template <typename Table>
class RangeParam {
 public:
  static constexpr int kRelationId = static_cast<int>(Table::kTable);
  using TableType = Table;
 private:
  const typename Table::Key *start;
  const typename Table::Key *end;
 public:
  RangeParam(const typename Table::Key &start, const typename Table::Key &end)
      : start(&start), end(&end) {}

  int EncodeToSliceId(int idx) { return -1 - idx; }
  int size() const { return 2; }
  const typename Table::Key &operator[](int idx) {
    if (idx == 0) return *start;
    else if (idx == 1) return *end;
    std::abort();
  }
};

class PlaceholderParam {
  int nr;
 public:
  using TableType = void;
  PlaceholderParam(int nr = 1) : nr(nr) {}
  int size() const { return nr; }
};

class NodeBitmap {
 public:
  using Pair = std::tuple<int16_t, uint16_t>;
 private:
  uint8_t len;
  Pair pairs[BaseTxn::BaseTxnIndexOpContext::kMaxPackedKeys];
 public:
  NodeBitmap() : len(0) {}
  NodeBitmap(const NodeBitmap &rhs) : len(rhs.len) {
    std::copy(rhs.pairs, rhs.pairs + len, pairs);
  }

  uint8_t size() const { return len; }
  Pair *begin() { return pairs; }
  Pair *end() { return pairs + len; }
  const Pair *begin() const { return pairs; }
  const Pair *end() const { return pairs + len; }

  void Add(int16_t node, uint16_t bitmap) {
    pairs[len++] = Pair(node, bitmap);
  }

  void MergeOrAdd(int16_t node, uint16_t bitmap) {
    for (int i = 0; i < len; i++) {
      auto [n, oldbitmap] = pairs[i];
      if (n == node) {
        pairs[i] = Pair(node, oldbitmap | bitmap);
        return;
      }
    }
    Add(node, bitmap);
  }

  NodeBitmap & operator+=(const NodeBitmap &rhs) {
    for (Pair e: rhs) {
      auto &[node, bitmap] = e;
      MergeOrAdd(node, bitmap);
    }
    return *this;
  }
};

template <typename T> class FutureValue;

template <>
class FutureValue<void> : public BaseFutureValue {};

template <typename T>
class FutureValue final : public BaseFutureValue {
  T value;
 public:
  using ValueType = T;
  FutureValue() : BaseFutureValue() {}
  FutureValue(const FutureValue<T> &rhs) : value(rhs.value) {
    this->operator=(rhs);
  }

  const FutureValue<T> &operator=(const FutureValue<T> &rhs) {
    BaseFutureValue::operator=(rhs);
    value = rhs.value;
    return *this;
  }

  void Signal(T v) {
    value = v;
    BaseFutureValue::Signal();
  }
  T &Wait() {
    BaseFutureValue::Wait();
    return value;
  }
  size_t EncodeSize() override {
    return sql::Serializer<T>::EncodeSize(&value);
  }
  void EncodeTo(uint8_t *buf) override {
    return sql::Serializer<T>::EncodeTo(buf, &value);
  }
  void DecodeFrom(const uint8_t *buf) override {
    return sql::Serializer<T>::DecodeFrom(&value, buf);
  }
};

template <typename TxnState> class Txn;

template <typename TxnState, typename ...Types>
struct InvokeHandle {
  using Context = typename Txn<TxnState>::template ContextType<Types...>;
  using RowFuncPtr = void (*)(const Context&, VHandle *);

  RowFuncPtr rowfunc = nullptr;
  VHandle *row = nullptr;

  InvokeHandle<TxnState, Types...> &operator=(const InvokeHandle<TxnState, Types...> &rhs) = default;

  void ClearCallback() {
    row = nullptr;
    rowfunc = nullptr;
  }

  bool has_callback() const {
    return rowfunc;
  }

  void InvokeWithContext(const Context& ctx) const {
    if (has_callback())
      rowfunc(ctx, row);
  }

  void Invoke(const typename Txn<TxnState>::State &state,
              const typename Txn<TxnState>::TxnHandle &index_handle,
              Types... args) const {
    if (has_callback())
      InvokeWithContext(Context(state, index_handle, args...));
  }
};

template <typename TxnState>
class Txn : public BaseTxn {
 public:
  typedef GenericEpochObject<TxnState> State;

 protected:
  PieceCollection *root;
  State state;
 public:

  class TxnRow : public BaseTxnRow {
   public:
    using BaseTxnRow::BaseTxnRow;

    template <typename T> T Read() {
      return ReadVarStr()->template ToType<T>();
    }
    template <typename T> bool Write(const T &o) {
      return WriteVarStr(o.Encode());
    }

    template <typename T> bool WriteTryInline(const T &o) {
      return WriteVarStr(o.EncodeToPtrOrDefault(vhandle->AllocFromInline(sizeof(VarStr) + o.EncodeSize())));
    }
  };

  class TxnHandle : public BaseTxnHandle {
   public:
    using BaseTxnHandle::BaseTxnHandle;
    TxnHandle(const BaseTxnHandle &rhs) : BaseTxnHandle(rhs) {}

    TxnRow operator()(VHandle *vhandle) const { return TxnRow(sid, epoch_nr, vhandle); }
  };

  TxnHandle index_handle() const { return TxnHandle(sid, epoch->id()); }

  struct TxnIndexOpContext : public BaseTxn::BaseTxnIndexOpContext {
   private:
    template <typename R>
    int _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param) {
      for (int i = bitshift; i < kMaxPackedKeys && i < bitshift + param.size(); i++) {
        if constexpr (!std::is_void<typename R::TableType>::value) {
          if (bitmap & (1 << i)) {
            auto view = param[i - bitshift].EncodeViewRoutine();
            key_len[shift] = view.length();
            key_data[shift] = view.data();
            relation_ids[shift] = R::kRelationId;
            slice_ids[shift] = param.EncodeToSliceId(i - bitshift);

            shift++;
          }
        }
      }
      return shift;
    }
    template <typename R, typename ...T>
    void _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param, T ...rest) {
      shift = _FromKeyParam(bitmap, bitshift, shift, param);
      _FromKeyParam(bitmap, bitshift + param.size(), shift, rest...);
    }
   public:
    template <typename ...T>
    TxnIndexOpContext(BaseTxnHandle handle, EpochObject state, uint16_t bitmap, T ...params) {
      this->handle = handle;
      this->state = state;
      this->keys_bitmap = this->slices_bitmap = this->rels_bitmap = bitmap;

      _FromKeyParam(bitmap, 0, 0, params...);
    }

    TxnIndexOpContext() {}
  };

  template <typename Extra>
  struct TxnIndexOpContextEx : public TxnIndexOpContext, public Extra {
    using TxnIndexOpContext::TxnIndexOpContext;

    void set_extra(const Extra &rhs) {
      (Extra &)(*this) = rhs;
    }

    size_t EncodeSize() const {
      return TxnIndexOpContext::EncodeSize() + Extra::EncodeSize();
    }
    uint8_t *EncodeTo(uint8_t *buf) const {
      return Extra::EncodeTo(TxnIndexOpContext::EncodeTo(buf));
    }
    const uint8_t *DecodeFrom(const uint8_t *buf) {
      return Extra::DecodeFrom(TxnIndexOpContext::DecodeFrom(buf));
    }
  };

  template <>
  struct TxnIndexOpContextEx<void> : public TxnIndexOpContext {
    using TxnIndexOpContext::TxnIndexOpContext;
  };

  Txn(uint64_t serial_id) : BaseTxn(serial_id) {}

  PieceCollection *root_promise() override final { return root; }
  void ResetRoot() override final { root = new PieceCollection(); }

  void PrepareState() override {
    epoch = util::Instance<EpochManager>().current_epoch();
    state = epoch->AllocateEpochObjectOnCurrentNode<TxnState>();
    // printf("state epoch %lu\n", state.nr());
  }

  template <typename ...Types> using ContextType = sql::Tuple<State, TxnHandle, Types...>;

  template <typename ...Types>
  ContextType<Types...> MakeContext(Types... params) {
    return ContextType<Types...>(state, index_handle(), params...);
  }

  template <typename Func, typename ...Types>
  std::tuple<ContextType<Types...>, int, Func>
  TxnProc(int node, Func func, Types... params) {
    return std::make_tuple(
        MakeContext(params...),
        node,
        func);
  }

  template <typename ...Types>
  InvokeHandle<TxnState, Types...> UpdateForKey(
      int node, VHandle *row,
      typename InvokeHandle<TxnState, Types...>::RowFuncPtr rowfunc,
      Types... params) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto aff = UpdateForKeyAffinity(node, row);
    InvokeHandle<TxnState, Types...> invoke_handle{rowfunc, row};

    if (aff != -1 && !EpochClient::g_enable_granola && !EpochClient::g_enable_pwv) {
      root->AttachRoutine(
          sql::MakeTuple(invoke_handle, MakeContext(params...)),
          node,
          [](const auto &t) {
            auto &[invoke_handle, ctx] = t;
            invoke_handle.InvokeWithContext(ctx);
          },
          aff);
      invoke_handle.ClearCallback();
    }
    return invoke_handle;
  }

 private:
  template <typename Router, typename KParam, typename ...KParams>
  void KeyParamsToBitmap(uint16_t bitmap_per_node[],
                         int bitshift, KParam param, KParams ...rest) {
    if constexpr (!std::is_void<typename KParam::TableType>::value) {
      auto &locator = util::Instance<SliceLocator<typename KParam::TableType>>();
      for (int i = 0; i < param.size(); i++) {
        auto node = util::Instance<NodeConfiguration>().node_id();
        auto slice_id = locator.Locate(param[i]);
        if (slice_id >= 0) node = Router::SliceToNodeId(slice_id);
        bitmap_per_node[node] |= 1 << (i + bitshift);
      }
    }
    KeyParamsToBitmap<Router>(bitmap_per_node, bitshift + param.size(), rest...);
  }
  template <typename Router>
  void KeyParamsToBitmap(uint16_t bitmap_per_node[], int bitshift) {}
 public:
  template <typename Router, typename ...KParams>
  NodeBitmap GenerateNodeBitmap(KParams ...params) {
    auto &conf = util::Instance<NodeConfiguration>();
    uint16_t bitmap_per_node[conf.nr_nodes() + 1];
    NodeBitmap nodes_bitmap;
    std::fill(bitmap_per_node, bitmap_per_node + conf.nr_nodes() + 1, 0);
    KeyParamsToBitmap<Router>(bitmap_per_node, 0, params...);
    for (int node = 1; node <= conf.nr_nodes(); node++) {
      if (bitmap_per_node[node] == 0) continue;
      nodes_bitmap.Add(node, bitmap_per_node[node]);
    }
    return nodes_bitmap;
  }

  static constexpr uint64_t kIndexOpFlatten = std::numeric_limits<uint32_t>::max();
  uint64_t txn_indexop_affinity = kIndexOpFlatten;

  template <typename IndexOp,
            typename OnCompleteParam,
            typename OnComplete,
            typename ...KParams>
  NodeBitmap TxnIndexOpWithNodeBitmap(NodeBitmap nodes_bitmap,
                                      OnCompleteParam *pp,
                                      KParams ...params) {
    auto current_node = util::Instance<NodeConfiguration>().node_id();
    for (auto &p: nodes_bitmap) {
      auto [node, bitmap] = p;
      auto op_ctx = TxnIndexOpContextEx<OnCompleteParam>(
          index_handle(), state, bitmap, params...);

      if constexpr(!std::is_void<OnCompleteParam>()) {
        op_ctx.set_extra(*pp);
      }

      if ((node != 0 && current_node != node) || txn_indexop_affinity != kIndexOpFlatten) {
        root->AttachRoutine(
            op_ctx, node,
            [](auto &ctx) {
              auto completion = OnComplete();
              if constexpr (!std::is_void<OnCompleteParam>()) {
                completion.args = (OnCompleteParam) ctx;
              }

              completion.handle = ctx.handle;
              completion.state = State(ctx.state);

              TxnIndexOpContext::ForEachWithBitmap(
                  ctx.keys_bitmap,
                  [&ctx, &completion](int j, int i) {
                    auto op = IndexOp(ctx, j);
                    completion(i, op.result);
                  });
            },
            txn_indexop_affinity);
      } else {
        auto completion = OnComplete();
        if constexpr (!std::is_void<OnCompleteParam>()) {
          completion.args = (OnCompleteParam) op_ctx;
        }

        completion.handle = TxnHandle(op_ctx.handle);
        completion.state = State(op_ctx.state);

        TxnIndexOpContext::ForEachWithBitmap(
            op_ctx.keys_bitmap,
            [&op_ctx, &completion](int j, int i) {
              auto op = IndexOp(op_ctx, j);
              completion(i, op.result);
            });
      }
    }
    return nodes_bitmap;
  }

  template <typename IndexOp,
            typename Router,
            typename OnCompleteParam,
            typename OnComplete,
            typename ...KParams>
  NodeBitmap TxnIndexOp(OnCompleteParam *pp,
                        KParams ...params) {
    return TxnIndexOpWithNodeBitmap<IndexOp, OnCompleteParam, OnComplete, KParams...>(
        GenerateNodeBitmap<Router>(params...),
        pp,
        params...);
  }

 public:
  struct TxnIndexLookupOpImpl {
    using ResultType = LookupRowResult;
    LookupRowResult result;
    TxnIndexLookupOpImpl(const BaseTxnIndexOpContext &ctx, int idx) {
      result = BaseTxnIndexOpLookup(ctx, idx);
    }
  };
  struct TxnIndexInsertOpImpl {
    using ResultType = VHandle *;
    VHandle *result;
    TxnIndexInsertOpImpl(const BaseTxnIndexOpContext &ctx, int idx) {
      result = BaseTxnIndexOpInsert(ctx, idx);
    }
  };
  template <typename Router,
            typename Completion,
            typename CompletionParam = void,
            typename ...KParams>
  NodeBitmap TxnIndexLookup(CompletionParam *pp,
                            KParams ...params) {
    return TxnIndexOp<TxnIndexLookupOpImpl,
                      Router,
                      CompletionParam,
                      Completion,
                      KParams...>(pp, params...);
  }

  template <typename Router,
            typename Completion,
            typename CompletionParam = void,
            typename ...KParams>
  NodeBitmap TxnIndexInsert(CompletionParam *pp,
                            KParams ...params) {
    return TxnIndexOp<TxnIndexInsertOpImpl,
                      Router,
                      CompletionParam,
                      Completion,
                      KParams...>(pp, params...);
  }
};

template <typename TxnState>
class TxnStateCompletion {
 protected:
  friend class Txn<TxnState>;
  typename Txn<TxnState>::TxnHandle handle;
  GenericEpochObject<TxnState> state;
};

}

#endif
