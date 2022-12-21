#ifndef PIECE_CC_H
#define PIECE_CC_H

#include "piece.h"

namespace felis {

class PieceCollection : public BasePieceCollection {
 public:
  using BasePieceCollection::BasePieceCollection;
  /**
   *
   * @tparam Func
   * @tparam Closure
   * @param capture
   * @param placement The destination node that this piece should run on
   * @param func
   * @param affinity
   * @param signals TODO: Shujian: is this ever used?
   * @return
   */
  template <typename Func, typename Closure>
  PieceRoutine *AttachRoutine(const Closure &capture, int placement, Func func,
                              uint64_t affinity = std::numeric_limits<uint64_t>::max(),
                              uint8_t signals = 0,
                              uint8_t future_source_node_id = 0) {
    // C++17 allows converting from a non-capture lambda to a constexpr function pointer! Cool!
    constexpr void (*native_func)(const Closure &) = func;

    auto static_func =
        [](PieceRoutine *routine) {
          Closure capture;
          capture.DecodeFrom(routine->capture_data);

          native_func(capture);
        };
    auto routine = PieceRoutine::CreateFromCapture(capture.EncodeSize());
    routine->node_id = placement;
    routine->callback = static_func;
    routine->affinity = affinity;
    routine->fv_signals = signals; // TODO: Seperate signals to AttachFuture
    routine->future_source_node_id = future_source_node_id;
    capture.EncodeTo(routine->capture_data);

    Add(routine);
    return routine;
  }
  

};

}

#endif
