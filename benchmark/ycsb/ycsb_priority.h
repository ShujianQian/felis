#ifndef YCSB_PRIORITY_H
#define YCSB_PRIORITY_H

#include "priority.h"
#include "benchmark/ycsb/ycsb.h"

namespace ycsb {

// Pre-generate priority txns for the benchmark before the experiment starts
void GeneratePriorityTxn();

// MW transaction: a priority txn, will modify-write x rows
struct MWTxnInput {
  static constexpr int kMaxRowUpdates = 5;
  int nr;
  uint64_t keys[kMaxRowUpdates];
};
    struct ECE496Input {
        uint warehouse_id;
    };

    template <>
    ECE496Input Client::GenerateTransactionInput<ECE496Input>();

template <>
MWTxnInput Client::GenerateTransactionInput<MWTxnInput>();

    bool MWTxn_Run(felis::PriorityTxn *txn);
    bool ECE496_Run(felis::PriorityTxn *txn);

}

#endif /* YCSB_PRIORITY_H */
