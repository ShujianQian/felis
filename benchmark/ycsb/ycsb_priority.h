#ifndef YCSB_PRIORITY_H
#define YCSB_PRIORITY_H

#include "priority.h"
#include "benchmark/ycsb/ycsb.h"
#include "YcsbVerificator.h"

namespace ycsb {

// Pre-generate priority txns for the benchmark before the experiment starts
void GeneratePriorityTxn();

// MW transaction: a priority txn, will modify-write x rows
struct MWTxnInput {
  static constexpr int kMaxRowUpdates = 5;
  int nr;
  uint64_t keys[kMaxRowUpdates];
};

template <>
MWTxnInput Client::GenerateTransactionInput<MWTxnInput>();

bool MWTxn_Run(felis::PriorityTxn *txn);

}
namespace verification{
    class PriorityMWVerificationTxn : public VerificationTxn {
    private:
        felis::MWTxnInput input;
        uint64_t sid;
    public:
        PriorityMWVerificationTxn(felis::MWTxnInput input, uint64_t sid):input(input),sid(sid){};
        ~PriorityMWVerificationTxn(){};
        void Run() override;
        VerificationTxnKeys GetTxnKeys() override final;
    };
}
#endif /* YCSB_PRIORITY_H */
