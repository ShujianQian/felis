//
// Created by maplestorytot on 2022-03-27.
//

#include "verification_txn_collector.h"
namespace verification{
    void VerificationTxnCollector::CollectTxn(uint64_t sid, VerificationTxn* txn) {
        this->txns.insert({sid, txn});
    }
    std::map<uint64_t, VerificationTxn*>* VerificationTxnCollector::GetTxns(){
        return &txns;
    }
    void VerificationTxnCollector::ClearTxns(){
        txns.clear();
    }

    VerificationTxnKeys InitializationCollectAllKeys::GetTxnKeys() {
        return VerificationTxnKeys{txn_keys.data(), static_cast<int> (txn_keys.size())};
    }

    void InitializationCollectAllKeys::Run() {}

    void InitializationCollectAllKeys::CollectKey(uint64_t key) {
        txn_keys.push_back(key);
    }
}