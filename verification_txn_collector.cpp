//
// Created by maplestorytot on 2022-03-27.
//

#include "verification_txn_collector.h"
namespace verification{
    void VerificationTxnCollector::CollectTxn(uint64_t sid, VerificationTxn* txn) {
        this->collection_mutex.lock();
        this->txns.insert({sid, txn});
        this->collection_mutex.unlock();
    }
    std::map<uint64_t, VerificationTxn*>* VerificationTxnCollector::GetTxns(){
        return &txns;
    }
    void VerificationTxnCollector::ClearTxns(){
        for(auto const& pair: txns){
            VerificationTxn* txn = pair.second;
            delete txn;
        }
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