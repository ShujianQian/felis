//
// Created by maplestorytot on 2022-03-27.
//

#ifndef FELIS_VERIFICATION_TXN_COLLECTOR_H
#define FELIS_VERIFICATION_TXN_COLLECTOR_H
#include <map>
#include "verification_table.h"
namespace verification{
    class InitializationCollectAllKeys: public VerificationTxn {
    private:
        std::vector<uint64_t> txn_keys;
    public:
        VerificationTxnKeys GetTxnKeys() override;
        void Run() override;
        void CollectKey(uint64_t key);
    };

    class VerificationTxnCollector {
    std::mutex collection_mutex;
    std::map<uint64_t, VerificationTxn*> txns;
    public:
        void CollectTxn(uint64_t sid, VerificationTxn* txn);
        std::map<uint64_t, VerificationTxn*>* GetTxns();
        void ClearTxns();
    };
}

namespace util {

    template <>
    struct InstanceInit<verification::VerificationTxnCollector> {
        static constexpr bool kHasInstance = true;
        static inline verification::VerificationTxnCollector *instance;

        InstanceInit() {
            instance = new verification::VerificationTxnCollector();
        }
    };

}

#endif //FELIS_VERIFICATION_TXN_COLLECTOR_H
