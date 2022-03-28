//
// Created by maplestorytot on 2022-03-27.
//

#ifndef FELIS_YCSBVERIFICATOR_H
#define FELIS_YCSBVERIFICATOR_H
#include <map>
#include "verification_table.h"

namespace verification {
    class YcsbVerificator {
    private:
        void VerifyKeysOfTxn(VerificationTxn* txn, void* buf);
    public:
        /*felis::VarStrView*//*sql::inline_str_16<sql::kYcsbRecordSize>*/
        VerificationTable<uint64_t, felis::VarStr* > table;
        void InitializeExperiment();

        void ExecuteEpoch();

        void VerifyDatabaseState();
    };

}



namespace util {

    template <>
    struct InstanceInit<verification::YcsbVerificator> {
        static constexpr bool kHasInstance = true;
        static inline verification::YcsbVerificator *instance;

        InstanceInit() {
            instance = new verification::YcsbVerificator();
        }
    };

}

#endif //FELIS_YCSBVERIFICATOR_H
