//
// Created by maplestorytot on 2022-03-27.
//

#ifndef FELIS_VERIFICATION_TABLE_H
#define FELIS_VERIFICATION_TABLE_H
#include "index_common.h"

namespace verification {
    struct VerificationTxnKeys{
        uint64_t* keys;
        int nr;
    };

    class VerificationTxn {
    public:
        virtual VerificationTxnKeys GetTxnKeys() = 0;
        virtual void Run() = 0;
    };

    template<typename Key, typename Value>
    class VerificationTable {
    private:
        std::unordered_map<Key, Value> table;
    public:
        void Update(Key key, Value value){
            table[key] = value;
        }
        Value Get(Key key){
            return table[key];
        }
    };
}
#endif //FELIS_VERIFICATION_TABLE_H
