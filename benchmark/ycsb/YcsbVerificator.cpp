//
// Created by maplestorytot on 2022-03-27.
//

#include "YcsbVerificator.h"
#include "ycsb.h"
#include "memory.h"
#include "verification_txn_collector.h"

namespace verification {
    void YcsbVerificator::InitializeExperiment() {
        InitializationCollectAllKeys initialization_keys;
        void *buf = alloca(512);
        auto nr_threads = felis::NodeConfiguration::g_nr_threads;
        for (auto t = 0; t < nr_threads; t++) {
            printf("verification t = %d\n", t);

            mem::ParallelPool::SetCurrentAffinity(t);
            unsigned long start = t * ycsb::Client::g_table_size / nr_threads;
            unsigned long end = (t + 1) * ycsb::Client::g_table_size / nr_threads;

            for (unsigned long i = start; i < end; i++) {
                ycsb::Ycsb::Value dbv;
//                dbv.v.resize_junk(999);
                std::string my_value = "ECE496" + std::to_string(i);
                dbv.v.assign(my_value);
                initialization_keys.CollectKey(i);
                this->table.Update( (uint64_t)i, dbv.Encode());
            }
        }

        VerifyKeysOfTxn(&initialization_keys, buf);
        logger->info("Successfully verified inital state of DB");
    }

    void YcsbVerificator::ExecuteEpoch() {
        std::map<uint64_t, VerificationTxn*>* txns = util::Instance<VerificationTxnCollector>().GetTxns();
        logger->info("Performing Execution of Verification DB");
        for (auto const& pair: *txns) {
            VerificationTxn* txn = pair.second;
//            logger->info("Serial ID: {}", pair.first);
            txn->Run();
        }
    }

    void YcsbVerificator::VerifyDatabaseState() {
        void *buf = alloca(512);
        std::map<uint64_t, VerificationTxn*>* txns = util::Instance<VerificationTxnCollector>().GetTxns();
        logger->info("Performing Verification on {} Transactions", txns->size());
        for (auto const& pair: *txns) {
            VerificationTxn* txn = pair.second;
//            logger->info("Verify Txn {}", pair.first);
            VerifyKeysOfTxn(txn, buf);
        }
        logger->info("Successfully verified epoch");
    }

    void YcsbVerificator::VerifyKeysOfTxn(VerificationTxn* txn, void* buf){
        VerificationTxnKeys list_keys = txn->GetTxnKeys();
        for (int i = 0; i < list_keys.nr; i++) {
            ycsb::Ycsb::Key ycsb_key = ycsb::Ycsb::Key::New(list_keys.keys[i]);
            auto verification_value = this->table.Get(list_keys.keys[i]);
            if (!verification_value) {
                logger->info("Should not occur. Key was not found in verification table");
                std::abort();
            }
            auto caracal_vhandle = util::Instance<felis::TableManager>().Get<ycsb::Ycsb>().Search(
                    ycsb_key.EncodeView(buf));/*CHECK: this can be reused below?!!!*/
            if (!caracal_vhandle) {
                logger->info("Failed Verification. Key (vhandle) was not found in caracal table during verification");
                std::abort();
            }
            uint64_t last_sid = caracal_vhandle->last_version();
            felis::VarStr* real_value;
            if (felis::NodeConfiguration::g_priority_txn &&
                last_sid < caracal_vhandle->last_priority_version()) {
                auto priority_caracal_value = caracal_vhandle->VerificatorGetExtraVhandle()->SpyLastVersion();
                if (!priority_caracal_value) {
                    logger->info("Failed Verification. Key (a priority version) was not found in caracal table during verification");
                    std::abort();
                }
                real_value = priority_caracal_value;
            }else {
                auto caracal_value = caracal_vhandle->ReadExactVersion(caracal_vhandle->nr_versions() - 1);
                if (!caracal_value) {
                    logger->info("Failed Verification. Key (a version) was not found in caracal table during verification");
                    std::abort();
                }
                real_value = caracal_value;
            }


            if (memcmp(verification_value->data(), real_value->data(), verification_value->length()) != 0) {
                auto check = caracal_vhandle->VerificatorGetExtraVhandle()->SpyLastVersion();
                logger->info("Failed Verification. Values in verification and caracal table mismatch");
                std::abort();
            }
        }
    }


}
