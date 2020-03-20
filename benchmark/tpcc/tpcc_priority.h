#include "priority.h"
#include "benchmark/tpcc/tpcc.h"

namespace tpcc {

// STOCK transaction: a priority txn, add stock to certain items in a warehouse
struct StockTxnInput {
  static constexpr int kStockMaxItems = 5;
  uint warehouse_id;
  uint nr_items;

  struct StockDetail {
    uint item_id[kStockMaxItems];
    uint stock_quantities[kStockMaxItems];
  } detail;
};

template <>
StockTxnInput ClientBase::GenerateTransactionInput<StockTxnInput>();

bool StockTxn_Run(felis::PriorityTxn *txn);

}