#ifndef TPCC_PAYMENT_H
#define TPCC_PAYMENT_H

#include "tpcc.h"
#include "txn_cc.h"
#include "pwv_graph.h"

namespace tpcc {

using namespace felis;

struct PaymentStruct {
  uint warehouse_id;
  uint district_id;
  uint customer_warehouse_id;
  uint customer_district_id;
  int payment_amount;
  uint32_t ts;
  uint customer_id;
};

struct PaymentState {
  VHandle *warehouse;
  VHandle *district;
  VHandle *customer;

  //FutureValue<int32_t> warehouse_tax_future;

  NodeBitmap nodes;

  InvokeHandle<PaymentState, int, uint> warehouse_future;
  InvokeHandle<PaymentState, int> district_future;
  InvokeHandle<PaymentState, int> customer_future;
  struct Completion : public TxnStateCompletion<PaymentState> {
    void operator()(int id, BaseTxn::LookupRowResult rows) {
      if (id == 0) {
        state->warehouse = rows[0];
        //state->warehouse_tax_future = FutureValue<int32_t>();
      } else if (id == 1) {
        state->district = rows[0];
      } else if (id == 2) {
        state->customer = rows[0];
      }
      handle(rows[0]).AppendNewVersion(1);
      if (Client::g_enable_pwv) {
        util::Instance<PWVGraphManager>().local_graph()->AddResource(
            handle.serial_id(), PWVGraph::VHandleToResource(rows[0]));
      }
    }
  };
};

class PaymentTxn : public Txn<PaymentState>, public PaymentStruct {
  Client *client;
 public:
  PaymentTxn(Client *client, uint64_t serial_id);

  void Prepare() override final;
  void Run() override final;
  void PrepareInsert() override final {}

  static void UpdateWarehouse(const State &state, const TxnHandle &index_handle,
                              int payment_amount, int customer_warehouse_id);
  static void UpdateDistrict(const State &state, const TxnHandle &index_handle,
                             int payment_amount);
  static void UpdateCustomer(const State &state, const TxnHandle &index_handle,
                             int payment_amount);
};

}

#endif
