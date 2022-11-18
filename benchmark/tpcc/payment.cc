#include "payment.h"

namespace tpcc {

template <>
PaymentStruct ClientBase::GenerateTransactionInput<PaymentStruct>()
{
  PaymentStruct s;
  s.warehouse_id = PickWarehouse();
  s.district_id = PickDistrict();
  if (nr_warehouses() == 1 || RandomNumber(1, 100) > int(kPaymentRemoteCustomer * 100)) {   
    s.customer_warehouse_id = s.warehouse_id;
    s.customer_district_id = s.district_id;
  } else {
    s.customer_warehouse_id = RandomNumberExcept(1, nr_warehouses(), s.warehouse_id);
    s.customer_district_id = PickDistrict();
  }
  s.payment_amount = RandomNumber(100, 500000);
  s.ts = GetCurrentTime();

  s.customer_id = GetCustomerId();
  return s;
}

PaymentTxn::PaymentTxn(Client *client, uint64_t serial_id)
    : Txn<PaymentState>(serial_id),
      PaymentStruct(client->GenerateTransactionInput<PaymentStruct>()),
      client(client)
{}

void PaymentTxn::Prepare()
{
  INIT_ROUTINE_BRK(4096);

  auto warehouse_key = Warehouse::Key::New(warehouse_id);
  auto district_key = District::Key::New(warehouse_id, district_id);
  auto customer_key = Customer::Key::New(
      customer_warehouse_id, customer_district_id, customer_id);

  if (!VHandleSyncService::g_lock_elision) {
    if (g_tpcc_config.IsWarehousePinnable()) {
      txn_indexop_affinity = g_tpcc_config.WarehouseToCoreId(warehouse_id);
    }

    state->nodes =
        TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
            nullptr,
            KeyParam<Warehouse>(warehouse_key),
            KeyParam<District>(district_key),
            KeyParam<Customer>(customer_key));
  } else {
    // Partition
    state->nodes = NodeBitmap();
    if (g_tpcc_config.IsWarehousePinnable()) {
      if (Client::g_enable_pwv) {
        auto &gm = util::Instance<PWVGraphManager>();
        gm[warehouse_id - 1]->ReserveEdge(serial_id(), 2);
        gm[customer_warehouse_id - 1]->ReserveEdge(serial_id());
      }

      txn_indexop_affinity = warehouse_id - 1;
      state->nodes = TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
          nullptr,
          KeyParam<Warehouse>(warehouse_key),
          KeyParam<District>(district_key));

      txn_indexop_affinity = customer_warehouse_id - 1;
      state->nodes += TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
          nullptr,
          PlaceholderParam(2),
          KeyParam<Customer>(customer_key));
    } else {
      ASSERT_PWV_CONT;

      int parts[3] = {
        1,
        g_tpcc_config.PWVDistrictToCoreId(district_id, 0),
        g_tpcc_config.PWVDistrictToCoreId(customer_district_id, 20),
      };

      if (Client::g_enable_pwv) {
        auto &gm = util::Instance<PWVGraphManager>();
        for (auto part_id: parts)
          gm[part_id]->ReserveEdge(serial_id());
      }

      txn_indexop_affinity = parts[0]; // Warehouse(1) partition
      state->nodes = TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
          nullptr,
          KeyParam<Warehouse>(warehouse_key));

      txn_indexop_affinity = parts[1];
      state->nodes += TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
          nullptr,
          PlaceholderParam(),
          KeyParam<District>(district_key));

      txn_indexop_affinity = parts[2];
      state->nodes += TxnIndexLookup<TpccSliceRouter, PaymentState::Completion, void>(
          nullptr,
          PlaceholderParam(2),
          KeyParam<Customer>(customer_key));
    }
  }
}

void PaymentTxn::UpdateWarehouse(const State &state, const TxnHandle &index_handle,
                                 int payment_amount, int customer_warehouse_id)
{
  TxnRow vhandle = index_handle(state->warehouse);
  auto w = vhandle.Read<Warehouse::Value>();
  w.w_ytd += payment_amount;
  vhandle.Write(w);

  // Notify this node when warehouse_tax has a value. If
  // customer_node is the local node, then Signal() should simply
  // flip the boolean flag without transfer value over the network.
  int customer_node = TpccSliceRouter::SliceToNodeId(g_tpcc_config.WarehouseToSliceId(customer_warehouse_id));
  state->warehouse_tax_future.Subscribe(customer_node);

  state->warehouse_tax_future.Signal(w.w_tax);
}

void PaymentTxn::UpdateDistrict(const State &state, const TxnHandle &index_handle, int payment_amount)
{
  TxnRow vhandle = index_handle(state->district);
  auto d = vhandle.Read<District::Value>();
  d.d_ytd += payment_amount;
  vhandle.Write(d);
}

void PaymentTxn::UpdateCustomer(const State &state, const TxnHandle &index_handle, int payment_amount)
{
  TxnRow vhandle = index_handle(state->customer);
  auto c = vhandle.Read<Customer::Value>();
  auto tax = state->warehouse_tax_future.Wait();
  //auto tax = 0;
  auto amount = payment_amount + payment_amount * tax / 100;

  c.c_balance -= amount;
  c.c_ytd_payment += amount;
  c.c_payment_cnt++;
  vhandle.Write(c);
}

void PaymentTxn::Run()
{
  auto &conf = util::Instance<NodeConfiguration>();
  for (auto &p: state->nodes) {
    auto [node, bitmap] = p;

    if (conf.node_id() == node) {

      state->warehouse_future = UpdateForKey(
          node, state->warehouse,
          [](const auto &ctx, VHandle *row) {
            auto &[state, index_handle, payment_amount, customer_warehouse_id] = ctx;
            UpdateWarehouse(state, index_handle, payment_amount, customer_warehouse_id);
          }, payment_amount, customer_warehouse_id);

      state->district_future = UpdateForKey(
          node, state->district,
          [](const auto &ctx, VHandle *row) {
            auto &[state, index_handle, payment_amount] = ctx;
            UpdateDistrict(state, index_handle, payment_amount);
          }, payment_amount);


      state->customer_future = UpdateForKey(
          node, state->customer,
          [](const auto &ctx, VHandle *row) {
            auto &[state, index_handle, payment_amount] = ctx;
            UpdateCustomer(state, index_handle, payment_amount);
          }, payment_amount);

      if (!state->warehouse_future.has_callback()
          && !state->district_future.has_callback()
          && !state->customer_future.has_callback())
        continue;

      std::array<int, 3> filters;
      if (!Options::kEnablePartition) {
        filters = {0x07, 0, 0};
      } else if (g_tpcc_config.IsWarehousePinnable()) {
        filters = {0x03, 0x04, 0};
      } else {
        filters = {0x01, 0x02, 0x04};
      }

      for (auto filter: filters) {
        if (filter == 0) continue;
        auto aff = std::numeric_limits<uint64_t>::max();

        if (g_tpcc_config.IsWarehousePinnable()) {
          if (filter & 0x01) {
            aff = Config::WarehouseToCoreId(warehouse_id);
          } else if (filter == 0x04) {
            aff = Config::WarehouseToCoreId(customer_warehouse_id);
          }
        } else if (Options::kEnablePartition) {
          if (filter & 0x01) {
            aff = 1; // Warehouse(1) partition
          } else if (filter & 0x02) {
            aff = g_tpcc_config.PWVDistrictToCoreId(district_id, 0);
          } else if (filter & 0x04) {
            aff = g_tpcc_config.PWVDistrictToCoreId(customer_district_id, 20);
          }
        }

        root->AttachRoutine(
            MakeContext(payment_amount, customer_warehouse_id, bitmap, filter), node,
            [](const auto &ctx) {
              auto &[state, index_handle, payment_amount, customer_warehouse_id, bitmap, filter] = ctx;

              probes::TpccPayment{0, __builtin_popcount(bitmap), (int) state->warehouse->object_coreid()}();

              if ((bitmap & 0x01) && (filter & 0x01)) {
                state->warehouse_future.Invoke(state, index_handle, payment_amount, customer_warehouse_id);

                if (Client::g_enable_pwv) {
                  util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                      index_handle.serial_id(),
                      PWVGraph::VHandleToResource(state->warehouse));
                }
              }

              if ((bitmap & 0x02) && (filter & 0x02)) {
                state->district_future.Invoke(state, index_handle, payment_amount);

                ClientBase::OnUpdateRow(state->district);
                if (Client::g_enable_pwv) {
                  util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                      index_handle.serial_id(),
                      PWVGraph::VHandleToResource(state->district));
                }
              }

              if ((bitmap & 0x04) && (filter & 0x04)) {
                state->customer_future.Invoke(state, index_handle, payment_amount);

                if (Client::g_enable_pwv) {
                  util::Instance<PWVGraphManager>().local_graph()->ActivateResource(
                      index_handle.serial_id(),
                      PWVGraph::VHandleToResource(state->customer));
                }
              }
            },
            aff, (((bitmap & 0x04) && (filter & 0x04)) && !((bitmap & 0x01) && (filter & 0x01))) ? 1:0 ); //1:0 in the ternary to enable
            //we'll need to add a count for ourselves if the wait stays here but the signal doesn't
      }
    } else {
      root->AttachRoutine(
          MakeContext(bitmap, payment_amount, customer_warehouse_id), node,
          [](const auto &ctx) {
            auto &[state, index_handle, bitmap, payment_amount, customer_warehouse_id] = ctx;

            if (bitmap & 0x01) {
              UpdateWarehouse(state, index_handle, payment_amount, customer_warehouse_id);
            }

            if (bitmap & 0x02) {
              UpdateDistrict(state, index_handle, payment_amount);
            }

            if (bitmap & 0x04) {
              UpdateCustomer(state, index_handle, payment_amount);
            }
          }, std::numeric_limits<uint64_t>::max(), ((bitmap & 0x04) && !(bitmap & 0x01)) ? 1:0); //1:0 in the ternary to enable
          //this part is bound for another node, set the flags for if we're sending a wait
          //we'll add a count for the wait unless the signal is also going
    }
  }
}

}
