#include "deposit_checking.h"

namespace smallbank {

template <>
DepositCheckingStruct ClientBase::GenerateTransactionInput<DepositCheckingStruct>() {
  DepositCheckingStruct s;
  s.account_id = PickAccount();
  s.deposit_v = RandomNumber(1, 100);
  return s;
}

DepositCheckingTxn::DepositCheckingTxn(Client *client, uint64_t serial_id)
    : Txn<DepositCheckingState>(serial_id),
      DepositCheckingStruct(client->GenerateTransactionInput<DepositCheckingStruct>()),
      client(client) {}

void DepositCheckingTxn::Prepare() {
  INIT_ROUTINE_BRK(8192);
  auto &mgr = util::Instance<TableManager>();
  void *buf = alloca(512);
  auto account_key = Account::Key::New(account_id);
  auto account_ptr = mgr.Get<Account>().Search(account_key.EncodeView(buf));
  // shirley note: this is a hack bc account table never changes so we can directly read from index_info using sid = 1
  uint64_t cid = account_ptr->ReadWithVersion(1)->template ToType<Account::Value>().CustomerID;

  auto checking_key = Checking::Key::New(cid);
  TxnIndexLookup<DummySliceRouter, DepositCheckingState::Completion, void>(
      nullptr, KeyParam<Checking>(checking_key));
}

void DepositCheckingTxn::Run() {
  auto aff = std::numeric_limits<uint64_t>::max();

  root->AttachRoutine(
    MakeContext(deposit_v), 1,
    [](const auto &ctx) {
      auto &[state, index_handle, deposit_v] = ctx;
      TxnRow vhandle = index_handle(state->checking);
      auto ck = vhandle.Read<Checking::Value>();
      ck.BalanceCk += deposit_v;
      vhandle.Write(ck);
    },
    aff
  );

  // shirley zen: add sfence after txn run
  // _mm_sfence();
}

} // namespace smallbank