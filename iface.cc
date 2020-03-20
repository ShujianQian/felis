// I really hope this could be a .cc file. However, Clang has problems with
// devirtualizing those virtual calls, even with -fvisibility=hidden.
//
// Only include this file from a .cc file!!!

#include "util.h"
#include "tcp_node.h"
#include "epoch.h"
#include "vhandle_sync.h"

namespace util {

using namespace felis;

IMPL(PromiseRoutineTransportService, TcpNodeTransport);
IMPL(PromiseRoutineDispatchService, EpochExecutionDispatchService);
IMPL(VHandleSyncService, SpinnerSlot);
// IMPL(VHandleSyncService, SimpleSync);
IMPL(PromiseAllocationService, EpochPromiseAllocationService);

}
