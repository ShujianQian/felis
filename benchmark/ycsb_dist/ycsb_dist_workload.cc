#include "ycsb_dist.h"
#include "module.h"
#include "opts.h"

namespace felis {

class YcsbDistModule : public Module<WorkloadModule> {
 public:
  YcsbDistModule() {
    info = {
      .name = "ycsb_dist",
      .description = "YCSB Distributed",
    };
  }
  void Init() override final {
    Module<CoreModule>::InitModule("node-server");
    Module<CoreModule>::InitModule("allocator");

    if (Options::kYcsbContentionKey) {
      ycsb_dist::Client::g_contention_key = Options::kYcsbContentionKey.ToInt();
    }
    if (Options::kYcsbSkewFactor) {
      ycsb_dist::Client::g_theta = 0.01 * Options::kYcsbSkewFactor.ToInt();
    }
    if (Options::kYcsbReadOnly) {
      ycsb_dist::Client::g_extra_read = Options::kYcsbReadOnly.ToInt();
    }
    if (Options::kYcsbDistFactor) {
      ycsb_dist::Client::g_dist_factor = Options::kYcsbDistFactor.ToInt();
    }
    ycsb_dist::Client::g_dependency = Options::kYcsbDependency;

    auto loader = new ycsb_dist::YcsbDistLoader();
    go::GetSchedulerFromPool(1)->WakeUp(loader);
    loader->Wait();

    EpochClient::g_workload_client = new ycsb_dist::Client();
  }
};

static YcsbDistModule ycsb_dist_module;

}
