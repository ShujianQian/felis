#include <unistd.h>
#include <cstdio>
#include <string_view>

#include "module.h"
#include "node_config.h"
#include "priority.h"
#include "tcp_node.h"
#include "console.h"
#include "log.h"
#include "epoch.h"
#include "opts.h"

void show_usage(const char *progname)
{
  printf("Usage: %s -w workload -n node_name -c controller_ip -p cpu_count -s cpu_core_shift -m\n\n", progname);
  puts("\t-w\tworkload name");
  puts("\t-n\tnode name");
  puts("\t-c\tcontroller IP address");

  puts("\nSee opts.h for extented options.");

  std::exit(-1);
}

#ifdef EL6_COMPAT

// Platform Supports
// We need to support EL6, which uses glibc 2.12
extern "C" {
  // Looks like we are not using memcpy anyway, so why don't we just make EL6 happy?
  asm (".symver memcpy, memcpy@GLIBC_2.2.5");
  asm (".symver getenv, getenv@GLIBC_2.2.5");
  asm (".symver clock_gettime, clock_gettime@GLIBC_2.2.5");
  void *__wrap_memcpy(void *dest, const void *src, size_t n) { return memcpy(dest, src, n); }
  char *__wrap_secure_getenv(const char *name) { return getenv(name); }
  int __wrap_clock_gettime(clockid_t clk_id, struct timespec *tp) { return clock_gettime(clk_id, tp); }
}
#endif

namespace felis {

void ParseControllerAddress(std::string arg);

}

using namespace felis;

int main(int argc, char *argv[])
{
  int opt;
  std::string workload_name;
  std::string node_name;

  while ((opt = getopt(argc, argv, "w:n:c:X:")) != -1) {
    switch (opt) {
      case 'w':
        workload_name = std::string(optarg);
        break;
      case 'n':
        node_name = std::string(optarg);
        break;
      case 'c':
        ParseControllerAddress(std::string(optarg));
        break;
      case 'X':
        if (!Options::ParseExtentedOptions(std::string(optarg))) {
          fprintf(stderr, "Ignoring extended argument %s\n", optarg);
        }
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }


  NodeConfiguration::g_nr_threads = Options::kCpu.ToInt("4");

  if (Options::kCoreShifting) {
    NodeConfiguration::g_core_shifting = Options::kCoreShifting.ToInt();
  }
  NodeConfiguration::g_data_migration = Options::kDataMigration;
  if (Options::kEpochSize)
    EpochClient::g_txn_per_epoch = Options::kEpochSize.ToInt();

  Module<CoreModule>::ShowAllModules();
  Module<WorkloadModule>::ShowAllModules();
  puts("\n");

  if (node_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  if (workload_name == "") {
    show_usage(argv[0]);
    return -1;
  }

  auto &console = util::Instance<Console>();
  console.set_server_node_name(node_name);

  Module<CoreModule>::InitRequiredModules();

  util::InstanceInit<NodeConfiguration>();
  util::Instance<NodeConfiguration>().SetupNodeName(node_name);
  util::InstanceInit<TcpNodeTransport>();

  logger->info("Priority Txn sercive status {}", Options::kPriorityTxn);
  if (Options::kPriorityTxn) {
    NodeConfiguration::g_priority_txn = true;
    util::InstanceInit<PriorityTxnService>();
  }
  logger->info("Priority Txn batch mode {}", Options::kPriorityBatchMode);
  if (Options::kPriorityBatchMode) {
    abort_if(Options::kPriorityTxn, "Cannot turn on both PriorityTxn and PriorityBatchMode");
    NodeConfiguration::g_priority_batch_mode = true;
    NodeConfiguration::g_priority_batch_mode_pct = Options::kPercentagePriorityTxn.ToInt();
  }

  // init tables from the workload module
  Module<WorkloadModule>::InitModule(workload_name);

  auto client = EpochClient::g_workload_client;
  logger->info("Generating Benchmarks...");
  client->GenerateBenchmarks();

  console.UpdateServerStatus(Console::ServerStatus::Listening);
  logger->info("Ready. Waiting for run command from the controller.");
  console.WaitForServerStatus(felis::Console::ServerStatus::Running);

  abort_if(EpochClient::g_workload_client == nullptr,
           "Workload Module did not setup the EpochClient properly");

  printf("\n");
  logger->info("Starting workload");
  client->Start();

  console.WaitForServerStatus(Console::ServerStatus::Exiting);
  go::WaitThreadPool();

  return 0;
}
