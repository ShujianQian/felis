#include <unistd.h>
#include <cstdio>

#include "module.h"
#include "node_config.h"
#include "console.h"
#include "log.h"
#include "epoch.h"

void show_usage(const char *progname)
{
  printf("Usage: %s -w workload -n node_name -c controller_ip -p cpu_count -s cpu_core_shift -m\n\n", progname);
  puts("\t-w\tworkload name");
  puts("\t-n\tnode name");
  puts("\t-c\tcontroller IP address");
  puts("\t-p\tCPU count");
  puts("\t-s\tCPU core shift offset");
  puts("\t-m\tturn on data migration mode");

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

  while ((opt = getopt(argc, argv, "w:n:c:p:s:m")) != -1) {
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
      case 'p':
        NodeConfiguration::g_nr_threads = atoi(optarg);
        break;
      case 's':
        NodeConfiguration::g_core_shifting = atoi(optarg);
        break;
      case 'm':
        NodeConfiguration::g_data_migration = true;
        break;
      default:
        show_usage(argv[0]);
        break;
    }
  }

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

  // init tables from the workload module
  Module<WorkloadModule>::InitModule(workload_name);

  logger->info("Ready. Waiting for run command from the controller.");
  console.WaitForServerStatus(felis::Console::ServerStatus::Running);

  abort_if(EpochClient::g_workload_client == nullptr,
           "Workload Module did not setup the EpochClient properly");

  logger->info("Starting workload");
  EpochClient::g_workload_client->Start();

  console.WaitForServerStatus(Console::ServerStatus::Exiting);

  return 0;
}
