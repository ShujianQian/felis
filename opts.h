#ifndef _OPTS_H
#define _OPTS_H

#include <string>
#include <vector>

namespace felis {

static inline long long ParseLargeNumber(std::string s)
{
  size_t pos;
  long long l = std::stoll(s, &pos);
  if (s.length() > pos) {
    auto ch = std::toupper(s[pos]);
    if (ch == 'G') l <<= 30;
    else if (ch == 'M') l <<= 20;
    else if (ch == 'K') l <<= 10;
  }
  return l;
}

struct Option {
  static inline std::vector<Option *> g_options;
  static inline std::vector<std::string> g_suffices;
  static inline std::vector<bool> g_present;

  int id;
  std::string prefix;
  bool is_arg;
  Option(const char *name, bool is_arg = true) : prefix(name), id(g_options.size()), is_arg(is_arg) {
    g_options.push_back(this);
    g_suffices.emplace_back();
    g_present.emplace_back(false);
  }

  operator bool() const { return g_present[id]; }
  std::string Get(const char *def = nullptr) const {
    if (*this)
      return g_suffices[id];
    else
      return def ? std::string(def) : std::string();
  }
  int ToInt(const char *def = nullptr) const {
    return std::stoi(Get(def));
  }
  unsigned long long ToULL(const char *def = nullptr) const {
    return std::strtoull(Get(def).c_str(), nullptr, 0);
  }
  long long ToLargeNumber(const char *def = nullptr) const {
    return ParseLargeNumber(Get(def));
  }
};

struct Options {

  // Long live the JVM style command line arguments!
  static inline const auto kCpu = Option("cpu");
  static inline const auto kMem = Option("mem");
  static inline const auto kOutputDir = Option("OutputDir");
  static inline const auto kDataMigration = Option("DataMigrationMode", false);
  static inline const auto kMaxNodeLimit = Option("MaxNodeLimit");
  static inline const auto kNoHugePage = Option("NoHugePage", false);

  static inline const auto kNrEpoch = Option("NrEpoch");
  static inline const auto kEpochSize = Option("EpochSize");
  static inline const auto kMajorGCThreshold = Option("MajorGCThreshold");
  static inline const auto kMajorGCLazy = Option("LazyMajorGC", false);
  static inline const auto kEpochQueueLength = Option("EpochQueueLength");
  static inline const auto kVHandleLockElision = Option("VHandleLockElision", false);
  static inline const auto kVHandleBatchAppend = Option("VHandleBatchAppend", false);
  static inline const auto kEnablePartition = Option("EnablePartition", false);
  static inline const auto kBatchAppendAlloc = Option("BatchAppendAlloc");

  // In 0.001 of txns per-epoch
  static inline const auto kCoreScaling = Option("CoreScaling");

  static inline const auto kOnDemandSplitting = Option("OnDemandSplitting");
  static inline const auto kAutoTuneThreshold = Option("AutoTuneThreshold", false);

  static inline const auto kBinpackSplitting = Option("BinpackSplitting", false);

  static inline const auto kTpccWarehouses = Option("TpccWarehouses");
  static inline const auto kTpccHotWarehouseBitmap = Option("TpccHotWarehouseBitmap");
  static inline const auto kTpccHotWarehouseLoad = Option("TpccHotWarehouseLoad");
  static inline const auto kTpccHashShard = Option("TpccHashShard", false);
  static inline const auto kTpccReadOnlyDelayQuery = Option("TpccReadOnlyDelayQuery", false);

  static inline const auto kYcsbContentionKey = Option("YcsbContentionKey");
  static inline const auto kYcsbSkewFactor = Option("YcsbSkewFactor");
  static inline const auto kYcsbReadOnly = Option("YcsbReadOnly");
  static inline const auto kYcsbDependency = Option("YcsbDependency", false);
  static inline const auto kYcsbDistFactor = Option("YcsbDistFactor");
  static inline const auto kYcsbPercentContended = Option("YcsbPercentContended");
  static inline const auto kYcsbNumKeyContended = Option("YcsbNumKeyContended");


  static inline const auto kEnableGranola = Option("EnableGranola", false);
  static inline const auto kEnablePWV = Option("EnablePWV", false);
  static inline const auto kPWVGraphAlloc = Option("PWVGraphAlloc");

  static inline const auto kUseCoroutineScheduler = Option("UseCoroSched", false);
  static inline const auto kCoroSchedSignalFuture = Option("CoroSchedSignalFuture", false);
  static inline const auto kOOOBufferSize = Option("OOOBufferSize");
  static inline const auto kPreemptStep = Option("PreemptStep");
  static inline const auto kMaxBackoff = Option("MaxBackoff");

  static inline bool ParseExtentedOptions(std::string arg)
  {
    for (auto o: Option::g_options) {
      bool match = false;
      if (o->is_arg) {
        match = (arg.compare(0, o->prefix.length(), o->prefix) == 0);
      } else {
        match = (arg == o->prefix);
      }
      if (match) {
        auto val = Option::g_suffices[o->id] = arg.substr(o->prefix.length());
        Option::g_present[o->id] = true;
        return true;
      }
    }
    return false;
  }

};

}

#endif
