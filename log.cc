#include <sys/time.h>
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/ansicolor_sink.h"
#include "opts.h"
#include "log.h"
#include "literals.h"

std::unique_ptr<spdlog::logger> logger(nullptr);

void InitializeLogger(const std::string &hostname)
{
  std::shared_ptr<spdlog::sinks::sink> file_sink;

  if (felis::Options::kOutputDir) {
    file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        felis::Options::kOutputDir.Get() + "/" + hostname + ".log",
        500_M, 100);
  } else {
    file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
      "dbg-" + hostname + ".log", true);
  }
  file_sink->set_level(spdlog::level::debug);

  auto console_sink = std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>();
  console_sink->set_level(spdlog::level::info);

  logger.reset(new spdlog::logger("global", {file_sink, console_sink}));
  logger->flush_on(spdlog::level::debug);

  const char *log_level = getenv("LOGGER");
  if (log_level == nullptr) {
#ifdef NDEBUG
    log_level = "info";
#else
    log_level = "debug";
#endif
  }

  logger->set_level(spdlog::level::from_str(log_level));
  logger->set_pattern("[%H:%M:%S.%e thr-%t] %v");
}

PerfLog::PerfLog()
    : is_started(false), duration(0)
{
  Start();
}

void PerfLog::Start()
{
  if (!is_started) {
    is_started = true;
    gettimeofday(&tv, NULL);
  }
}

void PerfLog::End()
{
  if (is_started) {
    is_started = false;
    struct timeval newtv;
    gettimeofday(&newtv, NULL);
    duration += (newtv.tv_sec - tv.tv_sec) * 1000
                + (newtv.tv_usec - tv.tv_usec) / 1000;
  }
}

void PerfLog::Show(const char *msg)
{
  if (is_started) End();
  logger->info("{} {}ms", msg, duration);
}

void PerfLog::Clear()
{
  if (!is_started) duration = 0;
}

static std::string format_size(uint64_t size)
{
  const std::string sizes[] {"B", "KB", "MB", "GB", "TB"};
  int order = 0;
  auto dsize = static_cast<double>(size);
  while (dsize > 1024) {
    dsize /= 1024.0;
    order++;
  }
  return fmt::format("{:.2f}{}", dsize, sizes[order]);
}

void log_mem_usage()
{
  FILE *f = fopen("/proc/self/statm", "r");
  uint64_t size, resident, share, text, lib, data, dirty;
  const uint64_t page_size = 64;
  fscanf(f, "%lu %lu %lu %lu %lu %lu %lu", &size, &resident, &share, &text, &lib, &data, &dirty);
  fclose(f);
  size *= page_size;
  resident *= page_size;
  share *= page_size;
  text *= page_size;
  lib *= page_size;
  data *= page_size;
  dirty *= page_size;
  logger->info("[Memory Usage Stat] size {}, res {}, shared {}, text {}, lib {}, data {}, dt {}",
               format_size(size), format_size(resident), format_size(share), format_size(text), format_size(lib),
               format_size(data), format_size(dirty));
}
