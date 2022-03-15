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
  logger->set_pattern("[%H:%M:%S.%e thr-%t] %l %v");
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
