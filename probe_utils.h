// -*- mode: c++ -*-
#ifndef PROBE_UTILS_H
#define PROBE_UTILS_H

#include <cstring>
#include <cmath>
#include <mutex>
#include <set>
#include <iostream>
#include <iomanip>
#include <limits>


template <typename T> void OnProbe(T t);

#define PROBE_PROXY(klass) void klass::operator()() const { OnProbe(*this); }

namespace agg {

// aggregations
#define AGG(ins) decltype(global.ins)::Value ins = global.ins

template <typename Impl>
class Agg : public Impl {
 public:
  struct Value : public Impl {
    Agg *parent;
    Value(Agg &agg) : parent(&agg) {
      parent->Add(this);
    }
    ~Value() {
      parent->Remove(this);
    }
  };

  void Add(Value *node) {
    std::lock_guard<std::mutex> _(m);
    values.insert(node);
  }

  void Remove(Value *node) {
    std::lock_guard<std::mutex> _(m);
    (*this) << *node;
    values.erase(node);
  }

  Impl operator()() {
    Impl o;
    o << *this;
    std::lock_guard<std::mutex> _(m);
    for (Value *e: values) {
      o << *e;
    }
    return o;
  }

 protected:
  std::set<Value *> values;
  std::mutex m;
};

struct Sum {
  long sum = 0;
  Sum &operator<<(long value) {
    sum += value;
    return *this;
  }
  Sum &operator<<(const Sum &rhs) {
    sum += rhs.sum;
    return *this;
  }
};

static inline std::ostream &operator<<(std::ostream &out, const Sum &s)
{
  out << s.sum;
  return out;
}

struct Average {
  long sum = 0;
  long cnt = 0;
  Average &operator<<(long value) {
    sum += value;
    cnt++;
    return *this;
  }
  Average &operator<<(const Average &rhs) {
    sum += rhs.sum;
    cnt += rhs.cnt;
    return *this;
  }
};

static inline std::ostream &operator<<(std::ostream &out, const Average &avg)
{
  out << 1.0l * avg.sum / avg.cnt;
  return out;
}

template <int N = 128, int Offset = 0, int Bucket = 100>
struct Histogram {
  long hist[N];
  Histogram() {
    memset(hist, 0, sizeof(long) * N);
  }
  Histogram &operator<<(long value) {
    long idx = (value - Offset) / Bucket;
    if (idx >= 0 && idx < N) hist[idx]++;
    if (idx >= N) hist[N-1]++;
    return *this;
  }
  Histogram &operator<<(const Histogram &rhs) {
    for (int i = 0; i < N; i++) hist[i] += rhs.hist[i];
    return *this;
  }
  int CalculatePercentile(double scale) {
    size_t total_nr = Count();
    long medium_idx = total_nr * scale;
    for (int i = 0; i < N; i++) {
      medium_idx -= hist[i];
      if (medium_idx < 0)
        return i * Bucket + Offset;
    }
    return Offset; // 0?
  }
  int CalculateMedian() { return CalculatePercentile(0.5); }
  size_t Count() {
    size_t total_nr = 0;
    for (int i = 0; i < N; i++)
      total_nr += hist[i];
    return total_nr;
  }
};

template <int N, int Offset, int Bucket>
std::ostream &operator<<(std::ostream &out, const Histogram<N, Offset, Bucket>& h)
{
  long last = std::numeric_limits<long>::max();
  bool repeat = false;
  long unit = 0;
  for (int i = 0; i < N; i++)
    if (unit < h.hist[i] / 100) unit = h.hist[i] / 100;

  for (int i = 0; i < N; i++) {
    long start = i * Bucket + Offset;
    if (i == N - 1) {
      out << std::setw(10) << " "
          << ">= "
          << std::setw(10) << start
          << ": "
          << std::setw(10) << h.hist[i] << " ";
    } else {
      out << std::setw(10) << start
          << " - "
          << std::setw(10) << start + Bucket
          << ": "
          << std::setw(10) << h.hist[i] << " ";
    }
    if (unit > 0)
      for (int j = 0; j < h.hist[i] / unit; j++)
        out << '@';

    out << std::endl;

    last = h.hist[i];
  }
  return out;
}

template <int N = 10, int Offset = 0, int Base = 2>
struct LogHistogram {
  static constexpr int kNrBins = N;
  long hist[N];
  long cnt;
  LogHistogram() {
    memset(hist, 0, sizeof(long) * N);
    cnt = 0;
  }
  LogHistogram &operator<<(long value) {
    cnt++;
    if (value <= 0) return *this;
    long idx = std::log2(value) / std::log2(Base) - Offset;
    if (idx >= 0 && idx < N) hist[idx]++;
    if (idx >= N) hist[N - 1]++;
    return *this;
  }
  LogHistogram &operator<<(const LogHistogram &rhs) {
    for (int i = 0; i < N; i++) hist[i] += rhs.hist[i];
    cnt += rhs.cnt;
    return *this;
  }
};

template <int N = 10, int Offset, int Base = 2>
std::ostream &operator<<(std::ostream &out, const LogHistogram<N, Offset, Base> &h)
{
  long unit = 0;
  for (int i = 0; i < N; i++)
    if (unit < h.hist[i] / 100) unit = h.hist[i] / 100;
  long start = long(std::pow(Base, Offset));
  for (int i = 0; i < N; i++) {
    long end = start * Base;
    if (i == N - 1) {
      out << std::setw(10) << " "
          << ">= "
          << std::setw(10) << start
          << ": "
          << std::setw(10) << h.hist[i] << " ";
    } else {
      out << std::setw(10) << start
          << " - "
          << std::setw(10) << end
          << ": "
          << std::setw(10) << h.hist[i] << " ";
    }
    if (unit > 0)
      for (int j = 0; j < h.hist[i] / unit; j++)
        out << '@';
    out << std::endl;
    start = end;
  }
  return out;
}

struct TimeValue {
  uint64_t epoch;
  uint64_t time;
  uint64_t val;
};

template <int NumEpoch = 20, int BucketSize = 1000, int NumBucket = 100, int Height = 20, bool AggPrint = true>
struct TimeAvgPlot {
  uint64_t cnt[NumEpoch][NumBucket] = {};
  uint64_t sum[NumEpoch][NumBucket] = {};
  TimeAvgPlot &operator<<(TimeValue tv) {
    if (tv.epoch > NumEpoch) return *this;
    long idx = tv.time / BucketSize;
    if (idx >= 0 && idx < NumBucket) {
      sum[tv.epoch][idx] += tv.val;
      cnt[tv.epoch][idx]++;
    }
    return *this;
  }
  TimeAvgPlot &operator<<(const TimeAvgPlot &rhs) {
    for (int i = 0; i < NumEpoch; i++) {
      for (int j = 0; j < NumBucket; j++) {
        sum[i][j] += rhs.sum[i][j];
        cnt[i][j] += rhs.cnt[i][j];
      }
    }
    return *this;
  }
};

template <int NumEpoch = 20, int BucketSize = 1000, int NumBucket = 100, int Height = 20, bool AggPrint = true>
std::ostream &operator<<(std::ostream &out, const TimeAvgPlot<NumEpoch, BucketSize, NumBucket, Height, AggPrint> &h)
{
  int64_t unit = 1;
  double avg[NumBucket];
  auto printRow = [&unit, &avg]() {
    for (int i = 0; i < Height; i++) {
      std::cout << std::setw(10) << unit * (Height - i) << " ";
      for (int j = 0; j < NumBucket; j++) {
        std::cout << ((avg[j] > unit * (Height - i - 1)) ? "#" : " ");
      }
      std::cout << std::endl;
    }
    std::cout << std::setw(10) << 0 << " ";
    for (int i = 0; i < NumBucket; i++) {
      std::cout << (((i + 1) % 10 == 0) ? "+" : "-");
    }
    std::cout << std::endl;
  };

  if (AggPrint) {
    for (int i = 0; i < NumBucket; i++) {
      double sum = 0, cnt = 0;
      for (int j = 0; j < NumEpoch; j++) {
        sum += h.sum[j][i];
        cnt += h.cnt[j][i];
      }
      avg[i] = sum / cnt;
      unit = std::max(unit, static_cast<int64_t>(std::ceil(avg[i] / Height)));
    }
    printRow();
  } else {
    for (int i = 0; i < NumEpoch; i++) {
      for (int j = 0; j < NumBucket; j++) {
        double sum = h.sum[i][j];
        double cnt = h.cnt[i][j];
        unit = std::max(unit, static_cast<int64_t>(std::ceil(sum / cnt / Height)));
      }
    }

    for (int i = 0; i < NumEpoch; i++) {
      for (int j = 0; j < NumBucket; j++) {
        double sum = h.sum[i][j];
        double cnt = h.cnt[i][j];
        avg[j] = sum / cnt;
      }
      printRow();
    }
  }
  return out;
}

}


#endif /* PROBE_UTILS_H */
