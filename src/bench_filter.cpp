/*
  _                     _       __ _ _ _
 | |__   ___ _ __   ___| |__   / _(_) | |_ ___ _ __
 | '_ \ / _ \ '_ \ / __| '_ \ | |_| | | __/ _ \ '__|
 | |_) |  __/ | | | (__| | | ||  _| | | ||  __/ |
 |_.__/ \___|_| |_|\___|_| |_||_| |_|_|\__\___|_|

Benchmark meter: receives bench_source payloads, measures one-way latency
(receive time - header send time) and delivered throughput, and when the
source's end marker arrives writes the statistics to the JSON file named by
the `results` parameter (and publishes them once).

Payloads arrive as a classic blob (load_data) on tcp/ipc, or as a zero-copy
view into the producer's shared-memory ring (load_data_view, protocol P9+).
The ring is lossy: if the producer rewrites the slot while we read it, the
header changes under us. We re-read the header after using the payload and
count the message as torn (and drop its sample) if it changed.

Parameters ([bench_filter] section or -o):
  results  path of the JSON results file (required)
  warmup   payloads (by sequence number) excluded from the statistics
           (default 20)
  touch    read every payload byte (checksum), as a consumer that actually
           uses the data would (default false: only the header is read)
*/
#include <filter.hpp>
#include <nlohmann/json.hpp>
#include <pugg/Kernel.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <numeric>
#include <vector>

#include "bench_common.hpp"

#ifndef PLUGIN_NAME
#define PLUGIN_NAME "bench_filter"
#endif

using namespace std;
using json = nlohmann::json;

class BenchFilter : public Filter<json, json> {
public:
  using Filter::Filter;

  string kind() override { return PLUGIN_NAME; }

  void set_params(const json &params) override {
    Filter::set_params(params);
    _params = {{"results", ""}, {"warmup", 20}, {"touch", false}};
    _params.merge_patch(params);
    _results_path = _params["results"].get<string>();
    _warmup = _params["warmup"].get<uint64_t>();
    _touch = _params["touch"].get<bool>();
    _latencies.reserve(1 << 16);
  }

  return_type load_data(json const &input, string topic = "",
                        vector<unsigned char> const *blob = nullptr) override {
    if (input.contains("bench_end"))
      return end_of_run(input);
    if (blob == nullptr || blob->empty())
      return return_type::retry;
    return record(blob->data(), blob->size());
  }

#if PLUGIN_PROTOCOL_VERSION >= 9
  return_type load_data_view(json const &input, string topic,
                             BlobView blob) override {
    if (input.contains("bench_end"))
      return end_of_run(input);
    if (blob.data == nullptr)
      return return_type::retry;
    _views++;
    return record(blob.data, blob.size);
  }
#endif

  // Only reached for the end marker: publish the summary once.
  return_type process(json &out, vector<unsigned char> *blob = nullptr) override {
    if (!_publish_summary)
      return return_type::retry;
    _publish_summary = false;
    out = _summary;
    return return_type::success;
  }

  map<string, string> info() override {
    return {{"Results file", _results_path},
            {"Warmup", to_string(_warmup)},
            {"Touch payload", _touch ? "yes" : "no"}};
  }

private:
  // One payload: returns retry so the host does not call process() (nothing
  // to publish per message).
  return_type record(const unsigned char *data, size_t len) {
    const int64_t t_recv = bench::now_ns();
    bench::BenchHeader h;
    if (!bench::read_header(data, len, h)) {
      _bad++;
      return return_type::retry;
    }
    if (_touch) {
      uint64_t sum = 0;
      for (size_t i = 0; i < len; i++)
        sum += data[i];
      _checksum += sum;
    }
    // Torn read check (zero-copy path): the producer stamps the header last,
    // so a rewritten slot shows a different header.
    bench::BenchHeader again;
    if (!bench::read_header(data, len, again) || again.seq != h.seq ||
        again.t_send_ns != h.t_send_ns) {
      _torn++;
      return return_type::retry;
    }
    _received++;
    _size = len;
    if (h.seq < _warmup)
      return return_type::retry;
    if (_measured == 0) {
      _first_seq = h.seq;
      _t_first = t_recv;
    }
    _last_seq = h.seq;
    _t_last = t_recv;
    _measured++;
    _bytes += len;
    _latencies.push_back(t_recv - h.t_send_ns);
    return return_type::retry;
  }

  return_type end_of_run(json const &input) {
    if (_done)
      return return_type::retry;
    _done = true;
    _summary = summarize(input);
    if (!_results_path.empty()) {
      ofstream f(_results_path);
      if (!f) {
        _error = "cannot write results to " + _results_path;
        return return_type::error;
      }
      f << _summary.dump(2) << endl;
    }
    _publish_summary = true;
    return return_type::success;
  }

  json summarize(json const &end) const {
    json s;
    const uint64_t sent = end.value("sent", uint64_t{0});
    s["size"] = end.value("size", _size);
    s["sent"] = sent;
    s["received"] = _received;
    s["measured"] = _measured;
    s["warmup"] = _warmup;
    s["bad"] = _bad;
    s["torn"] = _torn;
    s["zero_copy_views"] = _views;
    // Payloads after warmup that never arrived (dropped by a full queue or
    // overwritten in the ring before we got to them).
    const uint64_t expected = sent > _warmup ? sent - _warmup : 0;
    s["lost"] = expected > _measured ? expected - _measured : 0;
    if (_latencies.empty())
      return s;

    vector<int64_t> lat = _latencies;
    sort(lat.begin(), lat.end());
    auto pct = [&](double p) {
      size_t i = static_cast<size_t>(p * (lat.size() - 1) + 0.5);
      return lat[min(i, lat.size() - 1)] / 1000.0; // us
    };
    const double mean =
        accumulate(lat.begin(), lat.end(), 0.0) / lat.size() / 1000.0;
    double var = 0;
    for (auto v : lat)
      var += pow(v / 1000.0 - mean, 2);
    s["latency_us"] = {{"min", lat.front() / 1000.0},
                       {"p50", pct(0.50)},
                       {"p90", pct(0.90)},
                       {"p99", pct(0.99)},
                       {"max", lat.back() / 1000.0},
                       {"mean", mean},
                       {"stddev", sqrt(var / lat.size())}};
    const double dt = (_t_last - _t_first) / 1e9;
    s["duration_s"] = dt;
    if (dt > 0 && _measured > 1) {
      // Rate over the intervals between the first and last measured message
      s["msg_per_s"] = (_measured - 1) / dt;
      s["mb_per_s"] = (_bytes - _size) / dt / 1e6;
    }
    return s;
  }

  string _results_path;
  uint64_t _warmup = 20;
  bool _touch = false;
  uint64_t _received = 0, _measured = 0, _bad = 0, _torn = 0, _views = 0;
  uint64_t _first_seq = 0, _last_seq = 0, _bytes = 0, _checksum = 0;
  size_t _size = 0;
  int64_t _t_first = 0, _t_last = 0;
  vector<int64_t> _latencies;
  bool _done = false, _publish_summary = false;
  json _summary;
};

MADS_REGISTER_PLUGINS(BenchFilter);

/*
                  _
  _ __ ___   __ _(_)_ __
 | '_ ` _ \ / _` | | '_ \
 | | | | | | (_| | | | | |
 |_| |_| |_|\__,_|_|_| |_|

Self-test (the plugin file is also an executable on macOS): feed synthetic
payloads straight in and print the summary the filter would write.
*/
int main(int argc, char const *argv[]) {
  BenchFilter filter;
  filter.set_params({{"warmup", 2}, {"touch", true}});
  const size_t size = 4096;
  vector<unsigned char> blob(size, 0x5a);
  for (uint64_t seq = 0; seq < 100; seq++) {
    bench::stamp(blob.data(), seq, size);
    filter.load_data({{"seq", seq}}, "bench", &blob);
  }
  json out;
  filter.load_data({{"bench_end", true}, {"sent", 100}, {"size", size}});
  filter.process(out);
  cout << out.dump(2) << endl;
  return 0;
}
