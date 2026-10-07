/*
  _                     _
 | |__   ___ _ __   ___| |__    ___  ___  _   _ _ __ ___ ___
 | '_ \ / _ \ '_ \ / __| '_ \  / __|/ _ \| | | | '__/ __/ _ \
 | |_) |  __/ | | | (__| | | | \__ \ (_) | |_| | | | (_|  __/
 |_.__/ \___|_| |_|\___|_| |_| |___/\___/ \__,_|_|  \___\___|

Benchmark source: publishes `count` binary payloads of `size` bytes, each
starting with a bench::BenchHeader (sequence number + send timestamp), then a
few JSON end markers telling bench_filter how many were sent.

The payload content is generated once; each message copies it into the
outgoing buffer (a std::vector on the classic path, the ring slot on the
zero-copy path) and stamps the header last, so every transport pays the same
"producer writes the data once" cost and the timestamp excludes it.

Parameters ([bench_source] section or -o):
  size         payload bytes, header included (default 1024)
  count        payloads to send (default 1000)
  zero_copy    write straight into the shared-memory slot when the agent has
               shm = true (protocol P9+; default true)
  end_every    period of the end markers sent after the last payload, until
               the agent is stopped (ms, default 100): a free-running source
               can leave a full queue behind, which drops anything sent
               while it drains, so the marker is repeated until it gets in
*/
#include <source.hpp>
#include <nlohmann/json.hpp>
#include <pugg/Kernel.h>

#include <algorithm>
#include <thread>
#include <vector>

#include "bench_common.hpp"

#ifndef PLUGIN_NAME
#define PLUGIN_NAME "bench_source"
#endif

using namespace std;
using json = nlohmann::json;

class BenchSource : public Source<json> {
public:
  using Source::Source;

  string kind() override { return PLUGIN_NAME; }

  void set_params(const json &params) override {
    Source::set_params(params);
    _params = {{"size", 1024}, {"count", 1000}, {"zero_copy", true},
               {"end_every", 100}};
    _params.merge_patch(params);
    _size = max<size_t>(_params["size"].get<size_t>(),
                        sizeof(bench::BenchHeader));
    _count = _params["count"].get<uint64_t>();
    _zero_copy = _params["zero_copy"].get<bool>();
    _end_every = chrono::milliseconds(_params["end_every"].get<int>());
    _pattern.resize(_size);
    for (size_t i = 0; i < _size; i++)
      _pattern[i] = static_cast<unsigned char>(i * 131 + 7);
    _blob_format = "bench";
  }

  return_type get_output(json &out,
                         vector<unsigned char> *blob = nullptr) override {
    if (_seq >= _count)
      return end_marker(out);
    if (blob == nullptr) {
      _error = "no blob buffer provided by the host";
      return return_type::critical;
    }
    blob->assign(_pattern.begin(), _pattern.end());
    bench::stamp(blob->data(), _seq, _size);
    out["seq"] = _seq++;
    return return_type::success;
  }

#if PLUGIN_PROTOCOL_VERSION >= 9
  bool supports_shared_output() const override { return _zero_copy; }

  return_type get_output_shared(json &out, BlobBuffer &buf) override {
    if (_seq >= _count)
      return end_marker(out); // buf.size stays 0: no blob
    if (buf.capacity < _size) {
      buf.size = _size; // overflow: the host falls back to get_output()
      return return_type::retry;
    }
    std::memcpy(buf.data, _pattern.data(), _size);
    bench::stamp(buf.data, _seq, _size);
    buf.size = _size;
    out["seq"] = _seq++;
    return return_type::success;
  }
#endif

  map<string, string> info() override {
    return {{"Payload size", to_string(_size) + " bytes"},
            {"Payload count", to_string(_count)},
            {"Zero-copy output", _zero_copy ? "requested" : "off"}};
  }

private:
  // After the last payload: a JSON-only end marker every _end_every until the
  // agent is stopped (the filter uses the first that gets through).
  return_type end_marker(json &out) {
    if (_ends_sent > 0)
      this_thread::sleep_for(_end_every);
    _ends_sent++;
    out = {{"bench_end", true}, {"sent", _count}, {"size", _size}};
    return return_type::success;
  }

  size_t _size = 1024;
  uint64_t _count = 1000;
  uint64_t _seq = 0;
  bool _zero_copy = true;
  chrono::milliseconds _end_every{100};
  uint64_t _ends_sent = 0;
  vector<unsigned char> _pattern;
};

MADS_REGISTER_PLUGINS(BenchSource);

/*
                  _
  _ __ ___   __ _(_)_ __
 | '_ ` _ \ / _` | | '_ \
 | | | | | | (_| | | | | |
 |_| |_| |_|\__,_|_|_| |_|

Self-test (the plugin file is also an executable on macOS): generate a few
payloads and check their headers.
*/
int main(int argc, char const *argv[]) {
  BenchSource source;
  json out;
  vector<unsigned char> blob;
  source.set_params({{"size", 4096}, {"count", 3}, {"end_every", 1}});
  for (int i = 0; i < 4; i++) {
    out.clear();
    blob.clear();
    source.get_output(out, &blob);
    bench::BenchHeader h{};
    bool ok = bench::read_header(blob.data(), blob.size(), h);
    cout << out.dump() << " blob " << blob.size() << " bytes"
         << (blob.empty() ? "" : ok ? ", header ok" : ", BAD header") << endl;
  }
  return 0;
}
