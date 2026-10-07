/*
Shared definitions of the transport benchmark plugins.

Every payload starts with a BenchHeader, written by bench_source right before
the payload is handed to MADS and read by bench_filter right after it is
received, so the measured one-way latency covers only the transport (agent
publish -> broker -> agent receive), not payload generation.

Timestamps come from std::chrono::steady_clock, which is system-wide on all
supported platforms (mach_absolute_time on macOS, CLOCK_MONOTONIC on Linux,
QueryPerformanceCounter on Windows): source and filter must run on the same
host, which is the point of this benchmark anyway.
*/
#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>

namespace bench {

constexpr uint64_t MAGIC = 0x4D414453'42454E43ull; // "MADSBENC"

struct BenchHeader {
  uint64_t magic;
  uint64_t seq;
  int64_t t_send_ns; // steady_clock, nanoseconds
  uint64_t size;     // total payload bytes, header included
};

inline int64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Writes the header at the start of `payload` (which must hold at least
// sizeof(BenchHeader) bytes), stamping the send time last.
inline void stamp(unsigned char *payload, uint64_t seq, uint64_t size) {
  BenchHeader h{MAGIC, seq, 0, size};
  h.t_send_ns = now_ns();
  std::memcpy(payload, &h, sizeof h);
}

// Reads the header back; false if the payload is too short or not ours.
inline bool read_header(const unsigned char *payload, size_t len,
                        BenchHeader &h) {
  if (payload == nullptr || len < sizeof h)
    return false;
  std::memcpy(&h, payload, sizeof h);
  return h.magic == MAGIC && h.size == len;
}

} // namespace bench
