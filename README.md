# MADS transport benchmark

This suite measures latency and throughput between [MADS](https://github.com/pbosetti/MADS)
agents running on the same host as the broker, over each data path MADS offers:

| mode      | agent ↔ broker   | payload bytes                                 |
|-----------|------------------|-----------------------------------------------|
| `tcp`     | loopback tcp     | inside the ZeroMQ messages                    |
| `ipc`     | `ipc://` sockets | inside the ZeroMQ messages                    |
| `ipc+shm` | `ipc://` sockets | shared-memory ring; only a descriptor travels |
| `tcp+shm` | loopback tcp     | shared-memory ring (optional mode)            |

Each run starts a fresh `mads-broker`, plus two agents: a `mads-filter` running
`bench_filter.plugin` (the meter) and a `mads-source` running
`bench_source.plugin` (the payload generator).

You only need an installed MADS package and a C++20 compiler, not the MADS
sources. Run the suite on several machines and compare them with
`compare.py`.

## Requirements

- **MADS**, installed, with `mads` on the `PATH`. The `ipc` and `shm` modes
  need MADS ≥ 2.6. With an older MADS only `tcp` runs, and the other modes are
  skipped with a note.
- **Build tools:** CMake ≥ 3.28, a C++20 compiler and Ninja (recommended).
- **Python ≥ 3.9**, standard library only.

## Build

```sh
cmake -Bbuild -G Ninja && cmake --build build
```

The plugins must match the MADS that loads them: same plugin protocol, pugg
and nlohmann/json versions. CMake reads those from the installed package's
`<prefix>/share/plugin_deps.json` (the manifest `mads plugin` uses), where
`<prefix>` is the output of `mads -p`, and fetches exactly those versions.

| CMake option | use |
|---|---|
| `-DMADS_PREFIX=<dir>` | a MADS install that is not on the `PATH` |
| `-DMADS_BUILD_DIR=<MADS>/build` | benchmark a MADS development build. Headers and dependencies come from its build tree, with no download. |
| `-DMADS_PLUGIN_SOURCE_DIR=<dir>` | a local `mads_plugin` checkout for the plugin headers |

The build writes `build/bench_build_info.json`, which records the MADS and
plugin protocol it built against. `run_bench.py` then uses those same MADS
binaries by default.

With protocol P8 headers (MADS < 2.6) the zero-copy hooks are compiled out.
On macOS the plugin files are also executables: `./build/bench_source.plugin`
and `./build/bench_filter.plugin` run a small self-test.

## Run

```sh
python3 run_bench.py --repeat 3                    # all sizes and modes, both kinds
python3 run_bench.py --sizes 64,4K,1M,16M --kind latency --label "lab-pc"
python3 run_bench.py --modes tcp,ipc,ipc+shm,tcp+shm --touch
```

| option | meaning |
|---|---|
| `--label` | name of this machine/config in summaries and comparisons (default: host name) |
| `--sizes`, `--modes`, `--kind`, `--repeat` | the test matrix; modes are interleaved within each repeat |
| `--touch` | the filter reads every payload byte, like a consumer that uses the data |
| `--shm-slots` | ring depth for the `shm` modes (default 16; see below) |
| `--no-zero-copy` | the `shm` modes copy into the ring instead of writing in place |
| `--ipc-sockbuf` | kernel buffers of ipc connections. Default: MADS's own (1 MiB); `0` = OS default |
| `--sockbuf` | `sndbuf`/`rcvbuf` for every connection (tcp too) |
| `--budget` | payload bytes per throughput run (default 1G); this also bounds queue memory |
| `--mads-bin`, `--mads-lib` | run other MADS binaries than the ones the plugins were built against |
| `--port` | base port (`port`, `+1`, `+2`); keep it clear of a running broker |

Results go to `results/<timestamp>/`:
- `summary.md`: medians over repeats, ratios against `tcp`, and the machine
  details
- `results.csv` and `results.json`: every run
- `meta.json`: machine, OS, MADS version, plugin protocol, arguments
- `runs/<run>/`: each run's generated `mads.ini`, its three logs and the
  filter's raw results

## Compare machines

```sh
python3 compare.py results/20261007-102836 results-from-nuc/20261008-091500 -o comparison.md
```

This prints one table per metric: latency p50 (or `--metric p90|p99|mean`),
MB/s and msg/s. There is a row per size and mode and a column per result set.
A header table lists each machine's MADS version, CPU, RAM and OS.

## What is measured

- **Latency.** The source is paced so that queues stay empty: at most
  `--latency-bw` (default 100 MB/s) and at least one message every
  `--latency-period` ms. It stamps a `steady_clock` time into the payload
  header right before publishing, and the filter subtracts it from its own
  receive time. The result is one-way latency: source agent → broker →
  filter agent. `steady_clock` is system-wide on macOS, Linux and Windows, so
  both processes agree.
- **Throughput.** The source runs free for `--budget` bytes. The filter
  reports the messages/s and MB/s actually delivered, over the span between
  the first and the last measured message.
- **`lost`.** Payloads that never arrived. A free-running publisher overflows
  the ZeroMQ queues, which drop messages.
- **The shared-memory ring is lossy by design.** A slot rewritten before the
  consumer reads it is dropped. With small payloads the producer laps a
  16-slot ring in microseconds while descriptors wait in the queues, so
  almost nothing survives. The summary shows `-` when fewer than 1% of the
  payloads were measured. Use `--shm-slots 1100` (deeper than the queue) to
  measure capacity instead.
- **`torn`.** Payloads whose header changed while the filter read them
  (overwritten mid-read); those samples are discarded.
- **Generation cost is the same on every path.** The payload is generated
  once, and every message copies it into its outgoing buffer: a vector on
  the classic path, the ring slot on the zero-copy path. That models a
  producer that writes its data once. The header is stamped after the copy.
- **Warmup.** The first `--warmup` payloads are excluded from the statistics.
- **Fallbacks are flagged.** The script checks each agent's log for the
  transport and the shared-memory path it actually used, and warns on a
  silent fallback.

## Caveats

- **Same host only.** Cross-host tcp would need a different driver.
- **Startup delay over tcp.** A tcp subscription can take over half a second
  to become effective at the broker. `--settle` (default 2 s) waits between
  starting the filter and the source; lower it at your own risk.
- **Platform buffer defaults.** Unix-socket buffer defaults differ widely:
  8 KiB on macOS, about 200 KiB on Linux. `meta.json` records them. MADS ≥ 2.6
  sets 1 MiB for ipc connections (`ipc_sndbuf`/`ipc_rcvbuf`).
