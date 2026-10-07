#!/usr/bin/env python3
"""MADS transport benchmark: loopback tcp vs ipc vs ipc + shared memory.

For every (kind, payload size, mode) combination this script starts a fresh
mads-broker, a mads-filter running bench_filter.plugin and a mads-source
running bench_source.plugin, all on this host, waits for the filter's
results file and collects the statistics.

Modes (the data path between the agents and the broker):
  tcp       loopback tcp                       (--transport tcp)
  ipc       ipc:// sockets                     (--transport ipc)
  ipc+shm   ipc:// + shared-memory payloads    (--transport ipc, shm = true)
  tcp+shm   loopback tcp + shared memory       (--transport tcp, shm = true)

Kinds:
  latency     paced source (low rate, empty queues): one-way latency
  throughput  free-running source: delivered messages/s and MB/s (bounded
              by --budget bytes per run, which also bounds queue memory)

MADS binaries: by default the ones the plugins were built against (see
build/bench_build_info.json, written by CMake), else `mads -p`/bin. Modes
the installed MADS cannot run (ipc and shared memory need a MADS with
--transport) are skipped with a note.

Only the Python standard library is used. Example:
  python3 run_bench.py --sizes 64,4K,1M,16M --kind both --repeat 3
"""
import argparse
import csv
import json
import math
import os
import platform
import re
import shutil
import socket
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
IS_WINDOWS = os.name == "nt"

MODES = {
    "tcp": {"transport": "tcp", "shm": False},
    "ipc": {"transport": "ipc", "shm": False},
    "ipc+shm": {"transport": "ipc", "shm": True},
    "tcp+shm": {"transport": "tcp", "shm": True},
}
HEADER_BYTES = 32  # sizeof(bench::BenchHeader)


def parse_size(text):
    text = text.strip().upper()
    mult = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}
    if text and text[-1] in mult:
        return int(float(text[:-1]) * mult[text[-1]])
    return int(text)


def human_size(n):
    for unit, div in (("M", 1 << 20), ("K", 1 << 10)):
        if n >= div and n % div == 0:
            return f"{n // div}{unit}"
    return str(n)


def exe(directory, name):
    return Path(directory) / (name + (".exe" if IS_WINDOWS else ""))


def find_plugin(plugin_dir, name):
    direct = Path(plugin_dir) / f"{name}.plugin"
    if direct.exists():
        return direct
    found = sorted(Path(plugin_dir).rglob(f"{name}.plugin"))
    if not found:
        sys.exit(f"Cannot find {name}.plugin under {plugin_dir}: build the "
                 f"benchmark first (cmake -Bbuild && cmake --build build)")
    return found[0]


def run_text(cmd, timeout=10):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=timeout, errors="replace")
        return r.stdout + r.stderr
    except (OSError, subprocess.SubprocessError):
        return ""


def machine_info():
    """Hardware/OS facts that make results from different machines
    comparable. Best effort: missing entries are left out."""
    info = {"host": socket.gethostname(), "os": platform.platform(),
            "arch": platform.machine(), "cpus": os.cpu_count(),
            "python": platform.python_version()}
    sysname = platform.system()
    if sysname == "Darwin":
        def sysctl(key):
            return run_text(["sysctl", "-n", key]).strip()
        info["cpu"] = sysctl("machdep.cpu.brand_string")
        mem = sysctl("hw.memsize")
        info["ram_gb"] = round(int(mem) / 2**30, 1) if mem.isdigit() else None
        info["unix_sockbuf"] = {k: sysctl(f"net.local.stream.{k}")
                                for k in ("sendspace", "recvspace")}
    elif sysname == "Linux":
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    info["cpu"] = line.split(":", 1)[1].strip()
                    break
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal"):
                    info["ram_gb"] = round(int(line.split()[1]) / 2**20, 1)
            info["unix_sockbuf"] = {
                k: Path(f"/proc/sys/net/core/{k}_default").read_text().strip()
                for k in ("wmem", "rmem")}
        except OSError:
            pass
    elif sysname == "Windows":
        info["cpu"] = run_text(
            ["powershell", "-NoProfile", "-Command",
             "(Get-CimInstance Win32_Processor).Name"]).strip()
        mem = run_text(["powershell", "-NoProfile", "-Command",
                        "(Get-CimInstance Win32_ComputerSystem)"
                        ".TotalPhysicalMemory"]).strip()
        info["ram_gb"] = round(int(mem) / 2**30, 1) if mem.isdigit() else None
    if not info.get("cpu"):
        info["cpu"] = platform.processor() or "?"
    return {k: v for k, v in info.items() if v not in (None, "", {})}


def probe_mads(mads_bin):
    """Version of the MADS binaries and the features this benchmark needs."""
    source = str(exe(mads_bin, "mads-source"))
    version = run_text([source, "-v"]).strip().splitlines()
    help_text = run_text([source, "--help"])
    return {"version": version[-1].split()[-1] if version else "?",
            "transport": "--transport" in help_text}


def mode_supported(mode, features):
    """None if `mode` can run on this MADS, else why not."""
    if mode != "tcp" and not features["transport"]:
        return "needs a MADS with ipc transport (--transport)"
    return None


def toml_str(s):
    # literal string: no escaping, so Windows backslashes survive
    return "'" + str(s) + "'"


def plan_run(kind, size, args):
    """Source period (ms) and message count for one run."""
    if kind == "latency":
        # Pace so that the offered load stays well below any transport's
        # bandwidth (queues stay empty), and the run lasts ~latency_time.
        period = max(args.latency_period,
                     math.ceil(size / args.latency_bw * 1000))
        count = int(args.latency_time * 1000 / period)
        count = max(args.min_count, min(args.latency_count, count))
        return period, count
    count = args.budget // size
    count = max(args.min_count, min(args.max_count, count))
    return 0, count


def write_ini(path, port, mode, size, count, warmup, results, args):
    shm = MODES[mode]["shm"]
    sockbuf = (f"sndbuf = {args.sockbuf}\nrcvbuf = {args.sockbuf}\n"
               if args.sockbuf else "")
    if args.ipc_sockbuf is not None:
        sockbuf += (f"ipc_sndbuf = {args.ipc_sockbuf}\n"
                    f"ipc_rcvbuf = {args.ipc_sockbuf}\n")
    slot = math.ceil((size + 4096) / 4096) * 4096
    path.write_text(f"""# generated by run_bench.py
[agents]
frontend_address = "tcp://localhost:{port}"
backend_address = "tcp://localhost:{port + 1}"
{sockbuf}
[broker]
frontend_address = "tcp://*:{port}"
backend_address = "tcp://*:{port + 1}"
settings_address = "tcp://*:{port + 2}"
ipc = true

[bench_source]
pub_topic = "bench"
size = {size}
count = {count}
zero_copy = {str(not args.no_zero_copy).lower()}
shm = {str(shm).lower()}
shm_slots = {args.shm_slots}
shm_slot_size = {slot}

[bench_filter]
sub_topic = ["bench"]
pub_topic = "bench_out"
results = {toml_str(results)}
warmup = {warmup}
touch = {str(args.touch).lower()}
""")


def wait_port(port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def wait_text(path, text, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if text in path.read_text(errors="replace"):
                return True
        except OSError:
            pass
        time.sleep(0.1)
    return False


def stop(proc, timeout=5):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def tail(path, lines=15):
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
    except OSError:
        return "(no log)"


def run_one(kind, size, mode, rep, args, env, run_dir):
    period, count = plan_run(kind, size, args)
    warmup = min(args.warmup, count // 10)
    run_dir.mkdir(parents=True, exist_ok=True)
    ini = run_dir / "mads.ini"
    results = run_dir / "results.json"
    write_ini(ini, args.port, mode, size, count, warmup, results, args)

    settings = f"tcp://127.0.0.1:{args.port + 2}"
    transport = MODES[mode]["transport"]
    transport_arg = (["--transport", transport] if args.features["transport"]
                     else [])
    logs = {n: open(run_dir / f"{n}.log", "w") for n in
            ("broker", "filter", "source")}
    procs = {}
    record = {"kind": kind, "size": size, "mode": mode, "repeat": rep,
              "period_ms": period, "count": count}
    try:
        procs["broker"] = subprocess.Popen(
            [str(exe(args.mads_bin, "mads-broker")), "-s", str(ini), "-d"],
            cwd=run_dir, env=env, stdin=subprocess.DEVNULL,
            stdout=logs["broker"], stderr=subprocess.STDOUT)
        if not wait_port(args.port + 2, 10):
            raise RuntimeError("broker did not start:\n" +
                               tail(run_dir / "broker.log"))
        if MODES[mode]["transport"] == "ipc":
            time.sleep(0.2)  # ipc socket files are bound right after tcp

        procs["filter"] = subprocess.Popen(
            [str(exe(args.mads_bin, "mads-filter")), str(args.filter_plugin),
             "-n", "bench_filter", "-s", settings, *transport_arg,
             "--silent"],
            cwd=run_dir, env=env, stdin=subprocess.DEVNULL,
            stdout=logs["filter"], stderr=subprocess.STDOUT)
        if not wait_text(run_dir / "filter.log", "plugin started", 15):
            raise RuntimeError("filter did not start:\n" +
                               tail(run_dir / "filter.log"))
        time.sleep(args.settle)  # let the subscription reach the broker

        procs["source"] = subprocess.Popen(
            [str(exe(args.mads_bin, "mads-source")), str(args.source_plugin),
             "-n", "bench_source", "-s", settings, *transport_arg,
             "-p", str(period), "--silent"],
            cwd=run_dir, env=env, stdin=subprocess.DEVNULL,
            stdout=logs["source"], stderr=subprocess.STDOUT)

        expected = count * period / 1000 + 10 + count * size / 100e6
        deadline = time.monotonic() + max(args.timeout, expected)
        while not results.exists():
            for name, p in procs.items():
                if p.poll() is not None:
                    raise RuntimeError(f"{name} exited with code {p.returncode}:"
                                       f"\n" + tail(run_dir / f"{name}.log"))
            if time.monotonic() > deadline:
                raise RuntimeError("timed out waiting for results (end marker "
                                   "lost?)\n" + tail(run_dir / "filter.log"))
            time.sleep(0.1)
        time.sleep(0.1)  # let the filter finish writing
        record.update(json.loads(results.read_text()))
        # The agents print which transport/ring they actually used: check it
        # so a silent fallback does not masquerade as an ipc/shm result.
        src_log = (run_dir / "source.log").read_text(errors="replace")
        m = re.search(r"Transport:(?:\s|\x1b\[[0-9;]*m)*(\w+)", src_log)
        # a MADS without --transport prints no transport line: it is tcp
        record["actual_transport"] = (m.group(1) if m else
                                      "?" if args.features["transport"] else
                                      "tcp")
        record["shm_path"] = (
            "zero-copy" if "zero-copy (plugin writes into the ring)" in src_log
            else "copy" if "copied into the ring" in src_log else "none")
        record["status"] = "ok"
    except Exception as e:  # noqa: BLE001 - report and keep going
        record["status"] = "failed"
        record["error"] = str(e)
    finally:
        for name in ("source", "filter", "broker"):
            stop(procs.get(name))
        for f in logs.values():
            f.close()
    return record


def fmt(v, digits=1):
    return "-" if v is None else f"{v:,.{digits}f}"


def median_of(rows, path):
    vals = []
    for r in rows:
        v = r
        for k in path:
            v = v.get(k) if isinstance(v, dict) else None
        if isinstance(v, (int, float)):
            vals.append(v)
    return statistics.median(vals) if vals else None


def summarize(records, modes, out_dir, meta):
    ok = [r for r in records if r.get("status") == "ok"]
    m = meta["machine"]
    lines = [f"# {meta.get('label') or m.get('host', '?')}\n",
             f"- MADS {meta['mads']['version']} ({meta['mads_bin']}), "
             f"plugin protocol P{meta.get('plugin_protocol', '?')}",
             f"- {m.get('cpu', '?')}, {m.get('cpus', '?')} CPUs, "
             f"{m.get('ram_gb', '?')} GB RAM",
             f"- {m.get('os', '?')}"]
    for note in meta.get("skipped_modes", []):
        lines.append(f"- skipped: {note}")
    for kind in ("latency", "throughput"):
        rows = [r for r in ok if r["kind"] == kind]
        if not rows:
            continue
        sizes = sorted({r["size"] for r in rows})
        lines.append(f"\n## {kind} (median over repeats)\n")
        unit = "p50 / p99 µs" if kind == "latency" else "MB/s (msg/s)"
        ratio_modes = [m for m in modes if m != "tcp"] if "tcp" in modes else []
        cols = (["size"] + [f"{m} {unit}" for m in modes] +
                [f"{m} vs tcp" for m in ratio_modes])
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "---|" * len(cols))
        for size in sizes:
            cells, metric = [], {}
            for m in modes:
                sel = [r for r in rows if r["size"] == size and r["mode"] == m]
                if kind == "latency":
                    p50 = median_of(sel, ("latency_us", "p50"))
                    p99 = median_of(sel, ("latency_us", "p99"))
                    metric[m] = p50
                    cells.append(f"{fmt(p50)} / {fmt(p99)}")
                else:
                    # A rate over a handful of survivors (e.g. a small-payload
                    # shm run, where the lossy ring drops nearly everything)
                    # says nothing: require 1% of the payloads, at least 20.
                    sel_ok = [r for r in sel if r.get("measured", 0) >=
                              max(20, 0.01 * r.get("sent", 0))]
                    mb = median_of(sel_ok, ("mb_per_s",))
                    mps = median_of(sel_ok, ("msg_per_s",))
                    lost = median_of(sel, ("lost",))
                    metric[m] = mb
                    lost_txt = f", lost {lost:.0f}" if lost else ""
                    cells.append(f"{fmt(mb)} ({fmt(mps, 0)}{lost_txt})")
            ratios = []
            for m in ratio_modes:
                a, b = metric.get(m), metric.get("tcp")
                if not a or not b:
                    ratios.append("-")
                elif kind == "latency":
                    ratios.append(f"{b / a:.2f}× faster" if a < b
                                  else f"{a / b:.2f}× slower")
                else:
                    ratios.append(f"{a / b:.2f}×")
            lines.append("| " + " | ".join([human_size(size)] + cells +
                                           ratios) + " |")
    failed = [r for r in records if r.get("status") != "ok"]
    if failed:
        lines.append("\n## failed runs\n")
        for r in failed:
            first = r.get("error", "").splitlines()[0] if r.get("error") else ""
            lines.append(f"- {r['kind']} {human_size(r['size'])} {r['mode']} "
                         f"#{r['repeat']}: {first}")
    text = "\n".join(lines)
    (out_dir / "summary.md").write_text(text + "\n")
    return text


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mads-bin", type=Path, default=None,
                    help="directory with mads-broker/-source/-filter (default: "
                         "the MADS the plugins were built against, else "
                         "`mads -p`/bin)")
    ap.add_argument("--mads-lib", type=Path, default=None,
                    help="directory with MadsCore.dll, prepended to PATH on "
                         "Windows (default: from the plugin build)")
    ap.add_argument("--label", default="",
                    help="name for this machine/config in summaries and "
                         "compare.py (default: host name)")
    ap.add_argument("--plugin-dir", default=HERE / "build", type=Path,
                    help="directory with bench_source/bench_filter .plugin")
    ap.add_argument("--modes", default="tcp,ipc,ipc+shm",
                    help=f"comma list of {', '.join(MODES)}")
    ap.add_argument("--sizes", default="64,1K,16K,256K,1M,4M,16M",
                    help="comma list of payload sizes (K/M/G suffixes)")
    ap.add_argument("--kind", default="both",
                    choices=["latency", "throughput", "both"])
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--port", type=int, default=19490,
                    help="base port (uses port..port+2)")
    ap.add_argument("--out", type=Path, default=HERE / "results",
                    help="results root (a timestamped subdir is created)")
    ap.add_argument("--latency-period", type=int, default=2,
                    help="minimum source period for latency runs (ms)")
    ap.add_argument("--latency-bw", type=float, default=100e6,
                    help="max offered load for latency runs (bytes/s)")
    ap.add_argument("--latency-time", type=float, default=4.0,
                    help="target duration of a latency run (s)")
    ap.add_argument("--latency-count", type=int, default=2000,
                    help="max messages per latency run")
    ap.add_argument("--budget", type=lambda s: parse_size(s), default="1G",
                    help="payload bytes per throughput run (bounds memory)")
    ap.add_argument("--max-count", type=int, default=200000,
                    help="max messages per throughput run")
    ap.add_argument("--min-count", type=int, default=100)
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--shm-slots", type=int, default=16)
    ap.add_argument("--touch", action="store_true",
                    help="filter reads every payload byte (checksum)")
    ap.add_argument("--no-zero-copy", action="store_true",
                    help="shm modes: source publishes through the copying "
                         "path instead of writing into the ring slot")
    ap.add_argument("--sockbuf", type=lambda s: parse_size(s), default=0,
                    help="SO_SNDBUF/SO_RCVBUF for agents and broker (bytes, "
                         "K/M suffixes; 0 = OS default)")
    ap.add_argument("--ipc-sockbuf", type=lambda s: parse_size(s), default=None,
                    help="ipc_sndbuf/ipc_rcvbuf: kernel buffers of ipc "
                         "connections only (default: MADS's own, 1M; 0 = OS "
                         "default, i.e. MADS before the ipc buffer fix)")
    ap.add_argument("--settle", type=float, default=2.0,
                    help="wait between filter start and source start (s)")
    ap.add_argument("--timeout", type=float, default=60.0,
                    help="minimum per-run timeout (s)")
    args = ap.parse_args()

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for m in modes:
        if m not in MODES:
            ap.error(f"unknown mode {m!r}")
    sizes = [max(parse_size(s), HEADER_BYTES) for s in args.sizes.split(",")]
    kinds = ["latency", "throughput"] if args.kind == "both" else [args.kind]
    if isinstance(args.budget, str):
        args.budget = parse_size(args.budget)
    args.source_plugin = find_plugin(args.plugin_dir, "bench_source")
    args.filter_plugin = find_plugin(args.plugin_dir, "bench_filter")
    build_info = {}
    info_file = args.source_plugin.parent / "bench_build_info.json"
    if info_file.exists():
        build_info = json.loads(info_file.read_text())
    if args.mads_bin is None:
        if build_info.get("mads_bin"):
            args.mads_bin = Path(build_info["mads_bin"])
        else:
            prefix = run_text(["mads", "-p"]).strip()
            if not prefix:
                sys.exit("Cannot find MADS: put `mads` on the PATH or use "
                         "--mads-bin")
            args.mads_bin = Path(prefix) / "bin"
    if args.mads_lib is None and build_info.get("mads_lib"):
        args.mads_lib = Path(build_info["mads_lib"])
    for tool in ("mads-broker", "mads-source", "mads-filter"):
        if not exe(args.mads_bin, tool).exists():
            sys.exit(f"{exe(args.mads_bin, tool)} not found (use --mads-bin)")
    args.features = probe_mads(args.mads_bin)
    skipped = []
    for m in list(modes):
        why = mode_supported(m, args.features)
        if why:
            modes.remove(m)
            skipped.append(f"{m}: {why}")
            print(f"Skipping mode {m}: {why}")
    if not modes:
        sys.exit("No mode left to run")

    env = os.environ.copy()
    if IS_WINDOWS:
        # A system-wide MADS install would otherwise shadow the fresh
        # MadsCore.dll (STATUS_ENTRYPOINT_NOT_FOUND, 0xC0000139).
        lib = args.mads_lib or args.mads_bin
        env["PATH"] = os.pathsep.join([str(lib), str(args.mads_bin),
                                       env.get("PATH", "")])

    out_dir = args.out / datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = {"label": args.label, "started": datetime.now().isoformat(),
            "machine": machine_info(), "mads": args.features,
            "mads_bin": str(args.mads_bin),
            "plugin_protocol": build_info.get("plugin_protocol"),
            "build": build_info, "modes": modes, "skipped_modes": skipped,
            "args": {k: str(v) for k, v in vars(args).items()
                     if k != "features"}}
    print(f"MADS {args.features['version']} in {args.mads_bin}; "
          f"{meta['machine'].get('cpu', '?')}")
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    total = len(kinds) * len(sizes) * len(modes) * args.repeat
    records, n = [], 0
    for rep in range(1, args.repeat + 1):
        for kind in kinds:
            for size in sizes:
                for mode in modes:  # interleave modes: fairer under drift
                    n += 1
                    label = f"{kind} {human_size(size)} {mode} #{rep}"
                    print(f"[{n}/{total}] {label} ...", end=" ", flush=True)
                    run_dir = (out_dir / "runs" /
                               f"{kind}-{human_size(size)}-{mode}-{rep}")
                    r = run_one(kind, size, mode, rep, args, env, run_dir)
                    records.append(r)
                    if r["status"] != "ok":
                        print("FAILED")
                        print("   " + r["error"].replace("\n", "\n   "))
                        continue
                    if r.get("actual_transport") != MODES[mode]["transport"]:
                        print(f"(warning: ran over "
                              f"{r.get('actual_transport')}) ", end="")
                    if MODES[mode]["shm"] and r.get("shm_path") == "none":
                        print("(warning: shm not active) ", end="")
                    lat = r.get("latency_us", {})
                    print(f"p50 {fmt(lat.get('p50'))} µs, "
                          f"{fmt(r.get('mb_per_s'))} MB/s, "
                          f"lost {r.get('lost', 0)}, torn {r.get('torn', 0)}")

    (out_dir / "results.json").write_text(json.dumps(records, indent=2))
    fields = ["kind", "size", "mode", "repeat", "status", "actual_transport",
              "shm_path",
              "period_ms", "count", "sent", "received", "measured", "lost",
              "torn", "bad", "zero_copy_views", "msg_per_s", "mb_per_s",
              "duration_s", "lat_min_us", "lat_p50_us", "lat_p90_us",
              "lat_p99_us", "lat_max_us", "lat_mean_us", "lat_stddev_us",
              "error"]
    with open(out_dir / "results.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in records:
            row = dict(r)
            for k, v in r.get("latency_us", {}).items():
                row[f"lat_{k}_us"] = v
            w.writerow(row)
    print(summarize(records, modes, out_dir, meta))
    print(f"\nResults in {out_dir}")


if __name__ == "__main__":
    main()
