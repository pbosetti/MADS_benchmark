#!/usr/bin/env python3
"""Compare MADS transport benchmark results across machines or configs.

Each argument is a results directory written by run_bench.py (it holds
meta.json and results.json). The output is one Markdown table per kind and
metric, with a row per (payload size, mode) and a column per result set,
using the median over repeats.

  python3 compare.py results/20261007-102836 other-host/20261008-091500
  python3 compare.py results/* --metric p99 -o comparison.md
"""
import argparse
import json
import statistics
import sys
from pathlib import Path


def human_size(n):
    for unit, div in (("M", 1 << 20), ("K", 1 << 10)):
        if n >= div and n % div == 0:
            return f"{n // div}{unit}"
    return str(n)


def load(path):
    path = Path(path)
    try:
        meta = json.loads((path / "meta.json").read_text())
        records = json.loads((path / "results.json").read_text())
    except (OSError, json.JSONDecodeError) as e:
        sys.exit(f"{path}: not a run_bench.py results directory ({e})")
    machine = meta.get("machine", {})
    name = meta.get("label") or machine.get("host") or path.name
    return {"name": name, "path": path, "meta": meta, "records": records}


def value(record, metric):
    if metric in ("p50", "p90", "p99", "mean"):
        return record.get("latency_us", {}).get(metric)
    if metric in ("mb_per_s", "msg_per_s"):
        # same guard as run_bench.py: a rate over a handful of survivors of
        # a lossy run is meaningless
        if record.get("measured", 0) < max(20, 0.01 * record.get("sent", 0)):
            return None
        return record.get(metric)
    return None


def median(records, metric):
    vals = [v for v in (value(r, metric) for r in records)
            if isinstance(v, (int, float))]
    return statistics.median(vals) if vals else None


MODE_ORDER = ["tcp", "ipc", "ipc+shm", "tcp+shm"]


def table(sets, kind, metric, unit, digits):
    def order(t):
        mode = t[1]
        return (t[0], MODE_ORDER.index(mode) if mode in MODE_ORDER else 99, mode)
    rows = sorted({(r["size"], r["mode"]) for s in sets for r in s["records"]
                   if r["kind"] == kind and r.get("status") == "ok"}, key=order)
    if not rows:
        return []
    head = "| size | mode | " + " | ".join(s["name"] for s in sets) + " |"
    lines = [f"\n## {kind}: {metric} ({unit}, median over repeats)\n", head,
             "|" + "---|" * (len(sets) + 2)]
    for size, mode in rows:
        cells = []
        for s in sets:
            sel = [r for r in s["records"] if r["kind"] == kind and
                   r["size"] == size and r["mode"] == mode and
                   r.get("status") == "ok"]
            v = median(sel, metric)
            cells.append("-" if v is None else f"{v:,.{digits}f}")
        lines.append(f"| {human_size(size)} | {mode} | " + " | ".join(cells) +
                     " |")
    return lines


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="+", help="run_bench.py results dirs")
    ap.add_argument("--metric", default="p50",
                    choices=["p50", "p90", "p99", "mean"],
                    help="latency statistic to compare (default p50)")
    ap.add_argument("-o", "--output", type=Path,
                    help="also write the Markdown to this file")
    args = ap.parse_args()
    sets = [load(p) for p in args.results]

    lines = ["# MADS transport benchmark comparison\n",
             "| | " + " | ".join(s["name"] for s in sets) + " |",
             "|" + "---|" * (len(sets) + 1)]
    facts = [("MADS", lambda m: m.get("mads", {}).get("version")),
             ("plugin protocol", lambda m: m.get("plugin_protocol")),
             ("CPU", lambda m: m.get("machine", {}).get("cpu")),
             ("CPUs", lambda m: m.get("machine", {}).get("cpus")),
             ("RAM GB", lambda m: m.get("machine", {}).get("ram_gb")),
             ("OS", lambda m: m.get("machine", {}).get("os")),
             ("run", lambda m: m.get("started", "")[:16])]
    for label, get in facts:
        lines.append(f"| {label} | " +
                     " | ".join(str(get(s["meta"]) or "?") for s in sets) +
                     " |")
    lines += table(sets, "latency", args.metric, "µs", 1)
    lines += table(sets, "throughput", "mb_per_s", "MB/s", 1)
    lines += table(sets, "throughput", "msg_per_s", "msg/s", 0)
    text = "\n".join(lines) + "\n"
    print(text)
    if args.output:
        args.output.write_text(text)


if __name__ == "__main__":
    main()
