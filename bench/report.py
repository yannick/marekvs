#!/usr/bin/env python3
"""Render bench/results.csv into a side-by-side markdown report.

Usage: report.py [results.csv] [report.md]

Input columns (no header):
  engine,test,data_size,pipeline,clients,rps,avg_ms,p50_ms,p95_ms,p99_ms,max_ms
"""

import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results.csv")
REPORT = Path(sys.argv[2] if len(sys.argv) > 2 else "bench/report.md")

BASELINE = "keydb"  # ratios are marekvs / keydb


def fnum(x: str) -> float:
    try:
        return float(x)
    except ValueError:
        return 0.0


def main() -> None:
    rows = []
    for line in RESULTS.read_text().strip().splitlines():
        parts = line.split(",")
        if len(parts) < 11:
            continue
        rows.append(
            dict(
                engine=parts[0],
                test=parts[1],
                dsize=int(parts[2]),
                pipeline=int(parts[3]),
                clients=int(parts[4]),
                rps=fnum(parts[5]),
                avg=fnum(parts[6]),
                p50=fnum(parts[7]),
                p99=fnum(parts[9]),
            )
        )
    if not rows:
        sys.exit("no benchmark rows found — run the workloads first")

    engines = sorted({r["engine"] for r in rows})
    others = [e for e in engines if e != BASELINE]
    configs = sorted({(r["dsize"], r["pipeline"]) for r in rows})

    by_key = defaultdict(dict)  # (dsize, pipeline, test) -> engine -> row
    test_order: list[str] = []
    for r in rows:
        by_key[(r["dsize"], r["pipeline"], r["test"])][r["engine"]] = r
        if r["test"] not in test_order:
            test_order.append(r["test"])

    out = ["# marekvs vs KeyDB — benchmark report", ""]
    out.append(
        "Throughput in requests/sec (higher is better); latency in ms "
        "(lower is better). Ratio = marekvs ÷ keydb throughput."
    )
    out.append("")
    out.append(
        "> **Read the caveats in bench/README.md** — marekvs persists every "
        "write to an LSM on disk (128 ms fsync window); KeyDB runs fully "
        "in-memory with persistence off. This compares a disk-backed store "
        "against a RAM store on purpose: the interesting question is how "
        "close marekvs gets."
    )
    out.append("")

    clients = rows[0]["clients"]
    for dsize, pipeline in configs:
        out.append(f"## {dsize} B values, pipeline {pipeline}, {clients} clients")
        out.append("")
        header = "| test |"
        rule = "|---|"
        for e in engines:
            header += f" {e} rps | {e} p50 | {e} p99 |"
            rule += "---|---|---|"
        if others:
            header += " ratio |"
            rule += "---|"
        out.append(header)
        out.append(rule)
        for test in test_order:
            per_engine = by_key.get((dsize, pipeline, test))
            if not per_engine:
                continue
            line = f"| {test} |"
            for e in engines:
                r = per_engine.get(e)
                if r:
                    line += f" {r['rps']:,.0f} | {r['p50']:.2f} | {r['p99']:.2f} |"
                else:
                    line += " — | — | — |"
            if others:
                base = per_engine.get(BASELINE)
                other = per_engine.get(others[0])
                if base and other and base["rps"] > 0:
                    line += f" {other['rps'] / base['rps']:.2f}× |"
                else:
                    line += " — |"
            out.append(line)
        out.append("")

    # Geometric-mean summary per config (throughput ratio).
    if others:
        out.append("## Summary (geometric mean of throughput ratios)")
        out.append("")
        out.append("| config | marekvs ÷ keydb |")
        out.append("|---|---|")
        for dsize, pipeline in configs:
            ratios = []
            for test in test_order:
                per_engine = by_key.get((dsize, pipeline, test), {})
                base, other = per_engine.get(BASELINE), per_engine.get(others[0])
                if base and other and base["rps"] > 0 and other["rps"] > 0:
                    ratios.append(other["rps"] / base["rps"])
            if ratios:
                gm = 1.0
                for x in ratios:
                    gm *= x
                gm **= 1.0 / len(ratios)
                out.append(f"| {dsize} B, P={pipeline} | {gm:.2f}× |")
        out.append("")

    REPORT.write_text("\n".join(out) + "\n")
    print("\n".join(out))
    print(f"\nreport written to {REPORT}", file=sys.stderr)


if __name__ == "__main__":
    main()
