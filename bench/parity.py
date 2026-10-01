#!/usr/bin/env python3
"""Parity gate: is parquet2nanoarrow at least 1.02x arrow-rs on every file, averaged over 5 runs?

Writes the benchmark files once (bench/compare_pyarrow.py datasets), then repeats the
compare_arrow_rs.py measurement RUNS times and averages, per file, the speedup ratio
(arrow-rs ms / parquet2nanoarrow ms) for both views:
  warm   best of N reads in one process, both with glibc tuned to keep freed memory
  first  best of 3 fresh processes, default glibc
A file passes when its mean ratio is >= TARGET for the view. Exit 0 only if every file passes both.

usage: parity.py p2n_bench arrow_rs_bench [rows=5000000] [runs=5] [target=1.02]
"""
import importlib.util
import os
import statistics
import sys
import tempfile

import pyarrow.parquet as pq

here = os.path.dirname(os.path.abspath(__file__))


def load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(here, f"{name}.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


cp, ca = load("compare_pyarrow"), load("compare_arrow_rs")


def main():
    ours, rs = sys.argv[1], sys.argv[2]
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 5_000_000
    runs = int(sys.argv[4]) if len(sys.argv) > 4 else 5
    target = float(sys.argv[5]) if len(sys.argv) > 5 else 1.02
    with tempfile.TemporaryDirectory() as d:
        files = []
        for i, (name, table, cfg) in enumerate(cp.datasets(n)):
            path = os.path.join(d, f"f{i}.parquet")
            pq.write_table(table, path, **cfg)
            files.append((name, path, table.num_rows))
        warm = {name: [] for name, _, _ in files}
        first = {name: [] for name, _, _ in files}
        for run in range(runs):
            for name, path, rows in files:
                ca.run(ours, path, 1, ca.WARM)  # page cache
                a, b = ca.run(ours, path, 5, ca.WARM), ca.run(rs, path, 5, ca.WARM)
                assert a["rows"] == b["rows"] == rows
                af = min(ca.run(ours, path, 1, os.environ)["ms"] for _ in range(3))
                bf = min(ca.run(rs, path, 1, os.environ)["ms"] for _ in range(3))
                warm[name].append(b["ms"] / a["ms"])
                first[name].append(bf / af)
            print(f"run {run + 1}/{runs} done", file=sys.stderr, flush=True)
    ok = True
    print(f"{n:,} rows per file (nested: n/4); mean speedup over {runs} runs; target >= {target}\n")
    print("| file | warm mean | warm runs | first mean | first runs | verdict |")
    print("|---|---:|---|---:|---|---|")
    for name, _, _ in files:
        wm, fm = statistics.mean(warm[name]), statistics.mean(first[name])
        verdict = "PASS" if wm >= target and fm >= target else "FAIL"
        ok = ok and verdict == "PASS"
        print(f"| {name} | {wm:.3f} | {' '.join(f'{x:.2f}' for x in warm[name])} | {fm:.3f} | "
              f"{' '.join(f'{x:.2f}' for x in first[name])} | {verdict} |")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
