#!/usr/bin/env python3
"""Benchmark parquet2nanoarrow against arrow-rs (the Apache Arrow Rust `parquet` crate).

Same files and settings for both: a full single-threaded read of every column into Arrow arrays,
one batch per row group. Both use glibc malloc, so the comparison is like for like:
  * warm   best of N in one process, both with glibc tuned to keep freed memory (no fresh page
           faults on repeat reads)
  * first  a fresh process each (best of 3 processes), default glibc, both pay for new memory

usage: compare_arrow_rs.py /path/to/p2n_bench /path/to/arrow_rs_bench [rows=5000000] [reps=5]
Datasets come from compare_pyarrow.py (pyarrow writes the files).
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile

import pyarrow.parquet as pq

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("cp", os.path.join(here, "compare_pyarrow.py"))
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)

WARM = dict(os.environ, GLIBC_TUNABLES="glibc.malloc.mmap_threshold=4294967295:glibc.malloc.trim_threshold=4294967295")


def run(tool, path, reps, env):
    out = subprocess.run([tool, path, str(reps)], capture_output=True, text=True, check=True, env=env)
    return json.loads(out.stdout)


def main():
    ours, rs = sys.argv[1], sys.argv[2]
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 5_000_000
    reps = int(sys.argv[4]) if len(sys.argv) > 4 else 5
    print(f"{n:,} rows per file (nested: n/4), best of {reps}; single-threaded\n")
    print("| file | MB | p2n warm ms | arrow-rs warm ms | speedup | p2n first ms | arrow-rs first ms | speedup |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    with tempfile.TemporaryDirectory() as d:
        for name, table, cfg in cp.datasets(n):
            path = os.path.join(d, "f.parquet")
            pq.write_table(table, path, **cfg)
            run(ours, path, 1, WARM)  # warm the page cache
            a, b = run(ours, path, reps, WARM), run(rs, path, reps, WARM)
            assert a["rows"] == b["rows"] == table.num_rows, (a, b)
            af = min(run(ours, path, 1, os.environ)["ms"] for _ in range(3))
            bf = min(run(rs, path, 1, os.environ)["ms"] for _ in range(3))
            print(f"| {name} | {os.path.getsize(path) / 1e6:.0f} | {a['ms']:.1f} | {b['ms']:.1f} | "
                  f"{b['ms'] / a['ms']:.2f}x | {af:.1f} | {bf:.1f} | {bf / af:.2f}x |", flush=True)


if __name__ == "__main__":
    main()
