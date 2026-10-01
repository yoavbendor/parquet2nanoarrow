#!/usr/bin/env python3
"""Benchmark parquet2nanoarrow against pyarrow.parquet.read_table on the same files.

Both read every row group of every column into Arrow arrays. The file is read once before timing,
so both start from a warm page cache. Two views:

  * warm (best of N in one process): pyarrow's mimalloc pool keeps freed memory mapped, so its
    repeat reads skip the page faults of fresh output buffers. parquet2nanoarrow allocates with
    glibc malloc, which returns large blocks to the OS; for a like-for-like comparison the bench
    runs it with glibc tuned to keep freed memory (GLIBC_TUNABLES, the same effect as a pool).
  * first read (a fresh process each): both pay for every page of fresh output memory.

pyarrow is single-threaded (use_threads=False) in the like-for-like columns; its default thread
pool is shown for reference. parquet2nanoarrow is single-threaded.

usage: compare_pyarrow.py /path/to/p2n_bench [rows=5000000] [reps=5]
"""
import json
import os
import subprocess
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq


def datasets(n):
    import numpy as np
    rng = np.random.default_rng(1)
    ints = pa.table({f"i{k}": pa.array(rng.integers(-2**40, 2**40, n)) for k in range(4)})
    floats = pa.table({f"f{k}": pa.array(rng.standard_normal(n)) for k in range(4)})
    nullable = pa.table({
        "a": pa.array(rng.integers(0, 1000, n), mask=rng.random(n) < 0.1),
        "b": pa.array(rng.standard_normal(n), mask=rng.random(n) < 0.3),
    })
    words = np.array([f"word-{i:05d}" for i in range(5000)])
    strings = pa.table({"s": pa.array(words[rng.integers(0, 5000, n)]),
                        "u": pa.array([f"user_{i}_{i * 7919 % 100003}" for i in range(n // 4)] * 4)})
    mixed = pa.table({"id": pa.array(np.arange(n)), "x": pa.array(rng.standard_normal(n)),
                      "cat": pa.array(words[rng.integers(0, 50, n)]), "flag": pa.array(rng.random(n) < 0.5)})
    return [
        ("int64 x4, plain, uncompressed", ints, dict(compression="none", use_dictionary=False)),
        ("int64 x4, snappy", ints, dict(compression="snappy")),
        ("float64 x4, zstd", floats, dict(compression="zstd", use_dictionary=False)),
        ("nullable int+double, snappy", nullable, dict(compression="snappy")),
        ("strings, dictionary, snappy", strings, dict(compression="snappy")),
        ("strings, plain, zstd", strings, dict(compression="zstd", use_dictionary=False)),
        ("mixed, lz4_raw, page v2", mixed, dict(compression="lz4", data_page_version="2.0")),
    ]


PYARROW_FIRST = """
import sys, time
import pyarrow.parquet as pq
t0 = time.perf_counter()
pq.read_table(sys.argv[1], use_threads=False)
print((time.perf_counter() - t0) * 1e3)
"""


def best(fn, reps):
    t = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        t.append(time.perf_counter() - t0)
    return min(t) * 1e3


def main():
    tool = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 5_000_000
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    print(f"{n:,} rows per file, best of {reps}\n")
    print("| file | MB | p2n warm ms | pyarrow warm ms | speedup | p2n first ms | pyarrow first ms | speedup | pyarrow threaded ms |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    with tempfile.TemporaryDirectory() as d:
        for name, table, cfg in datasets(n):
            path = os.path.join(d, "f.parquet")
            pq.write_table(table, path, **cfg)
            pq.read_table(path)  # warm the page cache
            warm_env = dict(os.environ, GLIBC_TUNABLES="glibc.malloc.mmap_threshold=4294967295:"
                                                       "glibc.malloc.trim_threshold=4294967295")
            ours = json.loads(subprocess.run([tool, path, str(reps)], capture_output=True, text=True,
                                             check=True, env=warm_env).stdout)
            assert ours["rows"] == n, ours
            single = best(lambda: pq.read_table(path, use_threads=False), reps)
            threaded = best(lambda: pq.read_table(path), reps)
            ours_first = min(json.loads(subprocess.run([tool, path, "1"], capture_output=True, text=True,
                                                       check=True).stdout)["ms"] for _ in range(3))
            pa_first = min(float(subprocess.run(
                [sys.executable, "-c", PYARROW_FIRST, path], capture_output=True, text=True, check=True).stdout)
                for _ in range(3))
            print(f"| {name} | {os.path.getsize(path) / 1e6:.0f} | {ours['ms']:.1f} | {single:.1f} | "
                  f"{single / ours['ms']:.2f}x | {ours_first:.1f} | {pa_first:.1f} | {pa_first / ours_first:.2f}x | "
                  f"{threaded:.1f} |")


if __name__ == "__main__":
    main()
