#!/usr/bin/env python3
"""Write small seed Parquet files (several codecs/encodings/page versions) with pyarrow and run the
standalone mutation fuzzer over them.  usage: make_fuzz_seeds.py /path/to/p2n_fuzz [iterations]"""
import os
import subprocess
import sys
import tempfile

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pyarrow not installed: skipping")
    sys.exit(77)


def table(n=300):
    return pa.table({
        "i": pa.array([i * 7 if i % 5 else None for i in range(n)], pa.int64()),
        "s": pa.array([f"v{i % 17}" if i % 3 else None for i in range(n)], pa.string()),
        "f": pa.array([i / 3 for i in range(n)], pa.float64()),
        "b": pa.array([i % 2 == 0 for i in range(n)], pa.bool_()),
        "d": pa.array([i * 10 for i in range(n)], pa.decimal128(9, 2)),
        "l": pa.array([[j for j in range(i % 4)] if i % 6 else None for i in range(n)], pa.list_(pa.int32())),
        "st": pa.array([{"a": i, "b": [f"x{i % 3}"] * (i % 3)} if i % 5 else None for i in range(n)],
                       pa.struct([("a", pa.int64()), ("b", pa.list_(pa.string()))])),
        "m": pa.array([[(f"k{j}", j) for j in range(i % 3)] if i % 7 else None for i in range(n)],
                      pa.map_(pa.string(), pa.int64())),
    })


CFGS = [
    dict(compression="none", use_dictionary=False),
    dict(compression="snappy"),
    dict(compression="zstd", data_page_version="2.0"),
    dict(compression="lz4", data_page_size=256, row_group_size=100),
    dict(compression="gzip", use_dictionary=False, column_encoding={"i": "DELTA_BINARY_PACKED", "s": "DELTA_BYTE_ARRAY"}),
]


def main():
    iters = sys.argv[2] if len(sys.argv) > 2 else "4000"
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for i, cfg in enumerate(CFGS):
            p = os.path.join(d, f"seed{i}.parquet")
            pq.write_table(table(), p, **cfg)
            paths.append(p)
        return subprocess.run([sys.argv[1], iters, os.environ.get("P2N_FUZZ_SEED", "20261001"), *paths]).returncode


if __name__ == "__main__":
    sys.exit(main())
