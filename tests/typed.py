#!/usr/bin/env python3
"""typed_reader<Row> against pyarrow: every row of a described struct equals pyarrow's values; a
mismatching field type and nulls in a non-optional field are reported as errors.

usage: typed.py /path/to/p2n_typed_read"""
import os
import random
import subprocess
import sys
import tempfile

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as e:
    print(f"pyarrow not available ({e}): skipping")
    sys.exit(77)


def run(exe, path):
    return subprocess.run([exe, path], capture_output=True, text=True).stdout


def main():
    exe = sys.argv[1]
    rng = random.Random(7)
    n = 3000
    t = pa.table({
        "paid": pa.array([rng.random() < 0.5 for _ in range(n)]),  # read in a different order
        "id": pa.array(range(n), pa.int64()),
        "fare": pa.array([rng.uniform(0, 100) for _ in range(n)]),
        "passengers": pa.array([None if rng.random() < 0.2 else rng.randrange(1, 7) for _ in range(n)], pa.int32()),
        "note": pa.array([None if rng.random() < 0.3 else f"note {i}" for i in range(n)]),
        "unused": pa.array([1.0] * n),
    })
    failures = []
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "t.parquet")
        pq.write_table(t, path, row_group_size=700, compression="snappy")
        want = []
        for r in pq.read_table(path).to_pylist():
            want.append(f"{r['id']} {r['fare']!r} {'null' if r['passengers'] is None else r['passengers']} "
                        f"{'null' if r['note'] is None else r['note']} {int(r['paid'])}")
        got = run(exe, path).splitlines()
        # %.17g and repr may spell the same double differently: compare floats numerically
        def norm(line):
            p = line.split(" ")
            return [p[0], float(p[1])] + p[2:]
        if [norm(x) for x in got] != [norm(x) for x in want]:
            failures.append(f"rows differ (got {len(got)} lines, want {len(want)})")

        # nulls in a column read into a non-optional field
        t2 = t.set_column(1, "id", pa.array([None] + list(range(1, n)), pa.int64()))
        pq.write_table(t2, path)
        out = run(exe, path)
        if "has nulls" not in out:
            failures.append(f"nulls in a non-optional field not reported: {out[:200]}")

        # a column whose type does not fit the field
        t3 = t.set_column(2, "fare", pa.array([str(x) for x in range(n)]))
        pq.write_table(t3, path)
        out = run(exe, path)
        if "does not fit column type" not in out:
            failures.append(f"type mismatch not reported: {out[:200]}")

        # a missing column
        pq.write_table(t.drop_columns(["note"]), path)
        out = run(exe, path)
        if not out.startswith("error:"):
            failures.append(f"missing column not reported: {out[:200]}")
    for f in failures:
        print("FAIL", f)
    print(f"typed_reader: {'OK' if not failures else 'FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
