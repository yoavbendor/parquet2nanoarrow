#!/usr/bin/env python3
"""Real-world files: read every file of apache/parquet-testing (written by parquet-mr, Spark, Impala,
Arrow and others, incl. legacy list/map layouts) with parquet2nanoarrow and with pyarrow.

For each file, exactly one of:
  match        both read it; every column equals pyarrow's (types and values)
  both_reject  neither library reads it (corrupt-by-design files)
  unsupported  we decline with a "not supported" error (features this version lacks)
  MISMATCH     anything else: values or types differ, or one side fails unexpectedly

usage: parquet_testing.py /path/to/libparquet2nanoarrow_c.so /path/to/parquet-testing [--verbose]
exit 1 on any MISMATCH, 77 when pyarrow is unavailable.
"""
import importlib.util
import os
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pyarrow not installed: skipping")
    sys.exit(77)

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("differential", os.path.join(here, "differential.py"))
diff = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diff)


def main():
    lib = diff.P2N(sys.argv[1])
    root = sys.argv[2]
    verbose = "--verbose" in sys.argv
    files = []
    for dirpath, _, names in os.walk(os.path.join(root, "data")):
        files += [os.path.join(dirpath, n) for n in names if n.endswith(".parquet")]
    counts = {"match": 0, "both_reject": 0, "unsupported": 0, "MISMATCH": 0}
    for path in sorted(files):
        rel = os.path.relpath(path, root)
        try:
            expected, pa_err = pq.read_table(path), None
        except Exception as e:  # noqa: BLE001
            expected, pa_err = None, e
        try:
            got, our_err = lib.read(path, skip_unsupported=False), None
        except Exception as e:  # noqa: BLE001
            got, our_err = None, e
        if pa_err and our_err:
            verdict, why = "both_reject", f"pyarrow: {str(pa_err)[:80]} | ours: {str(our_err)[:80]}"
        elif our_err and "not supported" in str(our_err):
            verdict, why = "unsupported", str(our_err)[:160]
        elif our_err:
            verdict, why = "MISMATCH", f"we fail, pyarrow reads it: {str(our_err)[:200]}"
        elif pa_err:
            verdict, why = "MISMATCH", f"we read it, pyarrow fails: {str(pa_err)[:200]}"
        else:
            problems = []
            if got.num_rows != expected.num_rows:
                problems.append(f"{got.num_rows} rows vs {expected.num_rows}")
            for f in expected.schema:
                if f.name not in got.column_names:
                    problems.append(f"missing column {f.name}")
                    continue
                g = got[f.name]
                if g.type != f.type:
                    problems.append(f"{f.name}: type {g.type} vs {f.type}")
                    continue
                if not diff.same_column(g, expected[f.name]):
                    problems.append(f"{f.name}: values differ")
            verdict, why = ("MISMATCH", "; ".join(problems)[:300]) if problems else ("match", "")
        counts[verdict] += 1
        if verdict != "match" or verbose:
            print(f"{verdict:12} {rel}  {why}")
    print("parquet-testing:", ", ".join(f"{v} {k}" for k, v in counts.items()))
    return 1 if counts["MISMATCH"] else 0


if __name__ == "__main__":
    sys.exit(main())
