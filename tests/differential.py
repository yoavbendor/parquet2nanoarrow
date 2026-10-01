#!/usr/bin/env python3
"""Differential test: parquet2nanoarrow vs pyarrow on a matrix of real Parquet files.

pyarrow writes each file; parquet2nanoarrow reads it through its C ABI into an ArrowArrayStream,
which pyarrow imports through the Arrow C Data Interface (no copy, no conversion); the resulting
table must equal pyarrow.parquet.read_table() of the same file exactly: types, nullability,
values and null positions.

The matrix covers every physical type and the logical annotations the reader maps, every page
encoding pyarrow can write (PLAIN, dictionary, RLE booleans, DELTA_BINARY_PACKED,
DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT), every codec (none, snappy, gzip,
zstd, lz4_raw), data page v1 and v2, multi-page chunks, many row groups, and null patterns from
none to all.

usage: differential.py /path/to/libparquet2nanoarrow_c.so      (exit 77 = skipped: no pyarrow)
"""
import ctypes
import datetime
import decimal
import os
import random
import sys
import tempfile

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow.cffi import ffi
except ImportError as e:
    print(f"pyarrow/cffi not available ({e}): skipping")
    sys.exit(77)

N = 2500  # rows per file: enough for several pages at data_page_size=1024


class P2N:
    def __init__(self, lib_path):
        self.lib = ctypes.CDLL(lib_path)
        self.lib.p2n_open_stream.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.c_int,
                                             ctypes.c_int, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
        self.lib.p2n_open_stream.restype = ctypes.c_int

    def read(self, path, columns=None, skip_unsupported=True):
        stream = ffi.new("struct ArrowArrayStream*")
        addr = int(ffi.cast("uintptr_t", stream))
        err = ctypes.create_string_buffer(4096)
        cols = None
        n = 0
        if columns:
            n = len(columns)
            cols = (ctypes.c_char_p * n)(*[c.encode() for c in columns])
        rc = self.lib.p2n_open_stream(path.encode(), cols, n, int(skip_unsupported), addr, err, len(err))
        if rc != 0:
            raise RuntimeError(err.value.decode())
        return pa.RecordBatchReader._import_from_c(addr).read_all()


def null_mask(rng, n, kind):
    if kind == "none":
        return None
    if kind == "all":
        return [True] * n
    if kind == "sparse":
        return [rng.random() < 0.02 for _ in range(n)]
    return [rng.random() < 0.4 for _ in range(n)]


def make_table(rng, nulls):
    n = N
    def arr(values, typ):
        m = null_mask(rng, n, nulls)
        return pa.array(values, typ, mask=pa.array(m) if m is not None else None)
    words = [f"w{rng.randrange(40)}" + "x" * rng.randrange(8) for _ in range(n)]
    sorted_words = sorted(f"prefix/{i // 7:06d}/{rng.randrange(100)}" for i in range(n))  # DELTA_BYTE_ARRAY-friendly
    return pa.table({
        "b": arr([rng.random() < 0.5 for _ in range(n)], pa.bool_()),
        "i8": arr([rng.randrange(-128, 128) for _ in range(n)], pa.int8()),
        "u8": arr([rng.randrange(0, 256) for _ in range(n)], pa.uint8()),
        "i16": arr([rng.randrange(-32768, 32768) for _ in range(n)], pa.int16()),
        "u16": arr([rng.randrange(0, 65536) for _ in range(n)], pa.uint16()),
        "i32": arr([rng.randrange(-2**31, 2**31) for _ in range(n)], pa.int32()),
        "u32": arr([rng.randrange(0, 2**32) for _ in range(n)], pa.uint32()),
        "i64": arr([rng.randrange(-2**63, 2**63) for _ in range(n)], pa.int64()),
        "u64": arr([rng.randrange(0, 2**64) for _ in range(n)], pa.uint64()),
        "seq": arr(list(range(1000, 1000 + 3 * n, 3)), pa.int64()),
        "f32": arr([rng.uniform(-1e6, 1e6) for _ in range(n)], pa.float32()),
        "f64": arr([rng.uniform(-1e300, 1e300) if i % 9 else float("nan") for i in range(n)], pa.float64()),
        "f16": arr([rng.uniform(-100, 100) for _ in range(n)], pa.float16()),
        "s": arr(words, pa.string()),
        "s_sorted": arr(sorted_words, pa.string()),
        "bin": arr([rng.randbytes(rng.randrange(0, 20)) for _ in range(n)], pa.binary()),
        "fsb": arr([rng.randbytes(6) for _ in range(n)], pa.binary(6)),
        "d": arr([datetime.date(2000, 1, 1) + datetime.timedelta(days=rng.randrange(20000)) for _ in range(n)],
                 pa.date32()),
        "t_ms": arr([rng.randrange(0, 86400000) for _ in range(n)], pa.time32("ms")),
        "t_us": arr([rng.randrange(0, 86400000000) for _ in range(n)], pa.time64("us")),
        "t_ns": arr([rng.randrange(0, 86400000000000) for _ in range(n)], pa.time64("ns")),
        "ts_ms": arr([rng.randrange(0, 2**41) for _ in range(n)], pa.timestamp("ms")),
        "ts_us_utc": arr([rng.randrange(0, 2**51) for _ in range(n)], pa.timestamp("us", tz="UTC")),
        "ts_ns": arr([rng.randrange(-2**62, 2**62) for _ in range(n)], pa.timestamp("ns")),
        "dec9": arr([decimal.Decimal(rng.randrange(-10**9 + 1, 10**9)).scaleb(-2) for _ in range(n)],
                    pa.decimal128(9, 2)),
        "dec18": arr([decimal.Decimal(rng.randrange(-10**18 + 1, 10**18)).scaleb(-5) for _ in range(n)],
                     pa.decimal128(18, 5)),
        "dec38": arr([decimal.Decimal(rng.randrange(-10**38 + 1, 10**38)).scaleb(-10) for _ in range(n)],
                     pa.decimal128(38, 10)),
        "nested_list": pa.array([[1, 2]] * n, pa.list_(pa.int32())),  # unsupported in this version: skipped
    })


INT_COLS = ["i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "seq"]
CASES = [
    ("plain_none", dict(compression="none", use_dictionary=False)),
    ("dict_snappy", dict(compression="snappy")),
    ("dict_zstd_v2", dict(compression="zstd", data_page_version="2.0")),
    ("plain_gzip_v2", dict(compression="gzip", use_dictionary=False, data_page_version="2.0")),
    ("lz4raw_multipage", dict(compression="lz4", data_page_size=1024, row_group_size=700)),
    ("delta_ints", dict(compression="snappy", use_dictionary=False,
                        column_encoding={c: "DELTA_BINARY_PACKED" for c in INT_COLS})),
    ("delta_strings", dict(compression="none", use_dictionary=False,
                           column_encoding={"s": "DELTA_LENGTH_BYTE_ARRAY", "bin": "DELTA_LENGTH_BYTE_ARRAY",
                                            "s_sorted": "DELTA_BYTE_ARRAY", "fsb": "DELTA_BYTE_ARRAY"},
                           data_page_size=2048)),
    ("bss", dict(compression="zstd", use_dictionary=False,
                 column_encoding={"f32": "BYTE_STREAM_SPLIT", "f64": "BYTE_STREAM_SPLIT", "i32": "BYTE_STREAM_SPLIT",
                                  "i64": "BYTE_STREAM_SPLIT", "fsb": "BYTE_STREAM_SPLIT"})),
    ("rle_bool_v2", dict(compression="none", use_dictionary=False, data_page_version="2.0",
                         column_encoding={"b": "RLE"})),
    ("dec_as_int", dict(compression="snappy", store_decimal_as_integer=True)),
    ("int96", dict(compression="snappy", use_deprecated_int96_timestamps=True)),
    ("tiny_rowgroups", dict(compression="snappy", row_group_size=97, data_page_size=256, write_page_index=True)),
    # no embedded ARROW:schema: pyarrow maps the Parquet types itself, so the oracle is its type
    # mapping, not a round trip of the original Arrow schema
    ("no_arrow_schema", dict(compression="snappy", store_schema=False)),
    # format 1.0: legacy converted types only, PLAIN_DICTIONARY pages
    ("legacy_v1", dict(compression="snappy", version="1.0", store_schema=False, coerce_timestamps="us",
                       allow_truncated_timestamps=True)),
]


BITS = {pa.float16(): pa.int16(), pa.float32(): pa.int32(), pa.float64(): pa.int64()}


def same_column(a, b):
    """Exact equality; floats compare by bit pattern (Arrow's equals() treats NaN != NaN)."""
    if a.type in BITS and a.type == b.type:
        a, b = a.combine_chunks(), b.combine_chunks()
        return a.view(BITS[a.type]).equals(b.view(BITS[b.type]))
    return a.equals(b)


def main():
    lib = P2N(sys.argv[1])
    rng = random.Random(20261001)
    failures, files = [], 0
    with tempfile.TemporaryDirectory() as d:
        for name, cfg in CASES:
            for nulls in ("none", "sparse", "dense", "all"):
                table = make_table(rng, nulls)
                path = os.path.join(d, f"{name}_{nulls}.parquet")
                try:
                    pq.write_table(table, path, **cfg)
                except (pa.ArrowException, ValueError, TypeError) as e:
                    print(f"{name}/{nulls}: pyarrow cannot write {cfg} ({e}); skipped")
                    continue
                files += 1
                expected = pq.read_table(path, columns=[c for c in table.column_names if c != "nested_list"])
                try:
                    got = lib.read(path)
                except Exception as e:  # noqa: BLE001 - report every failure
                    failures.append(f"{name}/{nulls}: read failed: {e}")
                    continue
                if got.schema != expected.schema:
                    for fg, fe in zip(got.schema, expected.schema):
                        if fg != fe:
                            failures.append(f"{name}/{nulls}: field {fe.name}: got {fg} expected {fe}")
                    continue
                for col in expected.column_names:
                    if same_column(got[col], expected[col]):
                        continue
                    if True:
                        a, b = got[col].combine_chunks(), expected[col].combine_chunks()
                        first = next((i for i in range(len(b)) if not a[i].equals(b[i])), None)
                        failures.append(f"{name}/{nulls}: column {col} differs (first at row {first}: "
                                        f"{a[first] if first is not None else ''} vs {b[first] if first is not None else ''})")
                if got.num_rows != expected.num_rows:
                    failures.append(f"{name}/{nulls}: {got.num_rows} rows vs {expected.num_rows}")

        # required (non-nullable) columns: no definition levels at all, so PLAIN pages are
        # decompressed straight into the output buffers
        for name, cfg in [("req_snappy", dict(compression="snappy", use_dictionary=False)),
                          ("req_zstd_v2", dict(compression="zstd", use_dictionary=False, data_page_version="2.0")),
                          ("req_lz4", dict(compression="lz4", use_dictionary=False, data_page_size=4096)),
                          ("req_dict_gzip", dict(compression="gzip"))]:
            t = make_table(rng, "none").drop_columns(["nested_list"])
            t = t.cast(pa.schema([f.with_nullable(False) for f in t.schema]))
            path = os.path.join(d, f"{name}.parquet")
            pq.write_table(t, path, **cfg)
            files += 1
            got, expected = lib.read(path), pq.read_table(path)
            if got.schema != expected.schema:
                failures.append(f"{name}: schema {got.schema} vs {expected.schema}")
                continue
            for col in expected.column_names:
                if not same_column(got[col], expected[col]):
                    failures.append(f"{name}: column {col} differs")

        # projection: a reordered subset reads exactly those columns
        path = os.path.join(d, "proj.parquet")
        pq.write_table(make_table(rng, "sparse"), path)
        got = lib.read(path, columns=["s", "i64", "b"])
        if not got.equals(pq.read_table(path, columns=["s", "i64", "b"])):
            failures.append("projection: reordered subset differs")
        # the unsupported nested column is an error when requested or when not skipping
        for kwargs in (dict(columns=["nested_list"]), dict(skip_unsupported=False)):
            try:
                lib.read(path, **kwargs)
                failures.append(f"nested column did not fail with {kwargs}")
            except RuntimeError as e:
                if "not supported" not in str(e):
                    failures.append(f"unexpected error text: {e}")
        try:
            lib.read(path, columns=["nope"])
            failures.append("unknown column did not fail")
        except RuntimeError:
            pass
        # empty table
        epath = os.path.join(d, "empty.parquet")
        pq.write_table(make_table(rng, "none").slice(0, 0), epath)
        if lib.read(epath).num_rows != 0:
            failures.append("empty file returned rows")

    for f in failures:
        print("MISMATCH", f)
    print(f"parquet2nanoarrow differential: {files} files, {len(failures)} mismatches")
    return 1 if failures or files == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
