#!/usr/bin/env python3
"""parquet2nanoarrow reads s3:// objects: the same file, written locally and uploaded to S3, must read back as the
same table, and that table must equal pyarrow's own read of the local file. A missing key must fail by name.

usage: s3_read.py /path/to/libparquet2nanoarrow_c.so
environment: P2N_S3_TEST_PREFIX=s3://bucket/prefix/ (required; otherwise exit 77 = skipped), AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT_URL (an S3-compatible endpoint, e.g. moto).
"""
import ctypes
import os
import sys
import tempfile
import uuid
from urllib.parse import urlparse

try:
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq
    from pyarrow.cffi import ffi
except ImportError as e:
    print(f"pyarrow/cffi not available ({e}): skipping")
    sys.exit(77)

prefix = os.environ.get("P2N_S3_TEST_PREFIX", "")
if not prefix.startswith("s3://"):
    print("P2N_S3_TEST_PREFIX not set (s3://bucket/prefix/); skipping")
    sys.exit(77)


def read_p2n(lib, path):
    stream = ffi.new("struct ArrowArrayStream*")
    addr = int(ffi.cast("uintptr_t", stream))
    err = ctypes.create_string_buffer(4096)
    rc = lib.p2n_open_stream(path.encode(), None, 0, 1, addr, err, len(err))
    if rc != 0:
        raise RuntimeError(err.value.decode())
    return pa.RecordBatchReader._import_from_c(addr).read_all()


def main():
    lib = ctypes.CDLL(sys.argv[1])
    lib.p2n_open_stream.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.c_int, ctypes.c_int,
                                    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    lib.p2n_open_stream.restype = ctypes.c_int

    # A table with several row groups, nulls, strings, a list column and dictionary-friendly values.
    n = 60000
    table = pa.table({
        "id": pa.array(range(n), pa.int64()),
        "name": pa.array([None if i % 7 == 0 else f"name-{i % 997}" for i in range(n)]),
        "score": pa.array([i * 0.5 for i in range(n)], pa.float64()),
        "tags": pa.array([[i % 3, i % 5] if i % 4 else None for i in range(n)], pa.list_(pa.int32())),
    })
    bucket = urlparse(prefix).netloc
    key = urlparse(prefix).path.lstrip("/")  # the prefix inside the bucket, "" or ending in "/"
    obj_key = f"{key}p2n-s3-{uuid.uuid4().hex}.parquet"
    uri = f"s3://{bucket}/{obj_key}"

    endpoint = urlparse(os.environ.get("AWS_ENDPOINT_URL", ""))
    if endpoint.scheme == "http":
        os.environ.setdefault("AWS_ALLOW_HTTP", "true")  # the reader refuses plain HTTP unless told otherwise
    fs_args = dict(access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                   region=os.environ.get("AWS_REGION", "us-east-1"))
    if endpoint.netloc:
        fs_args.update(endpoint_override=endpoint.netloc, scheme=endpoint.scheme or "https")
    fs = pafs.S3FileSystem(**fs_args)

    # A wide table: its footer is larger than the first tail read (64 KiB), so the footer is read a second time.
    wide = pa.table({f"c{i:04d}": pa.array(range(50), pa.int32()) for i in range(1500)})

    with tempfile.TemporaryDirectory() as tmp:
        for name, t, row_group_size in (("sample", table, 15000), ("wide", wide, None)):
            local = os.path.join(tmp, f"{name}.parquet")
            pq.write_table(t, local, row_group_size=row_group_size)
            want = pq.read_table(local)
            obj = f"{key}p2n-s3-{name}-{uuid.uuid4().hex}.parquet"
            with open(local, "rb") as src, fs.open_output_stream(f"{bucket}/{obj}") as dst:
                dst.write(src.read())
            if name == "wide":
                assert pq.ParquetFile(local).metadata.serialized_size > 65536, "the wide footer must exceed 64 KiB"

            from_local = read_p2n(lib, local)
            from_s3 = read_p2n(lib, f"s3://{bucket}/{obj}")
            assert from_local.equals(want), f"{name}: local read differs from pyarrow"
            assert from_s3.equals(want), f"{name}: s3 read differs from pyarrow"
            print(f"s3 read {name}: {from_s3.num_rows} rows x {from_s3.num_columns} columns in "
                  f"{pq.ParquetFile(local).num_row_groups} row groups, identical to the local read")

    try:
        read_p2n(lib, f"s3://{bucket}/{obj_key}.missing")
        raise AssertionError("a missing key was read")
    except RuntimeError as e:
        print(f"missing key refused: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
