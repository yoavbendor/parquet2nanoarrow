# parquet2nanoarrow

Read Apache Parquet files into Arrow C Data Interface arrays (`ArrowSchema` / `ArrowArray` /
`ArrowArrayStream`), built with [nanoarrow](https://github.com/apache/arrow-nanoarrow). The output
goes to pyarrow, polars, DuckDB or any Arrow consumer without a copy.

It is the read-side sibling of [nanoarrow2parquet](https://github.com/yoavbendor/nanoarrow2parquet).
All decoding comes from [nanom](https://github.com/yoavbendor/nanom):

- the footer and page headers are nanom's reflected Thrift model (`nanom/formats/parquet_thrift.hpp`);
- pages are decoded by nanom's columnar kernels (`nanom/columnar.hpp`) and its dependency-free
  Snappy and LZ4 codecs (`nanom/codec.hpp`);
- zstd and zlib cover the remaining codecs.

```cpp
#include <parquet2nanoarrow/parquet2nanoarrow.hpp>

auto r = p2n::reader::open("data.parquet", {.columns = {"id", "name"}});
if (!r) { std::puts(r.error().message.c_str()); return 1; }

ArrowArrayStream stream;          // one struct batch per row group
r->to_stream(&stream);            // hand it to any Arrow consumer

ArrowArray batch;                 // or pull row groups yourself
r->read_row_group(0, &batch);
```

A C ABI (`parquet2nanoarrow.h`, `libparquet2nanoarrow_c.so`) exposes the same stream to other
languages. From Python with no bindings package:

```python
import ctypes, pyarrow as pa
from pyarrow.cffi import ffi
lib = ctypes.CDLL("libparquet2nanoarrow_c.so")
stream = ffi.new("struct ArrowArrayStream*")
err = ctypes.create_string_buffer(1024)
lib.p2n_open_stream(b"data.parquet", None, 0, 1, ctypes.c_void_p(int(ffi.cast("uintptr_t", stream))), err, 1024)
table = pa.RecordBatchReader._import_from_c(int(ffi.cast("uintptr_t", stream))).read_all()
```

## What it reads

| | supported |
|---|---|
| physical types | BOOLEAN, INT32, INT64, INT96 (legacy timestamps → `timestamp[ns]`), FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY |
| logical types | STRING / JSON / ENUM → `utf8`; INT(8/16/32/64, signed and unsigned); DATE; TIME (ms/us/ns); TIMESTAMP (ms/us/ns, UTC-adjusted or local); DECIMAL (INT32 / INT64 / FIXED / BYTE_ARRAY → `decimal128`, precision ≤ 38); FLOAT16; legacy converted types |
| encodings | PLAIN, PLAIN_DICTIONARY / RLE_DICTIONARY, RLE (booleans), DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT |
| codecs | UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4_RAW, LZ4 (Hadoop framing and bare blocks) |
| pages | data page v1 and v2, dictionary pages, multi-page chunks, any number of row groups |
| columns | flat top-level columns (required or optional), with projection by name |

**Not yet:**
- nested columns (struct / list / map), which are skipped with `skip_unsupported` or reported as an
  error naming the column;
- `large_string` for more than 2 GiB of strings in one row group;
- encrypted files;
- reading the embedded `ARROW:schema`, so a timestamp's original non-UTC timezone and other
  Arrow-only type details are not restored;
- multi-threaded decoding.

## Safety

The file is untrusted input.

- Every offset, length, count and dictionary index read from it is checked before use. A malformed
  file returns an error and is never read out of bounds.
- `read_options::max_page_bytes` caps each page's decompressed size. `read_options::max_row_group_rows`
  caps the rows (and so the output allocation) of one row group, because Parquet can legitimately
  encode millions of rows in a few RLE bytes.
- Strings in UTF-8 columns are validated (`read_options::validate_utf8`, on by default), so consumers
  that trust Arrow's UTF-8 guarantee never receive invalid text.
- Every decoded row group also passes nanoarrow's array validation before it is returned.

## Verification

| check | what it proves |
|---|---|
| `tests/differential.py` | 60 pyarrow-written files covering every type, encoding, codec, page version and null density above. Each is read through the C ABI and imported into pyarrow via the C Data Interface. Every table must equal `pyarrow.parquet.read_table` exactly: schema, nullability, values and null positions (floats by bit pattern). It also checks projection, unsupported-column errors and empty files. |
| `fuzz/fuzz_reader.cpp` + `tests/make_fuzz_seeds.py` | the whole reader on mutated files (footer-biased bit flips, overwrites, truncation, splices) under ASan/UBSan. 200,000 mutated files ran with no crash; about 50,000 row groups still decoded and the rest were rejected with errors. |
| nanom's own suites | the Thrift model against pyarrow footers, and the kernels and codecs against reference implementations, pyarrow's compressors and fuzzing |

## Performance

From `bench/compare_pyarrow.py`: 5,000,000 rows per file, single-threaded, warm page cache, on a
shared 4-core cloud container (indicative, not a lab measurement). pyarrow is 25.0.

| file | MB | p2n warm ms | pyarrow warm ms | speedup | p2n first read ms | pyarrow first read ms | speedup |
|---|---:|---:|---:|---:|---:|---:|---:|
| int64 x4, plain, uncompressed | 160 | 55.2 | 75.8 | 1.37x | 139.7 | 159.6 | 1.14x |
| int64 x4, snappy | 160 | 192.8 | 192.2 | 1.00x | 295.0 | 322.8 | 1.09x |
| float64 x4, zstd | 153 | 217.4 | 182.8 | 0.84x | 319.8 | 334.6 | 1.05x |
| nullable int + double, snappy | 36 | 78.5 | 70.7 | 0.90x | 139.5 | 158.3 | 1.14x |
| strings, dictionary, snappy | 58 | 249.3 | 320.9 | 1.29x | 418.2 | 545.4 | 1.30x |
| strings, plain, zstd | 33 | 337.0 | 284.4 | 0.84x | 448.1 | 394.7 | 0.88x |
| mixed, lz4_raw, page v2 | 67 | 181.1 | 211.1 | 1.17x | 286.1 | 366.0 | 1.28x |

How to read the columns:

- **warm** is the best of 5 in one process. pyarrow's mimalloc pool keeps freed memory mapped, so the
  benchmark runs parquet2nanoarrow with glibc tuned to do the same.
- **first read** is a fresh process for each library, so both pay for faulting in new output memory.
- pyarrow with its thread pool is faster than both single-threaded columns.
- The plain-strings row includes UTF-8 validation, which pyarrow does not do. With
  `validate_utf8 = false` that file reads in 267 ms, ahead of pyarrow's 284 ms.

How the reader keeps copies down:

- the file is memory-mapped;
- uncompressed pages are decoded straight from the mapping;
- PLAIN pages without levels are decompressed directly into the Arrow buffer;
- definition levels become the validity bitmap run by run;
- values are spread over null slots 64 rows at a time;
- bit-unpacking loops are specialized per bit width at compile time.

## Build

```sh
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build          # needs python3 with pyarrow + cffi; skipped otherwise
python3 bench/compare_pyarrow.py build/p2n_bench
```

Dependencies are fetched by CMake: nanom (header-only, pinned commit), nanoarrow, and zstd when it
isn't installed. zlib is used when found. To develop against a local nanom checkout, pass
`-DFETCHCONTENT_SOURCE_DIR_NANOM=/path/to/nanom`. The library builds with GCC ≥ 13 and Clang ≥ 18
(C++23) and requires a little-endian host.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
