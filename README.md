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
| columns | flat and **nested** columns, with projection by top-level name: struct, list, map and any nesting of them, with nulls at every level; legacy list layouts (2-level lists, `array` / `_tuple` elements, unannotated `repeated` fields), key-only maps (read as lists, as Arrow does), the `UNKNOWN` logical type (Arrow null type) |
| extension types | JSON → `arrow.json`, UUID → `arrow.uuid` (canonical Arrow extension types, as pyarrow returns them) |

**Not yet:**
- `large_string` / `large_list` for more than 2 GiB of strings or 2^31 list elements in one row group;
- `decimal256` (precision above 38) and the BROTLI codec;
- encrypted files;
- reading the embedded `ARROW:schema`, so a timestamp's original non-UTC timezone and other
  Arrow-only type details are not restored;
- multi-threaded decoding (arrow-rs has none built in either; it is the next lever beyond parity).

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
| `tests/differential.py` | 81 pyarrow-written files covering every type, encoding, codec, page version and null density above, plus 21 files of nested columns (lists of structs, structs of lists, maps of lists, lists of lists, three-deep structs; nulls and empty lists at every level). Each is read through the C ABI and imported into pyarrow via the C Data Interface. Every table must equal `pyarrow.parquet.read_table` exactly: schema, nullability, values and null positions (floats by bit pattern). It also checks projection, unsupported-column errors and empty files. |
| `tests/parquet_testing.py` | every file of [apache/parquet-testing](https://github.com/apache/parquet-testing) (pinned): files from parquet-mr, Spark, Impala, Arrow and others, incl. legacy list/map layouts and writer bugs (PARQUET-816 chunk sizes, concatenated gzip members, reused Thrift field ids). Result: **76 files equal pyarrow's output exactly, 5 are rejected by both libraries, 0 mismatches**. |
| `fuzz/fuzz_reader.cpp` + `tests/make_fuzz_seeds.py` | the whole reader on mutated files (footer-biased bit flips, overwrites, truncation, splices) under ASan/UBSan. 200,000 mutated files ran with no crash; about 50,000 row groups still decoded and the rest were rejected with errors. |
| nanom's own suites | the Thrift model against pyarrow footers, and the kernels and codecs against reference implementations, pyarrow's compressors and fuzzing |

## Performance

Measured against [arrow-rs](https://github.com/apache/arrow-rs) (the Apache Arrow Rust `parquet`
crate, 60.0.0) with `bench/parity.py`:
- **Files:** 5,000,000 rows per file (nested: 1.25M), written by pyarrow.
- **Settings:** both readers single-threaded, one batch per row group, glibc malloc.
- **Warm:** best of 5 reads in one process, both with glibc tuned to keep freed memory.
- **First read:** best of 3 fresh processes.
- **Reported value:** the speedup (arrow-rs time ÷ parquet2nanoarrow time), averaged over 5 full
  runs.

The target is at least 1.02x on every file, both ways.

| file | warm, mean of 5 | first read, mean of 5 |
|---|---:|---:|
| nested: list<int64> + struct<int64,string>, snappy | 1.39x | 1.26x |
| int64 x4, plain, uncompressed | 1.43x | 2.28x |
| int64 x4, snappy | 1.05x | 1.43x |
| float64 x4, zstd | 1.04x | 1.31x |
| nullable int + double, snappy | 1.08x | 1.16x |
| strings, dictionary, snappy | 1.12x | 1.37x |
| strings, plain, zstd | 1.17x | 1.39x |
| mixed, lz4_raw, page v2 | 1.15x | 1.46x |

All of this is from one shared 4-core cloud container, so treat it as indicative. Single runs vary by
up to about ±5%. Three warm margins are thin: float64/zstd (one run at 0.99x), int64/snappy and nullable.

What it rests on:
- **Memory:** the input file is memory-mapped. Uncompressed pages are decoded straight from the
  mapping, and PLAIN pages without levels are decompressed directly into the Arrow buffer.
- **Levels:** definition levels become the validity bitmap run by run, and values are spread over
  null slots 64 rows at a time.
- **Strings:** a dictionary is UTF-8-checked once and PLAIN pages in one bulk pass. Strings are
  copied in a single pass with fixed 16-byte moves.
- **Codecs:** Snappy and LZ4 use table-driven decoding with wild-copy fast paths.
- **Allocation:** output buffers come from a bounded reuse pool (`p2n::set_buffer_pool_limit`), so
  later row groups don't fault fresh memory in again. Scratch buffers are reused per thread.

`bench/compare_pyarrow.py` runs the same files against pyarrow.

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
