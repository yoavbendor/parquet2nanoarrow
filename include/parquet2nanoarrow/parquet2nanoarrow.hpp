// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// parquet2nanoarrow — read Apache Parquet files into Arrow C Data Interface arrays.
//
// The metadata (footer, page headers) is decoded by nanom's reflected Thrift model; pages are
// decoded by nanom's columnar kernels and codecs. The output is plain Arrow C Data Interface
// (ArrowSchema / ArrowArray / ArrowArrayStream) built with nanoarrow, so it can be handed to
// pyarrow, polars, DuckDB, nanoarrow or any Arrow consumer without a copy.
//
//   auto r = p2n::reader::open("data.parquet", {.columns = {"id", "name"}});
//   if (!r) { std::puts(r.error().message.c_str()); return; }
//   ArrowArrayStream stream;
//   r->to_stream(&stream);            // one struct batch per row group
//
// Safety: every offset, length and count read from the file is checked before use (the file is
// untrusted input); a malformed file yields an error, never an out-of-bounds read. Pages are
// size-capped (read_options::max_page_bytes) so a header cannot ask for an unbounded allocation.
#ifndef PARQUET2NANOARROW_HPP_INCLUDED
#define PARQUET2NANOARROW_HPP_INCLUDED

#include <nanoarrow/nanoarrow.h>
#include <nanom/formats/parquet_thrift.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace p2n {

struct error {
  std::string message;
};

template <class T>
using result = nanom::expected<T, error>;

struct ok_t {};
/// Success-or-error for operations that produce nothing (or fill an out-parameter).
using status = result<ok_t>;

struct read_options {
  /// Top-level columns to read, in output order; empty = every column.
  std::vector<std::string> columns;
  /// Skip columns this version cannot decode yet (nested groups, repeated fields) instead of
  /// failing when they are part of the default (all-columns) projection.
  bool skip_unsupported = false;
  /// Upper bound on one page's uncompressed size (decompression-bomb guard).
  std::size_t max_page_bytes = std::size_t(1) << 30;
  /// Upper bound on the rows of one row group. Output buffers are sized from the row count before
  /// decoding, and Parquet legitimately encodes millions of rows in a few RLE bytes, so this is the
  /// guard against a corrupted row count asking for an unbounded allocation (2^28 rows by default:
  /// ~4 GiB of 16-byte values per column).
  std::int64_t max_row_group_rows = std::int64_t(1) << 28;
  /// Validate UTF-8 in string (utf8-annotated BYTE_ARRAY) columns. On by default: Arrow consumers
  /// may assume valid UTF-8 for "u" arrays, so a corrupt file must not hand them invalid text.
  bool validate_utf8 = true;
};

/// One top-level output column, as resolved at open().
struct column_info {
  std::string name;
  int         leaf_index = -1;   ///< position among the file's leaf columns (column chunk index)
  std::string arrow_format;      ///< Arrow C Data Interface format string
  bool        nullable = true;
};

class file;  // internal: the mapped file + decoded footer

class reader {
 public:
  static result<reader> open(const std::filesystem::path& path, read_options options = {});
  /// Open a Parquet file already in memory. `data` must outlive the reader and every array it
  /// returns (arrays copy page data, but the reader decodes lazily from `data`).
  static result<reader> open_buffer(std::span<const std::byte> data, read_options options = {});

  std::int64_t num_rows() const;
  std::size_t  num_row_groups() const;
  const std::vector<column_info>& columns() const;
  /// The decoded footer (zero-copy views into the file; valid while the reader lives).
  const nanom_formats::parquet::FileMetaData& metadata() const;

  /// Export the output schema: a struct ("+s") of the projected columns.
  status schema(ArrowSchema* out) const;
  /// Decode one row group into a struct array of the projected columns.
  status read_row_group(std::size_t index, ArrowArray* out) const;
  /// Export every row group as a stream of struct batches. The stream keeps the reader's file
  /// alive; it may outlive this reader object.
  status to_stream(ArrowArrayStream* out) const;

 private:
  static result<reader> make(std::shared_ptr<const file> f, read_options options);
  std::shared_ptr<const file> file_;
  std::vector<column_info> columns_;
  read_options options_;
};

}  // namespace p2n

#endif  // PARQUET2NANOARROW_HPP_INCLUDED
