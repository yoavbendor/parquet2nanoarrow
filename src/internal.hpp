// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Internal types shared by the reader's translation units.
#ifndef PARQUET2NANOARROW_INTERNAL_HPP_INCLUDED
#define PARQUET2NANOARROW_INTERNAL_HPP_INCLUDED

#include "parquet2nanoarrow/parquet2nanoarrow.hpp"

#include <nanom/codec.hpp>
#include <nanom/columnar.hpp>

#include <bit>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

static_assert(std::endian::native == std::endian::little,
              "parquet2nanoarrow decodes PLAIN pages with memcpy: little-endian hosts only");

namespace p2n {

namespace pq = nanom_formats::parquet;

inline nanom::unexpected<error> fail(std::string msg) { return nanom::unexpected<error>(error{std::move(msg)}); }
inline status ok() { return ok_t{}; }

/// Read-only view of a whole file: mmap'd (open) or caller-provided (open_buffer).
class mapped_file {
 public:
  static result<std::shared_ptr<mapped_file>> open(const std::filesystem::path& path);
  ~mapped_file();
  mapped_file(const mapped_file&) = delete;
  mapped_file& operator=(const mapped_file&) = delete;
  std::span<const std::byte> bytes() const { return {data_, size_}; }

 private:
  mapped_file() = default;
  const std::byte* data_ = nullptr;
  std::size_t size_ = 0;
};

/// How a Parquet physical value becomes its Arrow value.
enum class conv : std::uint8_t {
  boolean,           // BOOLEAN -> bitmap
  copy,              // fixed width, Arrow layout == physical layout (int32/int64/float/double/FLBA/…)
  i32_to_i8,         // INT32 annotated INT(8)  -> int8 / uint8
  i32_to_i16,        // INT32 annotated INT(16) -> int16 / uint16
  int96_to_ns,       // INT96 (legacy timestamps) -> timestamp[ns]
  i32_to_dec128,     // DECIMAL stored as INT32
  i64_to_dec128,     // DECIMAL stored as INT64
  be_to_dec128,      // DECIMAL stored as big-endian FIXED_LEN_BYTE_ARRAY / BYTE_ARRAY
  binary,            // BYTE_ARRAY -> binary / utf8 (int32 offsets)
};

/// One leaf column of the file schema, with its resolved Arrow mapping.
struct leaf {
  std::string        name;                 ///< top-level column name
  int                index = -1;           ///< leaf position == column chunk index in each row group
  pq::Type           physical = pq::Type::INT32;
  std::int32_t       type_length = 0;      ///< FIXED_LEN_BYTE_ARRAY width
  std::int16_t       max_def = 0, max_rep = 0;
  bool               top_level = false;    ///< a direct, non-repeated primitive child of the root
  std::string        unsupported;          ///< non-empty: why this version cannot decode it
  conv               kind = conv::copy;
  std::string        format;               ///< Arrow format string
  std::size_t        phys_width = 0;       ///< bytes per decoded physical value (fixed-width types)
  std::size_t        out_width = 0;        ///< bytes per Arrow value (fixed-width outputs)
};

/// A top-level schema entry: one primitive leaf, or a group (unsupported in this version).
struct top_column {
  std::string name;
  int         leaf = -1;           ///< index into file::leaves when primitive
  std::string unsupported;
  bool        nullable = true;
};

class file {
 public:
  std::shared_ptr<mapped_file>     mapping;   ///< null when the caller owns the bytes
  std::span<const std::byte>       bytes;
  pq::FileMetaData                 meta;
  std::vector<pq::RowGroup>        row_groups;
  std::vector<leaf>                leaves;
  std::vector<top_column>          top;

  static result<std::shared_ptr<file>> load(std::shared_ptr<mapped_file> mapping, std::span<const std::byte> bytes);
  nanom::input input() const { return nanom::from(bytes); }
};

/// Resolve the leaf/top-level structure and Arrow mappings from the flattened Parquet schema.
status build_schema(file& f);

/// Arrow schema for one leaf (name, format, nullability) into a child ArrowSchema.
status leaf_arrow_schema(const leaf& l, const std::string& name, bool nullable, ArrowSchema* out);

/// Decode one column chunk of `rows` rows into the (already initialized) child array `out`.
status read_column_chunk(const file& f, const leaf& l, const pq::ColumnChunk& chunk, std::int64_t rows,
                         const read_options& opt, ArrowArray* out);

}  // namespace p2n

#endif  // PARQUET2NANOARROW_INTERNAL_HPP_INCLUDED
