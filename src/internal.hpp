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
  null,              // UNKNOWN logical type -> Arrow null type (every value must be null)
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
  std::string        extension;            ///< canonical Arrow extension name (arrow.json, arrow.uuid) or empty
  std::size_t        phys_width = 0;       ///< bytes per decoded physical value (fixed-width types)
  std::size_t        out_width = 0;        ///< bytes per Arrow value (fixed-width outputs)
};

/// One node of the Parquet schema tree (the flattened pre-order SchemaElement list, linked up).
struct pnode {
  std::string                 name;
  pq::FieldRepetitionType     repetition = pq::FieldRepetitionType::REQUIRED;
  bool                        group = false;
  bool                        is_list = false;      ///< LIST logical / converted annotation
  bool                        is_map = false;       ///< MAP logical / converted annotation
  bool                        is_map_kv = false;    ///< legacy MAP_KEY_VALUE on the repeated group
  int                         leaf = -1;            ///< index into file::leaves for primitives
  std::int16_t                def = 0, rep = 0;     ///< cumulative definition / repetition depth
  std::vector<int>            children;             ///< indices into file::nodes
};

/// The Arrow shape of one output column (or of a nested child), resolved from the Parquet tree.
///
/// Level semantics (Dremel): within the level stream of any leaf below this node, an entry is a
/// SLOT of this node when rep <= the enclosing list's repetition level and def >= the enclosing
/// list's element level (both 0 at the top level: one slot per row). A slot is non-null when
/// def >= def_present; a list/map slot holds at least one element when def >= def_elem, and each
/// following entry with rep == rep_elem (before the next slot) starts another element.
struct anode {
  enum class kind : std::uint8_t { primitive, structure, list, map };
  kind                 k = kind::primitive;
  std::string          name;
  bool                 nullable = true;
  int                  leaf = -1;            ///< primitive: index into file::leaves
  int                  first_leaf = -1;      ///< the leaf whose levels drive this node's structure
  std::int16_t         def_present = 0;
  std::int16_t         def_elem = 0, rep_elem = 0;  ///< list / map
  std::int16_t         d_enc = 0;            ///< primitive: element level of the innermost enclosing list
  std::vector<anode>   children;
  std::string          unsupported;          ///< non-empty: why this column cannot be read yet

  /// Every primitive leaf below (and including) this node, in schema order.
  void leaves(std::vector<const anode*>& out) const {
    if (k == kind::primitive) out.push_back(this);
    for (const auto& c : children) c.leaves(out);
  }
  /// A top-level primitive with no levels beyond one optional bit: the flat fast path.
  bool flat() const { return k == kind::primitive && d_enc == 0; }
};

class file {
 public:
  std::shared_ptr<mapped_file>     mapping;   ///< null when the caller owns the bytes
  std::span<const std::byte>       bytes;
  pq::FileMetaData                 meta;
  std::vector<pq::RowGroup>        row_groups;
  std::vector<leaf>                leaves;
  std::vector<pnode>               nodes;     ///< nodes[0] is the root
  std::vector<anode>               top;       ///< one per top-level column

  static result<std::shared_ptr<file>> load(std::shared_ptr<mapped_file> mapping, std::span<const std::byte> bytes);
  nanom::input input() const { return nanom::from(bytes); }
};

/// Resolve the leaf/top-level structure and Arrow mappings from the flattened Parquet schema.
status build_schema(file& f);

/// Arrow schema for an output node (recursively for nested types) into `out`.
status node_arrow_schema(const file& f, const anode& n, ArrowSchema* out);

/// Levels kept for a leaf that belongs to a nested column (structure is assembled from them).
struct leaf_levels {
  std::vector<std::uint16_t> rep, def;
};

/// Decode one column chunk into the (already initialized) array `out`.
///   flat column:  `rows` rows, one value slot per level entry; `keep` must be null.
///   nested leaf:  slots are the level entries with def >= d_enc; every entry's rep/def levels are
///                 appended to `keep` for the structural assembly.
status read_column_chunk(const file& f, const leaf& l, const pq::ColumnChunk& chunk, std::int64_t rows,
                         std::int16_t d_enc, leaf_levels* keep, const read_options& opt, ArrowArray* out);

/// Fill the struct / list / map arrays of a nested column from its leaves' levels (the leaf arrays
/// are already decoded). `rows` is the row group's row count.
status assemble_nested(const file& f, const anode& n, const std::vector<leaf_levels>& levels,
                       std::int64_t rows, ArrowArray* out);

}  // namespace p2n

#endif  // PARQUET2NANOARROW_INTERNAL_HPP_INCLUDED
