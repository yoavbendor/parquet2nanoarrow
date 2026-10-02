// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Typed reading: a described struct IS the projection.
//
//   struct trip { std::int64_t id; double fare; std::optional<std::string_view> note; };
//   NANOM_DESCRIBE(trip, id, fare, note);
//
//   auto r = p2n::typed_reader<trip>::open("trips.parquet");   // reads columns id, fare, note
//   if (!r) { std::puts(r.error().message.c_str()); return; }
//   for (std::size_t g = 0; g < r->num_row_groups(); ++g) {
//     auto b = r->read_row_group(g);
//     if (!b) ...;
//     for (const trip& t : *b) total += t.fare;               // rows built on the fly
//   }
//
// The column list, the per-field accessors and the accepted Arrow formats are all derived at
// compile time from the struct's describe<T> (NANOM_DESCRIBE, or C++26 reflection). At open the
// file's column types are checked once against the fields; a nullable column read into a plain
// (non-optional) field is accepted only for batches that hold no nulls. Row access has no runtime
// dispatch: each field is one load from its column's buffer.
//
// Field types: bool, std::int8_t .. std::int64_t, std::uint8_t .. std::uint64_t, float, double,
// std::string_view (utf8 / binary; views into the batch, valid while it lives), and
// std::optional of any of these for nullable columns. Temporal columns read as their storage
// integer (date32 -> int32, timestamp / date64 / time64 -> int64, time32 -> int32).
#ifndef PARQUET2NANOARROW_TYPED_HPP_INCLUDED
#define PARQUET2NANOARROW_TYPED_HPP_INCLUDED

#include "parquet2nanoarrow/parquet2nanoarrow.hpp"

#include <nanom/reflect.hpp>

#include <array>
#include <cstring>
#include <iterator>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace p2n {

namespace typed_detail {

template <class T> struct optional_of { using type = T; static constexpr bool value = false; };
template <class T> struct optional_of<std::optional<T>> { using type = T; static constexpr bool value = true; };

template <class> inline constexpr bool always_false = false;

/// Does an Arrow column of format `f` read into a field of value type V?
template <class V>
constexpr bool format_ok(std::string_view f) {
  if constexpr (std::is_same_v<V, bool>) return f == "b";
  else if constexpr (std::is_same_v<V, std::int8_t>) return f == "c";
  else if constexpr (std::is_same_v<V, std::uint8_t>) return f == "C";
  else if constexpr (std::is_same_v<V, std::int16_t>) return f == "s";
  else if constexpr (std::is_same_v<V, std::uint16_t>) return f == "S";
  else if constexpr (std::is_same_v<V, std::int32_t>) return f == "i" || f == "tdD" || f == "tts" || f == "ttm";
  else if constexpr (std::is_same_v<V, std::uint32_t>) return f == "I";
  else if constexpr (std::is_same_v<V, std::int64_t>)
    return f == "l" || f == "tdm" || f == "ttu" || f == "ttn" || f.starts_with("ts") || f.starts_with("tD");
  else if constexpr (std::is_same_v<V, std::uint64_t>) return f == "L";
  else if constexpr (std::is_same_v<V, float>) return f == "f";
  else if constexpr (std::is_same_v<V, double>) return f == "g";
  else if constexpr (std::is_same_v<V, std::string_view>) return f == "u" || f == "z";
  else static_assert(always_false<V>, "p2n::typed_reader: unsupported field type (see typed.hpp)");
}

template <class V>
V get(const ArrowArray* a, std::int64_t i) {
  const std::int64_t j = i + a->offset;
  if constexpr (std::is_same_v<V, bool>) {
    return (static_cast<const std::uint8_t*>(a->buffers[1])[j / 8] >> (j % 8)) & 1;
  } else if constexpr (std::is_same_v<V, std::string_view>) {
    const auto* o = static_cast<const std::int32_t*>(a->buffers[1]);
    return {static_cast<const char*>(a->buffers[2]) + o[j], std::size_t(o[j + 1] - o[j])};
  } else {
    V v;
    std::memcpy(&v, static_cast<const std::byte*>(a->buffers[1]) + std::size_t(j) * sizeof(V), sizeof(V));
    return v;
  }
}

inline bool valid(const ArrowArray* a, std::int64_t i) {
  if (a->null_count == 0 || !a->buffers[0]) return true;
  const std::int64_t j = i + a->offset;
  return (static_cast<const std::uint8_t*>(a->buffers[0])[j / 8] >> (j % 8)) & 1;
}

template <class Row>
std::vector<std::string> field_names() {
  std::vector<std::string> out;
  nanom::detail::for_each_field<Row>([&](auto f) { out.emplace_back(decltype(f)::name.sv()); });
  return out;
}

}  // namespace typed_detail

template <nanom::Described Row> class typed_reader;

/// One decoded row group, viewed as rows of `Row`. Owns its Arrow array (move-only).
template <nanom::Described Row>
class batch {
 public:
  batch() = default;
  batch(batch&& o) noexcept : arr_(std::exchange(o.arr_, ArrowArray{})) {}
  batch& operator=(batch&& o) noexcept {
    if (this != &o) {
      reset();
      arr_ = std::exchange(o.arr_, ArrowArray{});
    }
    return *this;
  }
  batch(const batch&) = delete;
  batch& operator=(const batch&) = delete;
  ~batch() { reset(); }

  std::size_t size() const { return std::size_t(arr_.length); }

  /// Row i, built from one load per field. No bounds check (see at()).
  Row operator[](std::size_t i) const {
    Row r{};
    std::size_t c = 0;
    nanom::detail::for_each_field<Row>([&](auto f) {
      constexpr auto mp = decltype(f)::mem_ptr;
      using F = nanom::detail::member_t<mp>;
      using V = typename typed_detail::optional_of<F>::type;
      const ArrowArray* col = arr_.children[c++];
      if constexpr (typed_detail::optional_of<F>::value) {
        if (typed_detail::valid(col, std::int64_t(i))) r.*mp = typed_detail::get<V>(col, std::int64_t(i));
        else r.*mp = std::nullopt;
      } else {
        r.*mp = typed_detail::get<V>(col, std::int64_t(i));
      }
    });
    return r;
  }
  std::optional<Row> at(std::size_t i) const {
    if (i >= size()) return std::nullopt;
    return (*this)[i];
  }

  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Row;
    using difference_type = std::ptrdiff_t;
    iterator() = default;
    Row operator*() const { return (*b_)[i_]; }
    iterator& operator++() { ++i_; return *this; }
    iterator operator++(int) { auto t = *this; ++i_; return t; }
    bool operator==(const iterator& o) const { return i_ == o.i_; }
   private:
    friend class batch;
    iterator(const batch* b, std::size_t i) : b_(b), i_(i) {}
    const batch* b_ = nullptr;
    std::size_t i_ = 0;
  };
  iterator begin() const { return {this, 0}; }
  iterator end() const { return {this, size()}; }

  /// The underlying struct array (one child per field, in field order).
  const ArrowArray& arrow() const { return arr_; }
  /// Hand the Arrow array to the caller (this batch becomes empty).
  ArrowArray release() { return std::exchange(arr_, ArrowArray{}); }

 private:
  friend class typed_reader<Row>;
  void reset() {
    if (arr_.release) arr_.release(&arr_);
    arr_ = ArrowArray{};
  }
  ArrowArray arr_{};
};

/// A reader whose projection and row type is `Row` (see the top of this header).
template <nanom::Described Row>
class typed_reader {
 public:
  static constexpr std::size_t width = nanom::detail::field_count_v<Row>;

  /// The top-level columns read: the field names, in declaration order.
  static std::vector<std::string> column_names() { return typed_detail::field_names<Row>(); }

  /// Open and check that every field's type fits its column. `options.columns` is ignored.
  static result<typed_reader> open(const std::filesystem::path& path, read_options options = {}) {
    options.columns = column_names();
    auto r = reader::open(path, std::move(options));
    if (!r) return nanom::unexpected<error>(r.error());
    return make(std::move(*r));
  }
  static result<typed_reader> open_buffer(std::span<const std::byte> data, read_options options = {}) {
    options.columns = column_names();
    auto r = reader::open_buffer(data, std::move(options));
    if (!r) return nanom::unexpected<error>(r.error());
    return make(std::move(*r));
  }

  std::int64_t num_rows() const { return r_.num_rows(); }
  std::size_t num_row_groups() const { return r_.num_row_groups(); }
  const reader& untyped() const { return r_; }

  /// Decode one row group. Fails if a column read into a non-optional field holds nulls.
  result<batch<Row>> read_row_group(std::size_t index) const {
    batch<Row> b;
    auto st = r_.read_row_group(index, &b.arr_);
    if (!st) return nanom::unexpected<error>(st.error());
    std::size_t c = 0;
    std::optional<std::string> bad;
    nanom::detail::for_each_field<Row>([&](auto f) {
      using F = nanom::detail::member_t<decltype(f)::mem_ptr>;
      if (!typed_detail::optional_of<F>::value && b.arr_.children[c]->null_count != 0 && !bad)
        bad = "column '" + std::string(decltype(f)::name.sv()) + "' has nulls in row group " +
              std::to_string(index) + ": declare the field std::optional";
      ++c;
    });
    if (bad) return nanom::unexpected<error>(error{*bad});
    return b;
  }

  /// Call fn(const Row&) for every row of every row group, in order; stops at the first error.
  template <class Fn>
  status for_each(Fn&& fn) const {
    for (std::size_t g = 0; g < num_row_groups(); ++g) {
      auto b = read_row_group(g);
      if (!b) return nanom::unexpected<error>(b.error());
      for (std::size_t i = 0; i < b->size(); ++i) {
        const Row row = (*b)[i];
        fn(row);
      }
    }
    return ok_t{};
  }

 private:
  explicit typed_reader(reader r) : r_(std::move(r)) {}
  static result<typed_reader> make(reader r) {
    const auto& cols = r.columns();
    if (cols.size() != width) return nanom::unexpected<error>(error{"internal: typed projection width"});
    std::size_t c = 0;
    std::optional<std::string> bad;
    nanom::detail::for_each_field<Row>([&](auto f) {
      using V = typename typed_detail::optional_of<nanom::detail::member_t<decltype(f)::mem_ptr>>::type;
      if (!bad && !typed_detail::format_ok<V>(cols[c].arrow_format))
        bad = "field '" + std::string(decltype(f)::name.sv()) + "' does not fit column type '" +
              cols[c].arrow_format + "'";
      ++c;
    });
    if (bad) return nanom::unexpected<error>(error{*bad});
    return typed_reader(std::move(r));
  }
  reader r_;
};

}  // namespace p2n

#endif  // PARQUET2NANOARROW_TYPED_HPP_INCLUDED
