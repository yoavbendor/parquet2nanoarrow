// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Parquet schema (a flattened pre-order tree of SchemaElements) -> leaf columns with definition /
// repetition levels and their Arrow types.
#include "internal.hpp"

namespace p2n {
namespace {

using pq::ConvertedType;
using pq::FieldRepetitionType;
using pq::Type;

std::string unit_suffix(const pq::TimeUnit& u, char ms, char us, char ns) {
  if (u.MILLIS->has_value()) return std::string(1, ms);
  if (u.MICROS->has_value()) return std::string(1, us);
  if (u.NANOS->has_value()) return std::string(1, ns);
  return {};
}

/// Fill l.kind / l.format / widths from the physical type and the logical (or legacy converted)
/// annotation. Leaves l.unsupported set when there is no mapping.
void map_type(const pq::SchemaElement& e, leaf& l) {
  const auto& lt = *e.logicalType;
  const std::optional<ConvertedType> ct = *e.converted_type;
  const auto decimal = [&](std::int32_t precision, std::int32_t scale, conv k) {
    if (precision < 1 || precision > 38) {
      l.unsupported = "decimal precision " + std::to_string(precision) + " (decimal256 not supported yet)";
      return;
    }
    l.kind = k;
    l.format = "d:" + std::to_string(precision) + "," + std::to_string(scale);
    l.out_width = 16;
  };
  const bool is_decimal = (lt && (*lt).DECIMAL->has_value()) || ct == ConvertedType::DECIMAL;
  std::int32_t precision = 0, scale = 0;
  if (lt && (*lt).DECIMAL->has_value()) {
    precision = *(**(*lt).DECIMAL).precision;
    scale = *(**(*lt).DECIMAL).scale;
  } else if (ct == ConvertedType::DECIMAL) {
    precision = e.precision->value_or(0);
    scale = e.scale->value_or(0);
  }

  switch (l.physical) {
    case Type::BOOLEAN:
      l.kind = conv::boolean;
      l.format = "b";
      return;
    case Type::INT32: {
      l.phys_width = 4;
      l.out_width = 4;
      l.kind = conv::copy;
      if (is_decimal) return decimal(precision, scale, conv::i32_to_dec128);
      int bits = 32;
      bool is_signed = true;
      if (lt && (*lt).INTEGER->has_value()) {
        bits = *(**(*lt).INTEGER).bitWidth;
        is_signed = *(**(*lt).INTEGER).isSigned;
      } else if (ct) {
        switch (*ct) {
          case ConvertedType::INT_8:   bits = 8;  break;
          case ConvertedType::INT_16:  bits = 16; break;
          case ConvertedType::UINT_8:  bits = 8;  is_signed = false; break;
          case ConvertedType::UINT_16: bits = 16; is_signed = false; break;
          case ConvertedType::UINT_32: is_signed = false; break;
          default: break;
        }
      }
      if ((lt && (*lt).DATE->has_value()) || ct == ConvertedType::DATE) { l.format = "tdD"; return; }
      if ((lt && (*lt).TIME->has_value()) || ct == ConvertedType::TIME_MILLIS) { l.format = "ttm"; return; }
      if (bits == 8) {
        l.kind = conv::i32_to_i8;
        l.out_width = 1;
        l.format = is_signed ? "c" : "C";
      } else if (bits == 16) {
        l.kind = conv::i32_to_i16;
        l.out_width = 2;
        l.format = is_signed ? "s" : "S";
      } else {
        l.format = is_signed ? "i" : "I";
      }
      return;
    }
    case Type::INT64: {
      l.phys_width = 8;
      l.out_width = 8;
      l.kind = conv::copy;
      if (is_decimal) return decimal(precision, scale, conv::i64_to_dec128);
      if (lt && (*lt).TIMESTAMP->has_value()) {
        const auto& ts = **(*lt).TIMESTAMP;
        l.format = "ts" + unit_suffix(*ts.unit, 'm', 'u', 'n') + ":" + (*ts.isAdjustedToUTC ? "UTC" : "");
        return;
      }
      if (ct == ConvertedType::TIMESTAMP_MILLIS) { l.format = "tsm:UTC"; return; }
      if (ct == ConvertedType::TIMESTAMP_MICROS) { l.format = "tsu:UTC"; return; }
      if (lt && (*lt).TIME->has_value()) {
        l.format = "tt" + unit_suffix(*(**(*lt).TIME).unit, 'm', 'u', 'n');
        return;
      }
      if (ct == ConvertedType::TIME_MICROS) { l.format = "ttu"; return; }
      bool is_signed = true;
      if (lt && (*lt).INTEGER->has_value()) is_signed = *(**(*lt).INTEGER).isSigned;
      else if (ct == ConvertedType::UINT_64) is_signed = false;
      l.format = is_signed ? "l" : "L";
      return;
    }
    case Type::INT96:
      l.phys_width = 12;
      l.out_width = 8;
      l.kind = conv::int96_to_ns;
      l.format = "tsn:";
      return;
    case Type::FLOAT:
      l.phys_width = l.out_width = 4;
      l.kind = conv::copy;
      l.format = "f";
      return;
    case Type::DOUBLE:
      l.phys_width = l.out_width = 8;
      l.kind = conv::copy;
      l.format = "g";
      return;
    case Type::BYTE_ARRAY: {
      if (is_decimal) return decimal(precision, scale, conv::be_to_dec128);
      l.kind = conv::binary;
      const bool text = (lt && ((*lt).STRING->has_value() || (*lt).JSON->has_value() || (*lt).ENUM->has_value())) ||
                        ct == ConvertedType::UTF8 || ct == ConvertedType::JSON || ct == ConvertedType::ENUM;
      l.format = text ? "u" : "z";
      return;
    }
    case Type::FIXED_LEN_BYTE_ARRAY: {
      if (l.type_length <= 0) {
        l.unsupported = "FIXED_LEN_BYTE_ARRAY without a positive type_length";
        return;
      }
      l.phys_width = std::size_t(l.type_length);
      if (is_decimal) {
        if (l.type_length > 16) {
          l.unsupported = "decimal wider than 16 bytes";
          return;
        }
        return decimal(precision, scale, conv::be_to_dec128);
      }
      l.kind = conv::copy;
      l.out_width = l.phys_width;
      if (lt && (*lt).FLOAT16->has_value() && l.type_length == 2) {
        l.format = "e";
        return;
      }
      l.format = "w:" + std::to_string(l.type_length);
      return;
    }
  }
  l.unsupported = "unknown physical type " + std::to_string(int(l.physical));
}

struct walker {
  file& f;
  const std::vector<pq::SchemaElement>& s;
  std::size_t pos = 1;  // element 0 is the root

  /// Walk one subtree rooted at s[pos]. `top` is the index into f.top it belongs to.
  status walk(std::size_t top, std::int16_t def, std::int16_t rep, int depth, bool under_group) {
    if (depth > 64) return fail("schema nested deeper than 64 levels");
    if (pos >= s.size()) return fail("schema: num_children runs past the element list");
    const pq::SchemaElement& e = s[pos++];
    const auto r = e.repetition_type->value_or(FieldRepetitionType::OPTIONAL);
    if (r == FieldRepetitionType::OPTIONAL) ++def;
    if (r == FieldRepetitionType::REPEATED) { ++def; ++rep; }
    const std::int32_t children = e.num_children->value_or(0);
    if (children < 0) return fail("schema: negative num_children");
    if (e.num_children->has_value() && !e.type->has_value()) {  // group
      if (std::size_t(children) > s.size() - pos) return fail("schema: num_children runs past the element list");
      if (f.top[top].unsupported.empty())
        f.top[top].unsupported = "nested group column (struct/list/map) — not supported yet";
      for (std::int32_t c = 0; c < children; ++c) {
        auto st = walk(top, def, rep, depth + 1, true);
        if (!st) return st;
      }
      return ok();
    }
    if (!e.type->has_value()) return fail("schema: leaf element '" + std::string(*e.name) + "' has no physical type");
    leaf l;
    l.name = std::string(*e.name);
    l.index = int(f.leaves.size());
    l.physical = **e.type;
    l.type_length = e.type_length->value_or(0);
    l.max_def = def;
    l.max_rep = rep;
    l.top_level = !under_group && r != FieldRepetitionType::REPEATED;
    map_type(e, l);
    if (!under_group) {
      if (r == FieldRepetitionType::REPEATED)
        f.top[top].unsupported = "repeated primitive column — not supported yet";
      else if (!l.unsupported.empty())
        f.top[top].unsupported = l.unsupported;
      f.top[top].leaf = l.index;
      f.top[top].nullable = r != FieldRepetitionType::REQUIRED;
    }
    f.leaves.push_back(std::move(l));
    return ok();
  }
};

}  // namespace

status build_schema(file& f) {
  auto elems = f.meta.schema->to_vector();
  if (!elems) return fail("schema: " + std::string(elems.error().expected));
  const auto& s = *elems;
  if (s.empty()) return fail("schema: no root element");
  const std::int32_t n = s[0].num_children->value_or(0);
  if (n < 0 || std::size_t(n) > s.size() - 1) return fail("schema: root num_children out of range");
  walker w{f, s};
  for (std::int32_t c = 0; c < n; ++c) {
    if (w.pos >= s.size()) return fail("schema: root num_children runs past the element list");
    f.top.push_back(top_column{std::string(*s[w.pos].name), -1, {}, true});
    auto st = w.walk(f.top.size() - 1, 0, 0, 1, false);
    if (!st) return st;
  }
  if (w.pos != s.size()) return fail("schema: elements left over after the root's children");
  return ok();
}

status leaf_arrow_schema(const leaf& l, const std::string& name, bool nullable, ArrowSchema* out) {
  ArrowSchemaInit(out);
  if (ArrowSchemaSetFormat(out, l.format.c_str()) != NANOARROW_OK ||
      ArrowSchemaSetName(out, name.c_str()) != NANOARROW_OK)
    return fail("cannot build the Arrow schema for column '" + name + "'");
  out->flags = nullable ? ARROW_FLAG_NULLABLE : 0;
  return ok();
}

}  // namespace p2n
