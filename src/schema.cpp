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
  // A logical annotation that does not fit its physical type is rejected (as Arrow does), not
  // silently ignored: reading such a column as its bare physical type would misrepresent it.
  if (lt) {
    const auto& L = *lt;
    const Type t = l.physical;
    const auto bad = [&](const char* what) {
      l.unsupported = std::string(what) + " annotation on physical type " + std::to_string(int(t)) + " is invalid";
    };
    if ((L.STRING->has_value() || L.ENUM->has_value() || L.JSON->has_value() || L.BSON->has_value()) &&
        t != Type::BYTE_ARRAY) return bad("STRING/ENUM/JSON/BSON");
    if (L.UUID->has_value() && !(t == Type::FIXED_LEN_BYTE_ARRAY && l.type_length == 16)) return bad("UUID");
    if (L.DATE->has_value() && t != Type::INT32) return bad("DATE");
    if (L.TIMESTAMP->has_value() && t != Type::INT64) return bad("TIMESTAMP");
    if (L.TIME->has_value() && t != Type::INT32 && t != Type::INT64) return bad("TIME");
    if (L.INTEGER->has_value() && t != Type::INT32 && t != Type::INT64) return bad("INTEGER");
    if (L.FLOAT16->has_value() && !(t == Type::FIXED_LEN_BYTE_ARRAY && l.type_length == 2)) return bad("FLOAT16");
    if (L.DECIMAL->has_value() && t != Type::INT32 && t != Type::INT64 && t != Type::FIXED_LEN_BYTE_ARRAY &&
        t != Type::BYTE_ARRAY) return bad("DECIMAL");
    if (L.UNKNOWN->has_value()) {  // the null type: every value is null
      l.kind = conv::null;
      l.format = "n";
      return;
    }
  }
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
      if ((lt && (*lt).JSON->has_value()) || ct == ConvertedType::JSON) l.extension = "arrow.json";
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
      if (lt && (*lt).UUID->has_value()) l.extension = "arrow.uuid";
      return;
    }
  }
  l.unsupported = "unknown physical type " + std::to_string(int(l.physical));
}

/// Link the flattened pre-order element list into file::nodes, with cumulative def/rep depths, and
/// map every primitive to a leaf.
struct tree_builder {
  file& f;
  const std::vector<pq::SchemaElement>& s;
  std::size_t pos = 0;

  result<int> node(std::int16_t parent_def, std::int16_t parent_rep, int depth) {
    if (depth > 64) return fail("schema nested deeper than 64 levels");
    if (pos >= s.size()) return fail("schema: num_children runs past the element list");
    const pq::SchemaElement& e = s[pos++];
    pnode n;
    n.name = std::string(*e.name);
    n.repetition = depth == 0 ? FieldRepetitionType::REQUIRED
                              : e.repetition_type->value_or(FieldRepetitionType::OPTIONAL);
    n.def = std::int16_t(parent_def + (n.repetition != FieldRepetitionType::REQUIRED));
    n.rep = std::int16_t(parent_rep + (n.repetition == FieldRepetitionType::REPEATED));
    const auto& lt = *e.logicalType;
    const std::optional<ConvertedType> ct = *e.converted_type;
    n.is_list = (lt && (*lt).LIST->has_value()) || ct == ConvertedType::LIST;
    n.is_map = (lt && (*lt).MAP->has_value()) || ct == ConvertedType::MAP;
    n.is_map_kv = ct == ConvertedType::MAP_KEY_VALUE;
    const std::int32_t children = e.num_children->value_or(0);
    if (children < 0 || std::size_t(children) > s.size() - pos) return fail("schema: num_children out of range");
    n.group = depth == 0 || (e.num_children->has_value() && !e.type->has_value());
    const int idx = int(f.nodes.size());
    f.nodes.push_back(std::move(n));
    if (f.nodes[std::size_t(idx)].group) {
      std::vector<int> kids;
      for (std::int32_t c = 0; c < children; ++c) {
        auto k = node(f.nodes[std::size_t(idx)].def, f.nodes[std::size_t(idx)].rep, depth + 1);
        if (!k) return k;
        kids.push_back(*k);
      }
      f.nodes[std::size_t(idx)].children = std::move(kids);
      return idx;
    }
    if (!e.type->has_value()) return fail("schema: leaf element '" + f.nodes[std::size_t(idx)].name + "' has no physical type");
    leaf l;
    l.name = f.nodes[std::size_t(idx)].name;
    l.index = int(f.leaves.size());
    l.physical = **e.type;
    l.type_length = e.type_length->value_or(0);
    l.max_def = f.nodes[std::size_t(idx)].def;
    l.max_rep = f.nodes[std::size_t(idx)].rep;
    map_type(e, l);
    f.nodes[std::size_t(idx)].leaf = l.index;
    f.leaves.push_back(std::move(l));
    return idx;
  }
};

/// Parquet node -> Arrow node, following the Parquet spec's LIST / MAP compatibility rules.
struct arrow_builder {
  const file& f;

  const pnode& at(int i) const { return f.nodes[std::size_t(i)]; }

  /// A node in an ordinary position (struct field or top-level column).
  anode convert(int i, std::int16_t d_enc) const {
    const pnode& p = at(i);
    if (p.repetition == FieldRepetitionType::REPEATED) {
      // unannotated repeated field: a non-null list of non-null elements
      anode l;
      l.k = anode::kind::list;
      l.name = p.name;
      l.nullable = false;
      l.def_present = std::int16_t(p.def - 1);
      l.def_elem = p.def;
      l.rep_elem = p.rep;
      l.children.push_back(element(i, p.def));
      return l;
    }
    if (p.group && p.is_list) return list_of(i);
    if (p.group && (p.is_map || (p.children.size() == 1 && at(p.children[0]).is_map_kv))) return map_of(i);
    if (p.group) {
      anode st;
      st.k = anode::kind::structure;
      st.name = p.name;
      st.nullable = p.repetition == FieldRepetitionType::OPTIONAL;
      st.def_present = p.def;
      if (p.children.empty()) st.unsupported = "empty group '" + p.name + "'";
      for (int c : p.children) st.children.push_back(convert(c, d_enc));
      return st;
    }
    return primitive(i, p.repetition == FieldRepetitionType::OPTIONAL, d_enc);
  }

  anode primitive(int i, bool nullable, std::int16_t d_enc) const {
    const pnode& p = at(i);
    anode a;
    a.k = anode::kind::primitive;
    a.name = p.name;
    a.nullable = nullable;
    a.leaf = p.leaf;
    a.def_present = p.def;
    a.d_enc = d_enc;
    a.unsupported = f.leaves[std::size_t(p.leaf)].unsupported;
    return a;
  }

  /// The repeated node `i` itself used as a list element (non-null: it exists whenever its slot does).
  anode element(int i, std::int16_t d_enc) const {
    const pnode& p = at(i);
    if (!p.group) return primitive(i, false, d_enc);
    if (p.is_list || p.is_map) {  // a LIST / MAP-annotated repeated group is itself a (non-null) list or map
      anode l = p.is_list ? list_of(i) : map_of(i);
      l.nullable = false;
      return l;
    }
    anode st;
    st.k = anode::kind::structure;
    st.name = p.name;
    st.nullable = false;
    st.def_present = p.def;
    for (int c : p.children) st.children.push_back(convert(c, d_enc));
    if (p.children.empty()) st.unsupported = "empty group '" + p.name + "'";
    return st;
  }

  anode list_of(int o) const {
    const pnode& outer = at(o);
    anode l;
    l.k = anode::kind::list;
    l.name = outer.name;
    l.nullable = outer.repetition == FieldRepetitionType::OPTIONAL;
    if (outer.children.size() != 1 || at(outer.children[0]).repetition != FieldRepetitionType::REPEATED) {
      l.unsupported = "LIST group '" + outer.name + "' without exactly one repeated child";
      return l;
    }
    const int r = outer.children[0];
    const pnode& rep = at(r);
    l.def_present = outer.def;
    l.def_elem = rep.def;
    l.rep_elem = rep.rep;
    const bool two_level = !rep.group || rep.children.size() > 1 || rep.name == "array" ||
                           rep.name == outer.name + "_tuple";
    if (two_level) l.children.push_back(element(r, rep.def));
    else l.children.push_back(convert(rep.children[0], rep.def));
    return l;
  }

  anode map_of(int m) const {
    const pnode& outer = at(m);
    anode mp;
    mp.k = anode::kind::map;
    mp.name = outer.name;
    mp.nullable = outer.repetition == FieldRepetitionType::OPTIONAL;
    if (outer.children.size() != 1) {
      mp.unsupported = "MAP group '" + outer.name + "' without exactly one child";
      return mp;
    }
    const int kvi = outer.children[0];
    const pnode& kv = at(kvi);
    if (kv.repetition == FieldRepetitionType::REPEATED && kv.group && kv.children.size() == 1) {
      // a key-only MAP (a set): read as a list of the keys, as Arrow does
      anode l;
      l.k = anode::kind::list;
      l.name = outer.name;
      l.nullable = mp.nullable;
      l.def_present = outer.def;
      l.def_elem = kv.def;
      l.rep_elem = kv.rep;
      l.children.push_back(convert(kv.children[0], kv.def));
      return l;
    }
    if (kv.repetition != FieldRepetitionType::REPEATED || !kv.group || kv.children.size() != 2) {
      mp.unsupported = "MAP group '" + outer.name + "' whose key_value is not a repeated group of key and value";
      return mp;
    }
    if (at(kv.children[0]).repetition != FieldRepetitionType::REQUIRED) {
      mp.unsupported = "MAP group '" + outer.name + "' with a nullable key";
      return mp;
    }
    mp.def_present = outer.def;
    mp.def_elem = kv.def;
    mp.rep_elem = kv.rep;
    anode entries;
    entries.k = anode::kind::structure;
    entries.name = kv.name;
    entries.nullable = false;
    entries.def_present = kv.def;
    entries.children.push_back(convert(kv.children[0], kv.def));
    entries.children.push_back(convert(kv.children[1], kv.def));
    mp.children.push_back(std::move(entries));
    return mp;
  }
};

/// Bubble the first unsupported reason up and fill first_leaf.
void finalize(anode& n) {
  if (n.k == anode::kind::primitive) {
    n.first_leaf = n.leaf;
    return;
  }
  for (auto& c : n.children) {
    finalize(c);
    if (n.first_leaf < 0) n.first_leaf = c.first_leaf;
    if (n.unsupported.empty() && !c.unsupported.empty()) n.unsupported = c.unsupported;
  }
  if (n.first_leaf < 0 && n.unsupported.empty()) n.unsupported = "group '" + n.name + "' without leaves";
}

}  // namespace

status build_schema(file& f) {
  auto elems = f.meta.schema->to_vector();
  if (!elems) return fail("schema: " + std::string(elems.error().expected));
  if (elems->empty()) return fail("schema: no root element");
  tree_builder tb{f, *elems};
  auto root = tb.node(0, 0, 0);
  if (!root) return nanom::unexpected<error>(root.error());
  if (tb.pos != elems->size()) return fail("schema: elements left over after the root's children");
  arrow_builder ab{f};
  for (int c : f.nodes[0].children) {
    anode a = ab.convert(c, 0);
    finalize(a);
    f.top.push_back(std::move(a));
  }
  return ok();
}

status node_arrow_schema(const file& f, const anode& n, ArrowSchema* out) {
  ArrowSchemaInit(out);
  const char* fmt = nullptr;
  switch (n.k) {
    case anode::kind::primitive: fmt = f.leaves[std::size_t(n.leaf)].format.c_str(); break;
    case anode::kind::structure: fmt = "+s"; break;
    case anode::kind::list:      fmt = "+l"; break;
    case anode::kind::map:       fmt = "+m"; break;
  }
  if (ArrowSchemaSetFormat(out, fmt) != NANOARROW_OK || ArrowSchemaSetName(out, n.name.c_str()) != NANOARROW_OK ||
      ArrowSchemaAllocateChildren(out, std::int64_t(n.children.size())) != NANOARROW_OK)
    return fail("cannot build the Arrow schema for column '" + n.name + "'");
  out->flags = n.nullable ? ARROW_FLAG_NULLABLE : 0;
  if (n.k == anode::kind::primitive && !f.leaves[std::size_t(n.leaf)].extension.empty()) {
    // canonical Arrow extension type: storage type + ARROW:extension:name / :metadata
    ArrowBuffer md;
    const bool okmd = ArrowMetadataBuilderInit(&md, nullptr) == NANOARROW_OK &&
                      ArrowMetadataBuilderAppend(&md, ArrowCharView("ARROW:extension:name"),
                                                 ArrowCharView(f.leaves[std::size_t(n.leaf)].extension.c_str())) == NANOARROW_OK &&
                      ArrowMetadataBuilderAppend(&md, ArrowCharView("ARROW:extension:metadata"), ArrowCharView("")) == NANOARROW_OK &&
                      ArrowSchemaSetMetadata(out, reinterpret_cast<const char*>(md.data)) == NANOARROW_OK;
    ArrowBufferReset(&md);
    if (!okmd) return fail("cannot attach the Arrow extension type to column '" + n.name + "'");
  }
  for (std::size_t i = 0; i < n.children.size(); ++i) {
    auto st = node_arrow_schema(f, n.children[i], out->children[i]);
    if (!st) return st;
  }
  return ok();
}

}  // namespace p2n
