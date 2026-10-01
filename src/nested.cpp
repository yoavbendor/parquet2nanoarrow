// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Nested columns: rebuild struct validity and list / map offsets from the repetition and
// definition levels of the leaves (Dremel record assembly). The leaf arrays are already decoded,
// each holding exactly the value slots inside its innermost enclosing list; this pass fills the
// structural arrays above them, top-down, one linear scan of one leaf's levels per node.
//
// For a node whose enclosing list has repetition level r_enc and element level d_enc (both 0 at
// the top level), the node's slots are the level entries with rep <= r_enc and def >= d_enc:
//   struct slot  valid when def >= def_present
//   list slot    valid when def >= def_present; non-empty when def >= def_elem, and every later
//                entry with rep == rep_elem (before the next entry with rep <= r_enc) is another
//                element
// Every child's length is checked against its parent's slot / element count, so a file whose
// leaves disagree about the structure is rejected instead of producing inconsistent arrays.
#include "internal.hpp"

namespace p2n {
namespace {

status set_validity(ArrowArray* arr, const std::vector<std::uint8_t>& valid, std::int64_t len, std::int64_t nulls) {
  ArrowBitmap* vb = ArrowArrayValidityBitmap(arr);
  if (nulls == 0) {
    ArrowBitmapReset(vb);
  } else {
    if (ArrowBitmapReserve(vb, len) != NANOARROW_OK) return fail("out of memory");
    if (len > 0) std::memcpy(vb->buffer.data, valid.data(), std::size_t((len + 7) / 8));
    vb->size_bits = len;
    vb->buffer.size_bytes = (len + 7) / 8;
  }
  arr->null_count = nulls;
  return ok();
}

status build(const file& f, const anode& n, const std::vector<leaf_levels>& levels, ArrowArray* arr,
             std::int16_t r_enc, std::int16_t d_enc, std::int64_t expected) {
  const leaf_levels& lv = levels[std::size_t(n.first_leaf)];
  const std::size_t entries = lv.def.size();
  const std::uint16_t* rep = lv.rep.data();
  const std::uint16_t* def = lv.def.data();

  if (n.k == anode::kind::primitive) {
    if (arr->length != expected)
      return fail("leaf '" + n.name + "' has " + std::to_string(arr->length) + " values where its parent has " +
                  std::to_string(expected) + " slots");
    return ok();
  }

  std::vector<std::uint8_t> valid(std::size_t((expected + 7) / 8) + 1, 0);
  std::int64_t slots = 0, nulls = 0;
  const auto mark = [&](bool v) {
    if (v) valid[std::size_t(slots / 8)] = std::uint8_t(valid[std::size_t(slots / 8)] | (1u << (slots % 8)));
    else ++nulls;
  };

  if (n.k == anode::kind::structure) {
    for (std::size_t i = 0; i < entries; ++i) {
      if (rep[i] > r_enc || def[i] < d_enc) continue;
      if (slots >= expected) return fail("struct '" + n.name + "' has more slots than its parent");
      mark(def[i] >= n.def_present);
      ++slots;
    }
    if (slots != expected) return fail("struct '" + n.name + "' has fewer slots than its parent");
    auto st = set_validity(arr, valid, slots, nulls);
    if (!st) return st;
    arr->length = slots;
    for (std::size_t c = 0; c < n.children.size(); ++c) {
      st = build(f, n.children[c], levels, arr->children[c], r_enc, d_enc, slots);
      if (!st) return st;
    }
    return ok();
  }

  // list / map: offsets + validity, then the single child over the elements
  ArrowBuffer* ob = ArrowArrayBuffer(arr, 1);
  if (ArrowBufferReserve(ob, (expected + 1) * 4) != NANOARROW_OK) return fail("out of memory");
  auto* offs = reinterpret_cast<std::int32_t*>(ob->data);
  std::int64_t elems = 0;
  bool open = false;  // the current slot holds a non-empty list
  for (std::size_t i = 0; i < entries; ++i) {
    if (rep[i] <= r_enc) {
      open = false;
      if (def[i] < d_enc) continue;  // an empty / null ancestor: not a slot of this list
      if (slots >= expected) return fail("list '" + n.name + "' has more slots than its parent");
      offs[slots] = std::int32_t(elems);
      mark(def[i] >= n.def_present);
      ++slots;
      if (def[i] >= n.def_elem) {
        open = true;
        ++elems;
      }
    } else if (rep[i] == n.rep_elem) {
      if (!open) return fail("list '" + n.name + "': element continues a list that has none");
      ++elems;
    }
    if (elems > INT32_MAX) return fail("list '" + n.name + "' has more than 2^31 elements in one row group");
  }
  if (slots != expected) return fail("list '" + n.name + "' has fewer slots than its parent");
  offs[slots] = std::int32_t(elems);
  ob->size_bytes = (slots + 1) * 4;
  auto st = set_validity(arr, valid, slots, nulls);
  if (!st) return st;
  arr->length = slots;
  return build(f, n.children[0], levels, arr->children[0], n.rep_elem, n.def_elem, elems);
}

}  // namespace

status assemble_nested(const file& f, const anode& n, const std::vector<leaf_levels>& levels,
                       std::int64_t rows, ArrowArray* out) {
  return build(f, n, levels, out, 0, 0, rows);
}

}  // namespace p2n
