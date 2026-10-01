// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// reader: open (mmap + footer + schema), projection, row groups -> struct arrays, and the
// ArrowArrayStream export.
#include "internal.hpp"

#include <unordered_map>

namespace p2n {

result<std::shared_ptr<file>> file::load(std::shared_ptr<mapped_file> mapping, std::span<const std::byte> bytes) {
  auto f = std::make_shared<file>();
  f->mapping = std::move(mapping);
  f->bytes = bytes;
  const nanom::input in = f->input();
  auto md = pq::read_file_metadata(in);
  if (!md) return fail("not a readable Parquet file: " + md.error().render(in));
  f->meta = std::move(md->value);
  auto rgs = f->meta.row_groups->to_vector();
  if (!rgs) return fail("bad row group metadata: " + rgs.error().render(in));
  f->row_groups = std::move(*rgs);
  auto st = build_schema(*f);
  if (!st) return nanom::unexpected<error>(st.error());
  return f;
}

namespace {

/// Resolve the projection (or the default all-columns set) into output columns.
result<std::vector<column_info>> resolve_columns(const file& f, const read_options& options) {
  std::vector<column_info> cols;
  const auto add = [&](const top_column& t) -> status {
    if (!t.unsupported.empty()) return fail("column '" + t.name + "': " + t.unsupported);
    const leaf& l = f.leaves[std::size_t(t.leaf)];
    cols.push_back(column_info{t.name, l.index, l.format, t.nullable});
    return ok();
  };
  if (options.columns.empty()) {
    for (const auto& t : f.top) {
      if (!t.unsupported.empty() && options.skip_unsupported) continue;
      auto st = add(t);
      if (!st) return nanom::unexpected<error>(st.error());
    }
    return cols;
  }
  std::unordered_map<std::string, const top_column*> by_name;
  for (const auto& t : f.top) by_name.emplace(t.name, &t);
  for (const auto& name : options.columns) {
    const auto it = by_name.find(name);
    if (it == by_name.end()) return fail("no column named '" + name + "'");
    auto st = add(*it->second);
    if (!st) return nanom::unexpected<error>(st.error());
  }
  return cols;
}

}  // namespace

result<reader> reader::make(std::shared_ptr<const file> f, read_options options) {
  auto cols = resolve_columns(*f, options);
  if (!cols) return nanom::unexpected<error>(cols.error());
  reader r;
  r.file_ = std::move(f);
  r.columns_ = std::move(*cols);
  r.options_ = std::move(options);
  return r;
}

result<reader> reader::open(const std::filesystem::path& path, read_options options) {
  auto m = mapped_file::open(path);
  if (!m) return nanom::unexpected<error>(m.error());
  const auto bytes = (*m)->bytes();
  auto f = file::load(std::move(*m), bytes);
  if (!f) return nanom::unexpected<error>(f.error());
  return make(std::move(*f), std::move(options));
}

result<reader> reader::open_buffer(std::span<const std::byte> data, read_options options) {
  auto f = file::load(nullptr, data);
  if (!f) return nanom::unexpected<error>(f.error());
  return make(std::move(*f), std::move(options));
}

std::int64_t reader::num_rows() const { return *file_->meta.num_rows; }
std::size_t reader::num_row_groups() const { return file_->row_groups.size(); }
const std::vector<column_info>& reader::columns() const { return columns_; }
const pq::FileMetaData& reader::metadata() const { return file_->meta; }

status reader::schema(ArrowSchema* out) const {
  ArrowSchemaInit(out);
  if (ArrowSchemaSetFormat(out, "+s") != NANOARROW_OK ||
      ArrowSchemaAllocateChildren(out, std::int64_t(columns_.size())) != NANOARROW_OK) {
    if (out->release) out->release(out);
    return fail("cannot build the Arrow schema");
  }
  out->flags = 0;
  for (std::size_t i = 0; i < columns_.size(); ++i) {
    const auto& c = columns_[i];
    auto st = leaf_arrow_schema(file_->leaves[std::size_t(c.leaf_index)], c.name, c.nullable, out->children[i]);
    if (!st) {
      out->release(out);
      return st;
    }
  }
  return ok();
}

status reader::read_row_group(std::size_t index, ArrowArray* out) const {
  if (index >= file_->row_groups.size()) return fail("row group index out of range");
  const pq::RowGroup& rg = file_->row_groups[index];
  const std::int64_t rows = *rg.num_rows;
  if (rows < 0) return fail("negative row count in row group " + std::to_string(index));
  if (rows > options_.max_row_group_rows)
    return fail("row group " + std::to_string(index) + " declares " + std::to_string(rows) +
                " rows, over read_options::max_row_group_rows");
  auto chunks = rg.columns->to_vector();
  if (!chunks) return fail("bad column chunk metadata in row group " + std::to_string(index));
  if (chunks->size() != file_->leaves.size())
    return fail("row group " + std::to_string(index) + " has " + std::to_string(chunks->size()) +
                " column chunks for " + std::to_string(file_->leaves.size()) + " schema leaves");

  ArrowSchema sch;
  auto st = schema(&sch);
  if (!st) return st;
  ArrowError aerr{};
  if (ArrowArrayInitFromSchema(out, &sch, &aerr) != NANOARROW_OK) {
    sch.release(&sch);
    return fail(std::string("cannot allocate the output array: ") + aerr.message);
  }
  sch.release(&sch);
  for (std::size_t i = 0; i < columns_.size(); ++i) {
    const leaf& l = file_->leaves[std::size_t(columns_[i].leaf_index)];
    st = read_column_chunk(*file_, l, (*chunks)[std::size_t(l.index)], rows, options_, out->children[i]);
    if (!st) {
      out->release(out);
      return fail("row group " + std::to_string(index) + ": " + st.error().message);
    }
  }
  out->length = rows;
  out->null_count = 0;
  if (ArrowArrayFinishBuildingDefault(out, &aerr) != NANOARROW_OK) {
    out->release(out);
    return fail(std::string("decoded row group failed Arrow validation: ") + aerr.message);
  }
  return ok();
}

// ---- ArrowArrayStream ------------------------------------------------------------------------

namespace {
struct stream_state {
  reader r;
  std::size_t next = 0;
  std::string last_error;
};

int stream_get_schema(ArrowArrayStream* s, ArrowSchema* out) {
  auto* st = static_cast<stream_state*>(s->private_data);
  auto r = st->r.schema(out);
  if (!r) { st->last_error = r.error().message; return EINVAL; }
  return 0;
}
int stream_get_next(ArrowArrayStream* s, ArrowArray* out) {
  auto* st = static_cast<stream_state*>(s->private_data);
  if (st->next >= st->r.num_row_groups()) {
    out->release = nullptr;  // end of stream
    return 0;
  }
  auto r = st->r.read_row_group(st->next++, out);
  if (!r) { st->last_error = r.error().message; return EIO; }
  return 0;
}
const char* stream_last_error(ArrowArrayStream* s) {
  auto* st = static_cast<stream_state*>(s->private_data);
  return st->last_error.empty() ? nullptr : st->last_error.c_str();
}
void stream_release(ArrowArrayStream* s) {
  delete static_cast<stream_state*>(s->private_data);
  s->private_data = nullptr;
  s->release = nullptr;
}
}  // namespace

status reader::to_stream(ArrowArrayStream* out) const {
  out->private_data = new stream_state{*this, 0, {}};
  out->get_schema = stream_get_schema;
  out->get_next = stream_get_next;
  out->get_last_error = stream_last_error;
  out->release = stream_release;
  return ok();
}

}  // namespace p2n
