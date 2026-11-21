// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// reader: open (mmap + footer + schema), projection, row groups -> struct arrays, and the
// ArrowArrayStream export.
#include "internal.hpp"

#include <algorithm>
#include <atomic>
#include <limits>
#include <optional>
#include <system_error>
#include <thread>
#include <unordered_map>

namespace p2n {

result<std::shared_ptr<file>> file::load(std::shared_ptr<mapped_file> mapping, std::span<const std::byte> bytes) {
  auto f = std::make_shared<file>();
  f->mapping = std::move(mapping);
  f->bytes = bytes;
  f->size = bytes.size();
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
/// Also records, per output column, the index of its top-level node in file::top.
result<std::vector<column_info>> resolve_columns(const file& f, const read_options& options,
                                                 std::vector<std::size_t>& top_index) {
  std::vector<column_info> cols;
  const auto add = [&](const anode& t) -> status {
    top_index.push_back(std::size_t(&t - f.top.data()));
    if (!t.unsupported.empty()) return fail("column '" + t.name + "' is not supported: " + t.unsupported);
    std::string fmt = t.k == anode::kind::primitive ? f.leaves[std::size_t(t.leaf)].format
                    : t.k == anode::kind::structure ? "+s" : t.k == anode::kind::list ? "+l" : "+m";
    cols.push_back(column_info{t.name, t.first_leaf, std::move(fmt), t.nullable});
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
  std::unordered_map<std::string, const anode*> by_name;
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
  reader r;
  auto cols = resolve_columns(*f, options, r.top_index_);
  if (!cols) return nanom::unexpected<error>(cols.error());
  r.file_ = std::move(f);
  r.columns_ = std::move(*cols);
  r.options_ = std::move(options);
  return r;
}

namespace {

constexpr std::byte kParquetMagic[4] = {std::byte{'P'}, std::byte{'A'}, std::byte{'R'}, std::byte{'1'}};

}  // namespace

result<std::shared_ptr<file>> file::load_remote(std::unique_ptr<byte_source> source) {
  auto f = std::make_shared<file>();
  f->size = source->size();
  f->remote = std::move(source);
  if (f->size < 12) return fail("not a readable Parquet file: too small to hold a footer");
  // The footer length and the trailing magic are the last 8 bytes; the footer is the bytes before them.
  const std::uint64_t tail_len = std::min(f->size, std::max<std::uint64_t>(f->remote->footer_read_size(), 8));
  std::vector<std::byte> tail(static_cast<std::size_t>(tail_len));
  std::string err;
  if (!f->remote->read(f->size - tail_len, tail_len, tail.data(), err)) return fail("cannot read the footer: " + err);
  if (!std::equal(tail.end() - 4, tail.end(), kParquetMagic)) return fail("not a Parquet file: no trailing PAR1");
  std::uint32_t footer_len = 0;
  for (int i = 0; i < 4; ++i)  // little-endian
    footer_len |= std::uint32_t(static_cast<std::uint8_t>(tail[tail.size() - 8 + i])) << (8 * i);
  if (footer_len > f->size - 8) return fail("footer length is larger than the file");
  const std::uint64_t need = std::uint64_t(footer_len) + 8;
  if (need > tail_len) {  // the footer did not fit in the first read: read exactly it
    tail.assign(static_cast<std::size_t>(need), std::byte{0});
    if (!f->remote->read(f->size - need, need, tail.data(), err)) return fail("cannot read the footer: " + err);
  }
  f->footer = std::move(tail);
  // The footer is the last footer_len + 8 bytes of the tail; the metadata is the footer's first footer_len bytes.
  const std::span<const std::byte> meta(f->footer.data() + (f->footer.size() - need), footer_len);
  auto md = pq::thrift_compact<pq::FileMetaData>()(nanom::from(meta));
  if (!md) return fail("not a readable Parquet file: " + md.error().render(nanom::from(meta)));
  if (!md->rest.empty()) return fail("not a readable Parquet file: FileMetaData does not span the footer");
  f->meta = std::move(md->value);
  const nanom::input in = f->input();
  auto rgs = f->meta.row_groups->to_vector();
  if (!rgs) return fail("bad row group metadata: " + rgs.error().render(in));
  f->row_groups = std::move(*rgs);
  auto st = build_schema(*f);
  if (!st) return nanom::unexpected<error>(st.error());
  return f;
}

result<fetched> file::fetch(std::uint64_t offset, std::uint64_t n) const {
  if (offset > size) return fail("read starts past the end of the file");
  n = std::min(n, size - offset);
  fetched out;
  if (!remote) {
    out.view = bytes.subspan(static_cast<std::size_t>(offset), static_cast<std::size_t>(n));
    return out;
  }
  out.owned.resize(static_cast<std::size_t>(n));
  std::string err;
  if (n != 0 && !remote->read(offset, n, out.owned.data(), err)) return fail("cannot read the file: " + err);
  out.view = out.owned;
  return out;
}

result<reader> reader::open(const std::filesystem::path& path, read_options options) {
  const std::string text = path.string();
  if (is_s3_uri(text)) {
    auto source = open_s3_source(text);
    if (!source) return nanom::unexpected<error>(source.error());
    auto f = file::load_remote(std::move(*source));
    if (!f) return nanom::unexpected<error>(f.error());
    return make(std::move(*f), std::move(options));
  }
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
    auto st = node_arrow_schema(*file_, *column_node(i), out->children[i]);
    if (!st) {
      out->release(out);
      return st;
    }
  }
  return ok();
}

const anode* reader::column_node(std::size_t i) const { return &file_->top[top_index_[i]]; }

// Reads the bytes of the given chunks of a row group in one range when they are close together (their range is at
// most twice their bytes); returns nothing otherwise, so each chunk is read by its own request.
result<std::optional<prefetched>> prefetch_chunks(const file& f, const std::vector<pq::ColumnChunk>& chunks,
                                                  const std::vector<std::size_t>& ids) {
  std::uint64_t lo = std::numeric_limits<std::uint64_t>::max(), hi = 0, sum = 0;
  for (const std::size_t id : ids) {
    std::uint64_t s = 0, n = 0;
    auto r = chunk_range(f, chunks[id], s, n);
    if (!r) return nanom::unexpected<error>(r.error());
    lo = std::min(lo, s);
    hi = std::max(hi, s + n);
    sum += n;
  }
  if (ids.empty() || hi <= lo || hi - lo > 2 * sum) return std::optional<prefetched>{};
  auto bytes = f.fetch(lo, hi - lo);
  if (!bytes) return nanom::unexpected<error>(bytes.error());
  prefetched p;
  p.base = lo;
  p.bytes = std::move(*bytes);
  return std::optional<prefetched>(std::move(p));
}

status reader::decode_leaves(const anode& n, const std::vector<pq::ColumnChunk>& chunks, std::int64_t rows,
                             std::vector<leaf_levels>& levels, ArrowArray* arr, const prefetched* pre) const {
  if (n.k == anode::kind::primitive) {
    const leaf& l = file_->leaves[std::size_t(n.leaf)];
    return read_column_chunk(*file_, l, chunks[std::size_t(l.index)], rows, n.d_enc, &levels[std::size_t(l.index)],
                             options_, arr, pre);
  }
  for (std::size_t c = 0; c < n.children.size(); ++c) {
    auto st = decode_leaves(n.children[c], chunks, rows, levels, arr->children[c], pre);
    if (!st) return st;
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
  if (!chunks)
    return fail("bad column chunk metadata in row group " + std::to_string(index) + ": " +
                chunks.error().render(file_->input()));
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
  // Remote file: the chunks this read decodes are fetched together, in one range, when that range is at most twice
  // their bytes; otherwise each chunk is fetched by its own request.
  std::optional<prefetched> group;
  if (file_->remote) {
    std::vector<std::size_t> needed;
    for (std::size_t i = 0; i < columns_.size(); ++i) {
      std::vector<const anode*> leaves;
      column_node(i)->leaves(leaves);
      for (const anode* a : leaves) needed.push_back(std::size_t(file_->leaves[std::size_t(a->leaf)].index));
    }
    auto g = prefetch_chunks(*file_, *chunks, needed);
    if (!g) return nanom::unexpected<error>(g.error());
    group = std::move(*g);
  }
  const prefetched* pre = group ? &*group : nullptr;

  // Output columns are independent: each is decoded into its own child array, by one thread.
  // Workers take the next column from a shared counter; the first failing column (by index) wins.
  const auto decode_column = [&](std::size_t i, std::vector<leaf_levels>& levels) -> status {
    const anode& n = *column_node(i);
    if (n.flat()) {
      const leaf& l = file_->leaves[std::size_t(n.leaf)];
      return read_column_chunk(*file_, l, (*chunks)[std::size_t(l.index)], rows, 0, nullptr, options_, out->children[i],
                               pre);
    }
    // nested: decode every leaf below (keeping its levels), then assemble the structure
    levels.assign(file_->leaves.size(), {});
    auto s = decode_leaves(n, *chunks, rows, levels, out->children[i], pre);
    if (s) s = assemble_nested(*file_, n, levels, rows, out->children[i]);
    return s;
  };
  const std::size_t ncols = columns_.size();
  unsigned threads = options_.threads ? options_.threads : std::max(1u, std::thread::hardware_concurrency());
  threads = unsigned(std::min<std::size_t>(threads, ncols));
  std::vector<std::optional<error>> errors(ncols);
  if (threads <= 1) {
    std::vector<leaf_levels> levels;
    for (std::size_t i = 0; i < ncols; ++i)
      if (auto s = decode_column(i, levels); !s) {
        errors[i] = std::move(s.error());
        break;
      }
  } else {
    std::atomic<std::size_t> next{0};
    std::atomic<bool> failed{false};
    const auto work = [&] {
      std::vector<leaf_levels> levels;
      for (std::size_t i; !failed.load(std::memory_order_relaxed) && (i = next.fetch_add(1)) < ncols;) {
        auto s = decode_column(i, levels);
        if (!s) {
          errors[i] = std::move(s.error());
          failed.store(true, std::memory_order_relaxed);
        }
      }
    };
    std::vector<std::jthread> pool;
    pool.reserve(threads - 1);
    try {
      for (unsigned t = 1; t < threads; ++t) pool.emplace_back(work);
    } catch (const std::system_error&) {
      // no more threads available: the ones started and this one finish the work
    }
    work();
    pool.clear();  // join
  }
  for (std::size_t i = 0; i < ncols; ++i)
    if (errors[i]) {
      out->release(out);
      return fail("row group " + std::to_string(index) + ": " + errors[i]->message);
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
