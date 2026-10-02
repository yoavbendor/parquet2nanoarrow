// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Column chunk -> Arrow array. This file is the Parquet framing only: it walks the chunk's pages
// (Thrift page headers decoded by nanom's reflected model), decompresses each page (nanom's Snappy
// / LZ4, or zstd / zlib), splits it into levels and values, and picks the nanom kernel for each
// encoding: nanom/columnar.hpp (bits -> integers), nanom/values.hpp (levels, bitmaps, null
// spreading, byte arrays, dictionaries, UTF-8, widening) and nanom/formats/parquet_values.hpp.
// Values are written straight into the nanoarrow output buffers:
//
//   * fixed-width values are decoded densely at their final position and then spread in place over
//     the null slots (one backward pass), so a column without nulls is written exactly once;
//   * an uncompressed page is decoded straight from the memory-mapped file (no staging copy);
//   * every count, length, index and offset that comes from the file is checked before use.
#include "internal.hpp"

#include <nanom/formats/parquet_values.hpp>
#include <nanom/values.hpp>

#include <zstd.h>
#ifdef P2N_HAVE_ZLIB
#include <zlib.h>
#endif

#include <algorithm>

namespace p2n {
namespace {

namespace col = nanom::columnar;
using pq::CompressionCodec;
using pq::Encoding;
using pq::Type;

using bytes_span = std::span<const std::byte>;

std::string enc_name(Encoding e) { return "encoding " + std::to_string(int(e)); }

std::uint32_t le32(const std::byte* p) {
  std::uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}
std::uint32_t be32(const std::byte* p) {
  return (std::uint32_t(std::uint8_t(p[0])) << 24) | (std::uint32_t(std::uint8_t(p[1])) << 16) |
         (std::uint32_t(std::uint8_t(p[2])) << 8) | std::uint32_t(std::uint8_t(p[3]));
}

// ---- decompression ----------------------------------------------------------------------------

/// Decompress `in` into exactly `out_size` bytes. Uncompressed data is returned as a view of the
/// input (zero copy); everything else lands in `buf`.
status decompress(CompressionCodec codec, bytes_span in, std::size_t out_size, std::vector<std::byte>& buf,
                  bytes_span& out) {
  if (out_size == 0) {  // an empty page (e.g. a data page v2 holding only nulls): nothing to inflate
    out = {};
    return ok();
  }
  if (codec == CompressionCodec::UNCOMPRESSED) {
    if (in.size() < out_size) return fail("uncompressed page shorter than its declared size");
    out = in.first(out_size);
    return ok();
  }
  buf.resize(out_size);
  const auto bad = [&](const char* what) { return fail(std::string("page decompression failed: ") + what); };
  switch (codec) {
    case CompressionCodec::SNAPPY: {
      auto r = nanom::codec::snappy_decompress(in, buf);
      if (!r) return bad(r.error().what);
      break;
    }
    case CompressionCodec::LZ4_RAW: {
      auto r = nanom::codec::lz4_block_decompress(in, buf);
      if (!r) return bad(r.error().what);
      if (*r != out_size) return bad("lz4: size differs from the page header");
      break;
    }
    case CompressionCodec::LZ4: {
      // Legacy Hadoop framing: [u32 BE raw size][u32 BE compressed size][LZ4 block]... ; some
      // writers emitted a bare block under this codec id, so fall back to that.
      std::size_t ip = 0, op = 0;
      bool framed = true;
      while (ip < in.size() && framed) {
        if (in.size() - ip < 8) { framed = false; break; }
        const std::size_t raw = be32(in.data() + ip), comp = be32(in.data() + ip + 4);
        ip += 8;
        if (comp > in.size() - ip || raw > out_size - op) { framed = false; break; }
        auto r = nanom::codec::lz4_block_decompress(in.subspan(ip, comp), std::span<std::byte>(buf).subspan(op, raw));
        if (!r || *r != raw) { framed = false; break; }
        ip += comp;
        op += raw;
      }
      if (!framed || op != out_size) {
        auto r = nanom::codec::lz4_block_decompress(in, buf);
        if (!r) return bad(r.error().what);
        if (*r != out_size) return bad("lz4: size differs from the page header");
      }
      break;
    }
    case CompressionCodec::ZSTD: {
      const std::size_t r = ZSTD_decompress(buf.data(), buf.size(), in.data(), in.size());
      if (ZSTD_isError(r)) return bad(ZSTD_getErrorName(r));
      if (r != out_size) return bad("zstd: size differs from the page header");
      break;
    }
    case CompressionCodec::GZIP: {
#ifdef P2N_HAVE_ZLIB
      z_stream zs{};
      if (inflateInit2(&zs, 15 + 32) != Z_OK) return bad("zlib init");  // gzip or zlib header
      zs.next_in = reinterpret_cast<Bytef*>(const_cast<std::byte*>(in.data()));
      zs.avail_in = uInt(std::min<std::size_t>(in.size(), UINT32_MAX));
      zs.next_out = reinterpret_cast<Bytef*>(buf.data());
      zs.avail_out = uInt(std::min<std::size_t>(buf.size(), UINT32_MAX));
      // a page may hold several concatenated gzip members: restart after each until the input or
      // the declared output is used up
      int rc = Z_OK;
      for (;;) {
        rc = inflate(&zs, Z_FINISH);
        if (rc != Z_STREAM_END || zs.avail_in == 0 || zs.avail_out == 0) break;
        if (inflateReset(&zs) != Z_OK) break;
      }
      const std::size_t produced = buf.size() - zs.avail_out;  // total_out restarts at each member
      inflateEnd(&zs);
      if (rc != Z_STREAM_END) return bad("gzip stream did not end where the page header says");
      if (produced != out_size) return bad("gzip: size differs from the page header");
      break;
#else
      return fail("GZIP pages need zlib (rebuild with zlib available)");
#endif
    }
    default:
      return fail("compression codec " + std::to_string(int(codec)) + " is not supported");
  }
  out = buf;
  return ok();
}

// ---- small helpers ----------------------------------------------------------------------------

// ---- the chunk decoder -------------------------------------------------------------------------

class chunk_decoder {
 public:
  chunk_decoder(const file& f, const leaf& l, const read_options& opt, ArrowArray* out, std::int64_t rows,
                std::int16_t d_enc, leaf_levels* keep)
      : f_(f), l_(l), opt_(opt), out_(out), rows_(rows), d_enc_(d_enc), keep_(keep) {}

  status run(const pq::ColumnChunk& chunk) {
    if (!chunk.meta_data->has_value()) return fail("column chunk without metadata (encrypted or external)");
    if (chunk.file_path->has_value()) return fail("column chunk stored in an external file is not supported");
    const pq::ColumnMetaData& md = **chunk.meta_data;
    if (*md.type != l_.physical) return fail("column chunk type differs from the schema");
    target_ = *md.num_values;
    if (target_ < 0) return fail("negative value count in column chunk");
    if (!keep_) {
      if (target_ != rows_)
        return fail("column chunk holds " + std::to_string(target_) + " values for " + std::to_string(rows_) +
                    " rows (flat column expected)");
      cap_ = rows_;
    } else {
      // nested leaf: one level entry per value slot or empty/null ancestor; slots <= entries
      if (target_ > opt_.max_row_group_rows)
        return fail("column chunk declares " + std::to_string(target_) +
                    " nested values, over read_options::max_row_group_rows");
      cap_ = target_;
      keep_->rep.reserve(std::size_t(target_));
      keep_->def.reserve(std::size_t(target_));
    }
    codec_ = *md.codec;

    std::int64_t start = *md.data_page_offset;
    if (md.dictionary_page_offset->has_value() && **md.dictionary_page_offset > 0 &&
        **md.dictionary_page_offset < start)
      start = **md.dictionary_page_offset;
    std::int64_t len = *md.total_compressed_size;
    const std::uint64_t fsize = f_.bytes.size();
    if (start < 0 || len < 0 || std::uint64_t(start) > fsize || std::uint64_t(len) > fsize - std::uint64_t(start))
      return fail("column chunk byte range lies outside the file");
    // PARQUET-816: some writers left the dictionary page header out of total_compressed_size.
    // Allow a bounded overrun (as Arrow does); pages are still bounds-checked against the file and
    // decoding stops at the declared value count, so a well-formed chunk is unaffected.
    len += std::int64_t(std::min<std::uint64_t>(100, fsize - std::uint64_t(start) - std::uint64_t(len)));

    auto st = init_output();
    if (!st) return st;
    const nanom::input whole = f_.input();
    nanom::input cur = whole.advance(std::size_t(start));
    cur = cur.with_range(cur.first, cur.first + len);
    while (entries_ < target_) {
      if (cur.empty())
        return fail("column chunk ended after " + std::to_string(entries_) + " of " + std::to_string(target_) + " values");
      auto ph = nanom::thrift_compact<pq::PageHeader>()(cur);
      if (!ph) return fail("bad page header: " + ph.error().render(whole));
      const pq::PageHeader& h = ph->value;
      const std::int32_t csize = *h.compressed_page_size, usize = *h.uncompressed_page_size;
      if (csize < 0 || usize < 0 || std::size_t(csize) > ph->rest.size())
        return fail("page size out of range for the column chunk");
      if (std::size_t(usize) > opt_.max_page_bytes)
        return fail("page uncompressed size " + std::to_string(usize) + " exceeds read_options::max_page_bytes");
      const bytes_span body(ph->rest.first, std::size_t(csize));
      switch (*h.type) {
        case pq::PageType::DICTIONARY_PAGE:
          st = dictionary_page(h, body);
          break;
        case pq::PageType::DATA_PAGE:
          st = data_page_v1(h, body);
          break;
        case pq::PageType::DATA_PAGE_V2:
          st = data_page_v2(h, body);
          break;
        default:
          st = ok();  // index pages carry nothing we need
      }
      if (!st) return st;
      cur = ph->rest.advance(std::size_t(csize));
    }
    return finish();
  }

 private:
  // ---- output buffers ----
  status init_output() {
    if (l_.kind == conv::null) return ok();  // the null type has no buffers
    validity_ = ArrowArrayValidityBitmap(out_);
    use_pool(&validity_->buffer);
    use_pool(ArrowArrayBuffer(out_, 1));
    if (l_.kind == conv::binary) use_pool(ArrowArrayBuffer(out_, 2));
    if (ArrowBitmapReserve(validity_, cap_) != NANOARROW_OK) return fail("out of memory");
    if (cap_ > 0) std::memset(validity_->buffer.data, 0xff, std::size_t((cap_ + 7) / 8));
    data_ = ArrowArrayBuffer(out_, 1);
    if (l_.kind == conv::boolean) {
      if (ArrowBufferReserve(data_, (cap_ + 7) / 8) != NANOARROW_OK) return fail("out of memory");
      if (cap_ > 0) std::memset(data_->data, 0, std::size_t((cap_ + 7) / 8));
    } else if (l_.kind == conv::binary) {
      if (ArrowBufferReserve(data_, (cap_ + 1) * 4) != NANOARROW_OK) return fail("out of memory");
      std::memset(data_->data, 0, 4);
      bdata_ = ArrowArrayBuffer(out_, 2);
    } else {
      const std::int64_t bytes = cap_ * std::int64_t(l_.out_width);
      if (ArrowBufferReserve(data_, bytes) != NANOARROW_OK) return fail("out of memory");
    }
    return ok();
  }

  status finish() {
    if (!keep_ && row_ != rows_) return fail("internal: flat column produced " + std::to_string(row_) + " rows");
    if (l_.kind == conv::null) {
      out_->length = row_;
      out_->null_count = row_;
      return ok();
    }
    if (null_count_ == 0) {
      ArrowBitmapReset(validity_);
    } else {
      validity_->size_bits = row_;
      validity_->buffer.size_bytes = (row_ + 7) / 8;
    }
    if (l_.kind == conv::boolean) data_->size_bytes = (row_ + 7) / 8;
    else if (l_.kind == conv::binary) data_->size_bytes = (row_ + 1) * 4;
    else data_->size_bytes = row_ * std::int64_t(l_.out_width);
    out_->length = row_;
    out_->null_count = null_count_;
    return ok();
  }

  // ---- pages ----
  status dictionary_page(const pq::PageHeader& h, bytes_span body) {
    if (!h.dictionary_page_header->has_value()) return fail("dictionary page without its header");
    const auto& dh = **h.dictionary_page_header;
    const auto enc = *dh.encoding;
    if (enc != Encoding::PLAIN && enc != Encoding::PLAIN_DICTIONARY) return fail("dictionary page " + enc_name(enc));
    if (*dh.num_values < 0) return fail("negative dictionary size");
    bytes_span page;
    auto st = decompress(codec_, body, std::size_t(*h.uncompressed_page_size), page_buf_, page);
    if (!st) return st;
    const std::size_t n = std::size_t(*dh.num_values);
    if (l_.physical == Type::BOOLEAN) return fail("dictionary-encoded BOOLEAN column");
    if (l_.physical == Type::BYTE_ARRAY) {
      if (n > page.size() / 4) return fail("dictionary entry count exceeds the page");
      views_.resize(n);
      if (!col::length_prefixed_views(page, n, views_)) return fail("dictionary entry runs past the page");
      std::size_t total = 0;
      for (const auto v : views_) total += v.size();
      dict_data_.resize(total + col::kValueSlack);  // readable slack for fixed 16-byte copies
      dict_off_.resize(n + 1);
      dict_off_[0] = 0;
      std::int64_t cur = 0;
      col::append_views(std::span<const std::string_view>(views_), n, nullptr, dict_data_.data(), cur, dict_off_.data() + 1);
      if (opt_.validate_utf8 && l_.format == "u") {
        // check the dictionary ONCE: lookups into it never need re-checking
        if (!col::valid_utf8(std::string_view(reinterpret_cast<const char*>(dict_data_.data()), total)) ||
            !col::utf8_starts_ok(dict_data_.data(), std::int64_t(total), dict_off_.data(), n))
          return fail("invalid UTF-8 in the dictionary of string column '" + l_.name + "'");
      }
    } else {
      const std::size_t w = l_.phys_width;
      if (n > page.size() / w) return fail("dictionary entry count exceeds the page");
      dict_fixed_.assign(page.data(), page.data() + n * w);
    }
    dict_n_ = n;
    have_dict_ = true;
    return ok();
  }

  status data_page_v1(const pq::PageHeader& h, bytes_span body) {
    if (!h.data_page_header->has_value()) return fail("data page without its header");
    const auto& dh = **h.data_page_header;
    if (*dh.num_values < 0) return fail("negative page value count");
    if (*dh.encoding == Encoding::PLAIN) {
      bool done = false;
      auto st = l_.max_def == 0
                    ? direct_plain(codec_, body, std::size_t(*h.uncompressed_page_size), std::size_t(*dh.num_values), done)
                    : in_place_v1(dh, body, std::size_t(*h.uncompressed_page_size), done);
      if (!st || done) return st;
    }
    bytes_span page;
    auto st = decompress(codec_, body, std::size_t(*h.uncompressed_page_size), page_buf_, page);
    if (!st) return st;
    return v1_levels_and_values(dh, page);
  }

  /// Split a decompressed v1 page into its levels and values, then decode them.
  status v1_levels_and_values(const pq::DataPageHeader& dh, bytes_span page) {
    bytes_span rep_lv, def;
    if (l_.max_rep > 0) {
      if (*dh.repetition_level_encoding != Encoding::RLE)
        return fail("repetition levels in " + enc_name(*dh.repetition_level_encoding) + " (only RLE is supported)");
      if (page.size() < 4) return fail("truncated repetition levels");
      const std::uint32_t len = le32(page.data());
      if (len > page.size() - 4) return fail("repetition levels run past the page");
      rep_lv = page.subspan(4, len);
      page = page.subspan(4 + len);
    }
    if (l_.max_def > 0) {
      if (*dh.definition_level_encoding != Encoding::RLE)
        return fail("definition levels in " + enc_name(*dh.definition_level_encoding) + " (only RLE is supported)");
      if (page.size() < 4) return fail("truncated definition levels");
      const std::uint32_t len = le32(page.data());
      if (len > page.size() - 4) return fail("definition levels run past the page");
      def = page.subspan(4, len);
      page = page.subspan(4 + len);
    }
    return page_values(std::size_t(*dh.num_values), rep_lv, def, *dh.encoding, page, -1);
  }

  status data_page_v2(const pq::PageHeader& h, bytes_span body) {
    if (!h.data_page_header_v2->has_value()) return fail("data page v2 without its header");
    const auto& dh = **h.data_page_header_v2;
    const std::int32_t rl = *dh.repetition_levels_byte_length, dl = *dh.definition_levels_byte_length;
    if (*dh.num_values < 0 || *dh.num_nulls < 0 || rl < 0 || dl < 0 || std::size_t(rl) + std::size_t(dl) > body.size() ||
        std::size_t(rl) + std::size_t(dl) > std::size_t(*h.uncompressed_page_size))
      return fail("data page v2 level sizes out of range");
    const bytes_span def = body.subspan(std::size_t(rl), std::size_t(dl));
    const bytes_span vals_in = body.subspan(std::size_t(rl) + std::size_t(dl));
    const std::size_t vals_size = std::size_t(*h.uncompressed_page_size) - std::size_t(rl) - std::size_t(dl);
    bytes_span vals;
    const bool compressed = dh.is_compressed->value_or(true);
    if (!keep_ && compressed && *dh.num_nulls == 0 && *dh.encoding == Encoding::PLAIN && l_.max_rep == 0) {
      bool done = false;
      auto st = direct_plain(codec_, vals_in, vals_size, std::size_t(*dh.num_values), done);
      if (!st || done) return st;
    }
    auto st = decompress(compressed ? codec_ : CompressionCodec::UNCOMPRESSED, vals_in, vals_size, page_buf_, vals);
    if (!st) return st;
    const bytes_span rep_lv = body.subspan(0, std::size_t(rl));
    return page_values(std::size_t(*dh.num_values), l_.max_rep > 0 ? rep_lv : bytes_span{},
                       l_.max_def > 0 ? def : bytes_span{}, *dh.encoding, vals, keep_ ? -1 : *dh.num_nulls);
  }

  /// PLAIN fixed-width page whose decompressed bytes ARE the output values (no levels inside, no
  /// nulls): decompress straight into the Arrow buffer at this page's rows — no staging copy.
  /// Returns false (doing nothing) when the page does not qualify.
  status direct_plain(CompressionCodec codec, bytes_span body, std::size_t out_size, std::size_t n, bool& done) {
    done = false;
    if (keep_ || l_.kind != conv::copy || codec == CompressionCodec::UNCOMPRESSED || l_.out_width == 0) return ok();
    if (std::int64_t(n) > cap_ - row_ || out_size != n * l_.out_width) return ok();
    if (!direct_codec(codec)) return ok();  // other codecs take the staged path
    std::span<std::byte> target(reinterpret_cast<std::byte*>(data_->data) + std::size_t(row_) * l_.out_width, out_size);
    auto st = decompress_into(codec, body, target);
    if (!st) return st;
    row_ += std::int64_t(n);
    entries_ += std::int64_t(n);
    done = true;
    return ok();
  }

  static bool direct_codec(CompressionCodec c) {
    return c == CompressionCodec::SNAPPY || c == CompressionCodec::LZ4_RAW || c == CompressionCodec::ZSTD;
  }
  /// Decompress into exactly `target` (a direct_codec codec).
  static status decompress_into(CompressionCodec codec, bytes_span body, std::span<std::byte> target) {
    switch (codec) {
      case CompressionCodec::SNAPPY: {
        auto r = nanom::codec::snappy_decompress(body, target);
        if (!r) return fail(std::string("page decompression failed: ") + r.error().what);
        return ok();
      }
      case CompressionCodec::LZ4_RAW: {
        auto r = nanom::codec::lz4_block_decompress(body, target);
        if (!r || *r != target.size()) return fail("page decompression failed: lz4");
        return ok();
      }
      case CompressionCodec::ZSTD: {
        const std::size_t r = ZSTD_decompress(target.data(), target.size(), body.data(), body.size());
        if (ZSTD_isError(r) || r != target.size()) return fail("page decompression failed: zstd");
        return ok();
      }
      default:
        return fail("internal: decompress_into codec");
    }
  }

  /// PLAIN fixed-width v1 page of a flat optional column: [u32 len][def levels][values]. Guess
  /// that the page has no nulls, so the values are its last n * width bytes, and decompress the
  /// whole page to start (4 + len) bytes BEFORE this page's rows in the Arrow buffer: the values
  /// then land in place and the staging copy disappears. The few bytes in front (earlier rows'
  /// values) are saved and restored around it; the level prefix is copied out first. When the
  /// guess is wrong (the prefix is not where the values start) the page is rebuilt in the
  /// staging buffer and decoded normally. Returns with done == false (nothing touched) when the
  /// page does not qualify.
  status in_place_v1(const pq::DataPageHeader& dh, bytes_span body, std::size_t size, bool& done) {
    static constexpr std::size_t kMaxPrefix = 64;
    done = false;
    const std::size_t w = l_.out_width, n = std::size_t(*dh.num_values);
    if (keep_ || l_.max_rep != 0 || l_.kind != conv::copy || w == 0 || w != l_.phys_width ||
        !direct_codec(codec_) || *dh.definition_level_encoding != Encoding::RLE)
      return ok();
    if (std::int64_t(n) > cap_ - row_ || size < n * w || size - n * w < 4 || size - n * w > kMaxPrefix ||
        std::size_t(row_) * w < size - n * w)
      return ok();
    const std::size_t pre_n = size - n * w;
    std::byte* const dst = reinterpret_cast<std::byte*>(data_->data) + std::size_t(row_) * w;
    std::byte* const base = dst - pre_n;
    std::byte saved[kMaxPrefix], pre[kMaxPrefix];
    std::memcpy(saved, base, pre_n);
    auto st = decompress_into(codec_, body, std::span<std::byte>(base, size));
    std::memcpy(pre, base, pre_n);
    std::memcpy(base, saved, pre_n);
    if (!st) return st;
    done = true;
    const std::uint32_t len = le32(pre);
    if (std::size_t(len) + 4 != pre_n) {
      // the guess was wrong: reassemble the page and take the staged path
      page_buf_.resize(size);
      std::memcpy(page_buf_.data(), pre, pre_n);
      std::memcpy(page_buf_.data() + pre_n, dst, size - pre_n);
      return v1_levels_and_values(dh, bytes_span(page_buf_));
    }
    return page_values(n, {}, bytes_span(pre + 4, len), Encoding::PLAIN, bytes_span(dst, n * w), -1);
  }

  /// Nested leaf: decode this page's rep/def levels, append them to keep_, and build the page's
  /// slot validity (slots = entries with def >= d_enc; valid = def == max_def). Returns the slot
  /// count in `slots` and the non-null count in `nn`.
  status nested_levels(std::size_t n, bytes_span rep, bytes_span def, std::size_t& slots, std::size_t& nn) {
    const std::size_t base = keep_->def.size();
    keep_->rep.resize(base + n);
    keep_->def.resize(base + n);
    std::uint16_t* const rl = keep_->rep.data() + base;
    std::uint16_t* const dl0 = keep_->def.data() + base;
    if (l_.max_rep > 0) {
      if (auto k = col::decode_levels(rep, std::uint16_t(l_.max_rep), n, rl); !k)
        return fail(std::string("repetition levels: ") + k.error);
      if (n && entries_ == 0 && rl[0] != 0) return fail("column chunk does not start at a row boundary");
    } else {
      std::fill_n(rl, n, std::uint16_t(0));
    }
    if (l_.max_def > 0) {
      if (auto k = col::decode_levels(def, std::uint16_t(l_.max_def), n, dl0); !k)
        return fail(std::string("definition levels: ") + k.error);
    } else {
      std::fill_n(dl0, n, std::uint16_t(0));
    }
    const std::uint16_t* dl = keep_->def.data() + base;
    pv_.assign(((n + 63) / 64) * 8, std::uint8_t(0));
    const auto c = col::level_slots(dl, n, std::uint16_t(d_enc_), std::uint16_t(l_.max_def), pv_.data());
    slots = c.slots;
    nn = c.non_null;
    return ok();
  }

  /// Common tail of v1/v2 pages: levels -> validity, then values.
  status page_values(std::size_t n, bytes_span rep, bytes_span def, Encoding enc, bytes_span vals,
                     std::int64_t declared_nulls) {
    if (std::int64_t(n) > target_ - entries_) return fail("pages hold more values than the column chunk declares");
    entries_ += std::int64_t(n);
    if (keep_) {
      std::size_t slots = 0, nn = 0;
      auto st = nested_levels(n, rep, def, slots, nn);
      if (!st) return st;
      if (std::int64_t(slots) > cap_ - row_) return fail("internal: nested slots exceed capacity");
      has_nulls_in_page_ = nn != slots;
      if (l_.kind != conv::null && has_nulls_in_page_ &&
          !col::copy_bits(std::as_bytes(std::span(pv_)), slots,
                          std::span<std::byte>(reinterpret_cast<std::byte*>(validity_->buffer.data),
                                               std::size_t((cap_ + 7) / 8)),
                          std::size_t(row_)))
        return fail("internal: validity bitmap too small");
      return values(enc, vals, slots, nn);
    }
    if (std::int64_t(n) > cap_ - row_) return fail("pages hold more values than the row group has rows");
    std::size_t nn = n;
    has_nulls_in_page_ = false;
    if (l_.max_def > 0) {
      // page-local validity (padded to whole 64-bit words for the word-at-a-time spread below)
      pv_.assign(((n + 63) / 64) * 8, std::uint8_t(0));
      if (l_.max_def == 1) {
        // flat optional column: level 1 = valid, 0 = null, so the levels ARE the validity bitmap
        col::rle_bp_decoder d(def, 1);
        if (auto k = col::rle_bitmap(d, n, std::as_writable_bytes(std::span(pv_)), 0); !k)
          return fail(std::string("definition levels: ") + k.error);
        nn = col::count_bits(pv_.data(), 0, n);
      } else {
        levels_.resize(n);
        if (auto k = col::decode_levels(def, std::uint16_t(l_.max_def), n, levels_.data()); !k)
          return fail(std::string("definition levels: ") + k.error);
        nn = col::level_slots(levels_.data(), n, 0, std::uint16_t(l_.max_def), pv_.data()).non_null;
      }
      has_nulls_in_page_ = nn != n;
      if (l_.kind != conv::null && has_nulls_in_page_ &&
          !col::copy_bits(std::as_bytes(std::span(pv_)), n,
                          std::span<std::byte>(reinterpret_cast<std::byte*>(validity_->buffer.data),
                                               std::size_t((cap_ + 7) / 8)),
                          std::size_t(row_)))
        return fail("internal: validity bitmap too small");
    }
    if (declared_nulls >= 0 && std::size_t(declared_nulls) != n - nn) return fail("data page v2 num_nulls disagrees with its levels");
    return values(enc, vals, n, nn);
  }

  /// Decode nn non-null values and place them over the page's n slots (validity in pv_).
  status values(Encoding enc, bytes_span vals, std::size_t n, std::size_t nn) {
    null_count_ += std::int64_t(n - nn);
    if (l_.kind == conv::null) {
      if (nn) return fail("non-null value in a column annotated UNKNOWN (the null type)");
      row_ += std::int64_t(n);
      return ok();
    }
    status st = ok();
    switch (l_.kind) {
      case conv::boolean: st = booleans(enc, vals, n, nn); break;
      case conv::binary:  st = binaries(enc, vals, n, nn); break;
      case conv::be_to_dec128:
        if (l_.physical == Type::BYTE_ARRAY) { st = binaries(enc, vals, n, nn); break; }
        [[fallthrough]];
      default:            st = fixed(enc, vals, n, nn); break;
    }
    if (!st) return st;
    row_ += std::int64_t(n);
    return ok();
  }

  bool valid(std::size_t i) const { return !has_nulls_in_page_ || ((pv_[i / 8] >> (i % 8)) & 1); }

  // ---- fixed-width values ----
  /// Decode nn physical values densely into dst (nn * phys_width bytes).
  status decode_fixed_dense(Encoding enc, bytes_span vals, std::size_t nn, std::byte* dst) {
    const std::size_t w = l_.phys_width;
    switch (enc) {
      case Encoding::PLAIN:
        if (nn > vals.size() / w) return fail("PLAIN page shorter than its values");
        if (nn && vals.data() != dst) std::memcpy(dst, vals.data(), nn * w);  // in_place_v1: already there
        return ok();
      case Encoding::PLAIN_DICTIONARY:
      case Encoding::RLE_DICTIONARY: {
        if (!have_dict_) return fail("dictionary-encoded page without a dictionary page");
        idx_.resize(nn);
        if (auto k = pq::dictionary_indices(vals, nn, dict_n_, idx_); !k) return fail(k.error);
        col::gather_fixed(dict_fixed_.data(), w, std::span<const std::uint32_t>(idx_.data(), nn), dst);
        return ok();
      }
      case Encoding::DELTA_BINARY_PACKED: {
        if (l_.physical == Type::INT32) {
          auto r = col::delta_binary_packed<std::int32_t>(vals, std::span<std::int32_t>(reinterpret_cast<std::int32_t*>(dst), nn), nn);
          return r ? ok() : fail("malformed DELTA_BINARY_PACKED page");
        }
        if (l_.physical == Type::INT64) {
          auto r = col::delta_binary_packed<std::int64_t>(vals, std::span<std::int64_t>(reinterpret_cast<std::int64_t*>(dst), nn), nn);
          return r ? ok() : fail("malformed DELTA_BINARY_PACKED page");
        }
        return fail("DELTA_BINARY_PACKED on a non-integer column");
      }
      case Encoding::BYTE_STREAM_SPLIT:
        return col::byte_stream_split(vals, w, nn, std::span<std::byte>(dst, nn * w))
                   ? ok() : fail("BYTE_STREAM_SPLIT page shorter than its values");
      case Encoding::DELTA_BYTE_ARRAY: {
        if (l_.physical != Type::FIXED_LEN_BYTE_ARRAY) return fail("DELTA_BYTE_ARRAY on a fixed-width numeric column");
        auto st = decode_views(enc, vals, nn);
        if (!st) return st;
        for (std::size_t i = 0; i < nn; ++i) {
          if (views_[i].size() != w) return fail("DELTA_BYTE_ARRAY value length differs from the column width");
          std::memcpy(dst + i * w, views_[i].data(), w);
        }
        return ok();
      }
      default:
        return fail(enc_name(enc) + " is not supported for this column type");
    }
  }

  status fixed(Encoding enc, bytes_span vals, std::size_t n, std::size_t nn) {
    const std::size_t ow = l_.out_width;
    std::byte* dst = reinterpret_cast<std::byte*>(data_->data) + std::size_t(row_) * ow;
    if (l_.kind == conv::copy) {
      auto st = decode_fixed_dense(enc, vals, nn, dst);
      if (!st) return st;
      if (nn != n) col::spread_nulls(dst, n, nn, ow, pv_.data());
      return ok();
    }
    // converting kinds: dense physical values into scratch, then convert into each row's slot
    const std::size_t pw = l_.phys_width;
    dense_.resize(nn * pw);
    auto st = decode_fixed_dense(enc, vals, nn, dense_.data());
    if (!st) return st;
    std::size_t j = 0;
    for (std::size_t r = 0; r < n; ++r) {
      std::byte* o = dst + r * ow;
      if (!valid(r)) {
        std::memset(o, 0, ow);
        continue;
      }
      const std::byte* in = dense_.data() + (j++) * pw;
      switch (l_.kind) {
        case conv::i32_to_i8: { std::int32_t v; std::memcpy(&v, in, 4); const auto b = std::int8_t(v); std::memcpy(o, &b, 1); break; }
        case conv::i32_to_i16: { std::int32_t v; std::memcpy(&v, in, 4); const auto b = std::int16_t(v); std::memcpy(o, &b, 2); break; }
        case conv::int96_to_ns: { const std::int64_t t = pq::int96_to_unix_nanos(in); std::memcpy(o, &t, 8); break; }
        case conv::i32_to_dec128: { std::int32_t v; std::memcpy(&v, in, 4); col::sign_extend_le(v, o, 16); break; }
        case conv::i64_to_dec128: { std::int64_t v; std::memcpy(&v, in, 8); col::sign_extend_le(v, o, 16); break; }
        case conv::be_to_dec128: col::sign_extend_be(in, pw, o, 16); break;
        default: return fail("internal: unexpected conversion");
      }
    }
    return ok();
  }

  // ---- booleans ----
  status booleans(Encoding enc, bytes_span vals, std::size_t n, std::size_t nn) {
    auto* bits = reinterpret_cast<std::byte*>(data_->data);
    const std::span<std::byte> out(bits, std::size_t((cap_ + 7) / 8));
    if (enc == Encoding::PLAIN && nn == n)
      return col::copy_bits(vals, n, out, std::size_t(row_)) ? ok() : fail("PLAIN boolean page shorter than its values");
    // the nn values as a dense bitmap: PLAIN pages are one already; RLE pages are expanded run by
    // run (an RLE run is a range fill, a bit-packed run of width 1 is a bitmap slice)
    bytes_span dense;
    if (enc == Encoding::PLAIN) {
      if (vals.size() < (nn + 7) / 8) return fail("PLAIN boolean page shorter than its values");
      dense = vals;
    } else if (enc == Encoding::RLE) {
      if (vals.size() < 4) return fail("truncated RLE boolean page");
      const std::uint32_t len = le32(vals.data());
      if (len > vals.size() - 4) return fail("RLE boolean run data past the page");
      col::rle_bp_decoder d(vals.subspan(4, len), 1);
      if (nn == n) {  // no nulls: the runs go straight into the output bitmap
        auto k = col::rle_bitmap(d, n, out, std::size_t(row_));
        return k ? ok() : fail(std::string("RLE boolean page: ") + k.error);
      }
      dense_bits_.assign((nn + 7) / 8, 0);
      if (auto k = col::rle_bitmap(d, nn, std::as_writable_bytes(std::span(dense_bits_)), 0); !k)
        return fail(std::string("RLE boolean page: ") + k.error);
      dense = std::as_bytes(std::span(dense_bits_));
    } else {
      return fail(enc_name(enc) + " is not supported for BOOLEAN");
    }
    // nulls: value j goes to the j-th non-null row (null rows stay 0)
    col::scatter_bits(reinterpret_cast<const std::uint8_t*>(dense.data()), has_nulls_in_page_ ? pv_.data() : nullptr, n,
                      reinterpret_cast<std::uint8_t*>(bits), std::size_t(row_));
    return ok();
  }

  // ---- byte arrays ----
  /// Decode nn byte-array values of the page into views_ (into the page, the dictionary or arena_).
  status decode_views(Encoding enc, bytes_span vals, std::size_t nn) {
    views_.resize(nn);
    switch (enc) {
      case Encoding::PLAIN:
        return col::length_prefixed_views(vals, nn, views_) ? ok() : fail("PLAIN byte array runs past the page");
      case Encoding::PLAIN_DICTIONARY:
      case Encoding::RLE_DICTIONARY: {
        if (!have_dict_) return fail("dictionary-encoded page without a dictionary page");
        idx_.resize(nn);
        if (auto k = pq::dictionary_indices(vals, nn, dict_n_, idx_); !k) return fail(k.error);
        const char* dd = reinterpret_cast<const char*>(dict_data_.data());
        for (std::size_t i = 0; i < nn; ++i)
          views_[i] = std::string_view(dd + dict_off_[idx_[i]], dict_off_[idx_[i] + 1] - dict_off_[idx_[i]]);
        return ok();
      }
      case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
        auto k = col::delta_length_views(vals, nn, lengths_, views_);
        return k ? ok() : fail(k.error);
      }
      case Encoding::DELTA_BYTE_ARRAY: {
        auto k = col::delta_prefix_views(vals, nn, prefix_, lengths_, arena_, views_, opt_.max_page_bytes);
        return k ? ok() : fail(k.error);
      }
      default:
        return fail(enc_name(enc) + " is not supported for BYTE_ARRAY");
    }
  }

  /// UTF-8 for the bytes this page appended, in one pass: the concatenation must be valid and every
  /// value must start on a character boundary (then each value is valid on its own).
  status check_utf8_page(std::int64_t from, std::int64_t to, std::size_t n) const {
    const auto* d = reinterpret_cast<const std::byte*>(bdata_->data);
    if (!col::valid_utf8(std::string_view(reinterpret_cast<const char*>(d) + from, std::size_t(to - from))) ||
        !col::utf8_starts_ok(d, to, reinterpret_cast<const std::int32_t*>(data_->data) + row_, n))
      return fail("invalid UTF-8 in string column '" + l_.name + "'");
    return ok();
  }

  status binaries(Encoding enc, bytes_span vals, std::size_t n, std::size_t nn) {
    if (l_.kind == conv::be_to_dec128) {  // DECIMAL stored as BYTE_ARRAY
      auto st = decode_views(enc, vals, nn);
      if (!st) return st;
      std::byte* dst = reinterpret_cast<std::byte*>(data_->data) + std::size_t(row_) * 16;
      std::size_t j = 0;
      for (std::size_t r = 0; r < n; ++r) {
        if (!valid(r)) { std::memset(dst + r * 16, 0, 16); continue; }
        const auto v = views_[j++];
        if (v.size() > 16) return fail("decimal value wider than 16 bytes");
        col::sign_extend_be(reinterpret_cast<const std::byte*>(v.data()), v.size(), dst + r * 16, 16);
      }
      return ok();
    }
    const bool utf8 = opt_.validate_utf8 && l_.format == "u";
    const std::uint8_t* validity = has_nulls_in_page_ ? pv_.data() : nullptr;
    auto* offs = reinterpret_cast<std::int32_t*>(data_->data);
    const std::int64_t start = bdata_->size_bytes;
    std::int64_t cur = start;
    const auto too_big = [&] {
      return fail("column '" + l_.name + "' exceeds 2 GiB of data in one row group (large_string not supported yet)");
    };
    switch (enc) {
      case Encoding::PLAIN: {  // one pass: parse each length, copy, write the offset
        if (ArrowBufferReserve(bdata_, std::int64_t(vals.size() + col::kValueSlack)) != NANOARROW_OK) return fail("out of memory");
        auto k = col::append_length_prefixed(vals, n, validity, reinterpret_cast<std::byte*>(bdata_->data), cur,
                                             offs + row_ + 1, INT32_MAX);
        if (!k) return cur > INT32_MAX ? too_big() : fail(std::string("PLAIN byte array: ") + k.error);
        break;
      }
      case Encoding::PLAIN_DICTIONARY:
      case Encoding::RLE_DICTIONARY: {  // indices first, then exact-size gather from the dictionary
        if (!have_dict_) return fail("dictionary-encoded page without a dictionary page");
        idx_.resize(nn);
        if (auto k = pq::dictionary_indices(vals, nn, dict_n_, idx_); !k) return fail(k.error);
        const std::span<const std::uint32_t> idx(idx_.data(), nn);
        const auto total = std::int64_t(col::gathered_size(dict_off_.data(), idx));
        if (start + total > INT32_MAX) return too_big();
        if (ArrowBufferReserve(bdata_, total + std::int64_t(col::kValueSlack)) != NANOARROW_OK) return fail("out of memory");
        // dict_data_ carries kValueSlack padding: every value may move as a 16-byte copy
        col::append_gathered(dict_data_.data(), dict_data_.data() + dict_data_.size(), dict_off_.data(), idx, n, validity,
                             reinterpret_cast<std::byte*>(bdata_->data), cur, offs + row_ + 1);
        bdata_->size_bytes = cur;
        return ok();  // the dictionary was UTF-8 checked once, when it was read
      }
      default: {  // DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY: through views
        auto st = decode_views(enc, vals, nn);
        if (!st) return st;
        std::int64_t total = 0;
        for (std::size_t i = 0; i < nn; ++i) total += std::int64_t(views_[i].size());
        if (start + total > INT32_MAX) return too_big();
        if (ArrowBufferReserve(bdata_, total + std::int64_t(col::kValueSlack)) != NANOARROW_OK) return fail("out of memory");
        col::append_views(std::span<const std::string_view>(views_.data(), nn), n, validity,
                          reinterpret_cast<std::byte*>(bdata_->data), cur, offs + row_ + 1);
      }
    }
    bdata_->size_bytes = cur;
    if (utf8) return check_utf8_page(start, cur, n);
    return ok();
  }

  const file& f_;
  const leaf& l_;
  const read_options& opt_;
  ArrowArray* out_;
  std::int64_t rows_;
  std::int16_t d_enc_ = 0;
  leaf_levels* keep_ = nullptr;
  std::int64_t cap_ = 0;       ///< value slots the output buffers hold
  std::int64_t target_ = 0;    ///< level entries (values incl. nulls) the chunk declares
  std::int64_t entries_ = 0;   ///< level entries consumed so far
  std::int64_t row_ = 0, null_count_ = 0;  ///< row_: value slots written so far
  CompressionCodec codec_ = CompressionCodec::UNCOMPRESSED;
  ArrowBitmap* validity_ = nullptr;
  ArrowBuffer* data_ = nullptr;
  ArrowBuffer* bdata_ = nullptr;
  bool has_nulls_in_page_ = false;

  bool have_dict_ = false;
  std::size_t dict_n_ = 0;

  // Scratch buffers live per thread and are reused across column chunks (grow-only, bounded by
  // the largest page / dictionary seen): allocating them per chunk paid fresh page faults for every
  // decompression buffer of every column of every row group. Decoding on one thread is strictly
  // sequential, so one set per thread is enough; have_dict_ / dict_n_ above stay per chunk, so a
  // previous chunk's dictionary contents can never be used.
  struct scratch {
    std::vector<std::byte> dict_fixed, dict_data, page_buf, dense;
    std::vector<std::size_t> dict_off;
    std::vector<std::uint8_t> dense_bits, pv;
    std::vector<std::uint16_t> levels;
    std::vector<std::uint32_t> idx;
    std::vector<std::string_view> views;
    std::vector<std::int32_t> lengths, prefix;
    std::vector<char> arena;
  };
  static scratch& thread_scratch() {
    static thread_local scratch s;
    return s;
  }
  scratch& s_ = thread_scratch();
  std::vector<std::byte>& dict_fixed_ = s_.dict_fixed;
  std::vector<std::byte>& dict_data_ = s_.dict_data;
  std::vector<std::size_t>& dict_off_ = s_.dict_off;
  std::vector<std::byte>& page_buf_ = s_.page_buf;
  std::vector<std::byte>& dense_ = s_.dense;
  std::vector<std::uint8_t>& dense_bits_ = s_.dense_bits;
  std::vector<std::uint16_t>& levels_ = s_.levels;
  std::vector<std::uint8_t>& pv_ = s_.pv;  ///< page-local validity bits, padded to whole 64-bit words
  std::vector<std::string_view>& views_ = s_.views;
  std::vector<std::int32_t>& lengths_ = s_.lengths;
  std::vector<std::int32_t>& prefix_ = s_.prefix;
  std::vector<std::uint32_t>& idx_ = s_.idx;
  std::vector<char>& arena_ = s_.arena;
};

}  // namespace

status read_column_chunk(const file& f, const leaf& l, const pq::ColumnChunk& chunk, std::int64_t rows,
                         std::int16_t d_enc, leaf_levels* keep, const read_options& opt, ArrowArray* out) {
  chunk_decoder d(f, l, opt, out, rows, d_enc, keep);
  auto st = d.run(chunk);
  if (!st) return fail("column '" + l.name + "': " + st.error().message);
  return st;
}

}  // namespace p2n
