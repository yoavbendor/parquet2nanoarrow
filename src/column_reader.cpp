// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Column chunk -> Arrow array. Walks the chunk's pages (Thrift page headers decoded by nanom's
// reflected model), decompresses each page, decodes definition levels and values with nanom's
// columnar kernels, and writes straight into the nanoarrow output buffers:
//
//   * fixed-width values are decoded densely at their final position and then spread in place over
//     the null slots (one backward pass), so a column without nulls is written exactly once;
//   * an uncompressed page is decoded straight from the memory-mapped file (no staging copy);
//   * every count, length, index and offset that comes from the file is checked before use.
#include "internal.hpp"

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

bool valid_utf8(std::string_view s) {
  const auto* p = reinterpret_cast<const unsigned char*>(s.data());
  const auto* e = p + s.size();
  while (p < e) {
    if (e - p >= 16) {  // ASCII fast path: 16 bytes per step
      std::uint64_t w0, w1;
      std::memcpy(&w0, p, 8);
      std::memcpy(&w1, p + 8, 8);
      if (!((w0 | w1) & 0x8080808080808080ull)) { p += 16; continue; }
    } else if (e - p >= 8) {
      std::uint64_t w;
      std::memcpy(&w, p, 8);
      if (!(w & 0x8080808080808080ull)) { p += 8; continue; }
    }
    const unsigned c = *p;
    if (c < 0x80) { ++p; continue; }
    int n;
    std::uint32_t cp;
    if ((c & 0xe0) == 0xc0) { n = 1; cp = c & 0x1f; }
    else if ((c & 0xf0) == 0xe0) { n = 2; cp = c & 0x0f; }
    else if ((c & 0xf8) == 0xf0) { n = 3; cp = c & 0x07; }
    else return false;
    if (e - p <= n) return false;
    for (int i = 1; i <= n; ++i) {
      if ((p[i] & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (p[i] & 0x3f);
    }
    if ((n == 1 && cp < 0x80) || (n == 2 && cp < 0x800) || (n == 3 && cp < 0x10000) || cp > 0x10ffff ||
        (cp >= 0xd800 && cp <= 0xdfff))
      return false;
    p += n + 1;
  }
  return true;
}

void be_to_dec128(const std::byte* b, std::size_t k, std::byte* out) {
  const std::byte sign = k && (std::uint8_t(b[0]) & 0x80) ? std::byte{0xff} : std::byte{0};
  for (std::size_t i = 0; i < 16; ++i) out[i] = i < k ? b[k - 1 - i] : sign;
}
void i64_to_dec128(std::int64_t v, std::byte* out) {
  std::memcpy(out, &v, 8);
  const std::int64_t hi = v < 0 ? -1 : 0;
  std::memcpy(out + 8, &hi, 8);
}

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
      dict_off_.assign(1, 0);
      dict_data_.clear();
      if (n > page.size() / 4) return fail("dictionary entry count exceeds the page");
      dict_off_.reserve(n + 1);
      std::size_t p = 0;
      for (std::size_t i = 0; i < n; ++i) {
        if (page.size() - p < 4) return fail("truncated dictionary entry");
        const std::uint32_t len = le32(page.data() + p);
        p += 4;
        if (len > page.size() - p) return fail("dictionary entry runs past the page");
        dict_data_.insert(dict_data_.end(), page.data() + p, page.data() + p + len);
        p += len;
        dict_off_.push_back(dict_data_.size());
      }
      if (opt_.validate_utf8 && l_.format == "u") {
        // check the dictionary ONCE: lookups into it never need re-checking
        const std::string_view all(reinterpret_cast<const char*>(dict_data_.data()), dict_data_.size());
        if (!valid_utf8(all)) return fail("invalid UTF-8 in the dictionary of string column '" + l_.name + "'");
        for (std::size_t i = 0; i < n; ++i)
          if (dict_off_[i] < dict_data_.size() && (std::uint8_t(dict_data_[dict_off_[i]]) & 0xc0) == 0x80)
            return fail("invalid UTF-8 in the dictionary of string column '" + l_.name + "'");
      }
      dict_data_.resize(dict_data_.size() + kSlack);  // readable slack for fixed 16-byte copies
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

  /// Decode n RLE / bit-packed levels (each <= maxv) into out, run by run: an RLE run is a fill,
  /// a bit-packed run is unpacked in blocks. Returns null on success, else what went wrong.
  const char* decode_levels(bytes_span data, std::int16_t maxv, std::size_t n, std::uint16_t* out) {
    const unsigned w = unsigned(std::bit_width(unsigned(maxv)));
    col::rle_bp_decoder d(data, w);
    col::rle_bp_decoder::run lr;
    std::uint32_t tmp[512];
    for (std::size_t i = 0; i < n; i += lr.count) {
      if (!d.next_run(lr, n - i)) return "shorter than the page";
      if (!lr.packed) {
        if (lr.value > std::uint32_t(maxv)) return "level above the column's maximum";
        std::fill_n(out + i, lr.count, std::uint16_t(lr.value));
        continue;
      }
      std::uint32_t hi = 0;
      if (lr.bit_offset == 0) {
        for (std::size_t k = 0; k < lr.count; k += 512) {
          const std::size_t m = std::min<std::size_t>(512, lr.count - k);
          const std::size_t byte = k * w / 8;  // k is a multiple of 8: a whole-byte position
          if (!col::unpack_bits<std::uint32_t>(lr.bits.subspan(byte), w, std::span<std::uint32_t>(tmp), m))
            return "shorter than the page";
          for (std::size_t j = 0; j < m; ++j) {
            hi |= tmp[j] > std::uint32_t(maxv) ? 1u : 0u;
            out[i + k + j] = std::uint16_t(tmp[j]);
          }
        }
      } else {  // a run resumed mid-byte (not produced by one pass, kept for generality)
        const auto* b = reinterpret_cast<const std::uint8_t*>(lr.bits.data());
        for (std::size_t k = 0; k < lr.count; ++k) {
          const std::size_t bit = lr.bit_offset + k * w;
          std::uint32_t v = 0;
          for (unsigned t = 0; t < w; ++t) {
            const std::size_t q = bit + t;
            if (q / 8 >= lr.bits.size()) return "shorter than the page";
            v |= std::uint32_t((b[q / 8] >> (q % 8)) & 1) << t;
          }
          hi |= v > std::uint32_t(maxv) ? 1u : 0u;
          out[i + k] = std::uint16_t(v);
        }
      }
      if (hi) return "level above the column's maximum";
    }
    return nullptr;
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
      if (const char* e = decode_levels(rep, l_.max_rep, n, rl)) return fail(std::string("repetition levels: ") + e);
      if (n && entries_ == 0 && rl[0] != 0) return fail("column chunk does not start at a row boundary");
    } else {
      std::fill_n(rl, n, std::uint16_t(0));
    }
    if (l_.max_def > 0) {
      if (const char* e = decode_levels(def, l_.max_def, n, dl0)) return fail(std::string("definition levels: ") + e);
    } else {
      std::fill_n(dl0, n, std::uint16_t(0));
    }
    const std::uint16_t* dl = keep_->def.data() + base;
    pv_.assign(((n + 63) / 64) * 8, std::uint8_t(0));
    slots = 0;
    nn = 0;
    for (std::size_t i = 0; i < n; ++i) {
      if (dl[i] < std::uint16_t(d_enc_)) continue;  // an empty or null ancestor: no slot here
      if (dl[i] == std::uint16_t(l_.max_def)) {
        pv_[slots / 8] = std::uint8_t(pv_[slots / 8] | (1u << (slots % 8)));
        ++nn;
      }
      ++slots;
    }
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
        // flat optional column: level 1 = valid, 0 = null. Act on whole runs: an RLE run is a
        // range fill, a bit-packed run of width-1 levels is already a validity bitmap.
        col::rle_bp_decoder d(def, 1);
        col::rle_bp_decoder::run lr;
        std::size_t i = 0;
        nn = 0;
        while (i < n) {
          if (!d.next_run(lr, n - i)) return fail("definition levels shorter than the page");
          if (!lr.packed) {
            if (lr.value > 1) return fail("definition level above the column's maximum");
            if (lr.value == 1) set_bits(pv_.data(), i, lr.count);
          } else if (lr.bit_offset == 0) {
            if (!col::copy_bits(lr.bits, lr.count, std::as_writable_bytes(std::span(pv_)), i))
              return fail("definition levels shorter than the page");
          } else {
            for (std::size_t k = 0; k < lr.count; ++k) {
              const std::size_t b = lr.bit_offset + k;
              if ((std::uint8_t(lr.bits[b / 8]) >> (b % 8)) & 1)
                pv_[(i + k) / 8] = std::uint8_t(pv_[(i + k) / 8] | (1u << ((i + k) % 8)));
            }
          }
          i += lr.count;
        }
        // copy_bits may leave level bits beyond n in the last byte: clear them, then count once
        if (n % 8) pv_[n / 8] = std::uint8_t(pv_[n / 8] & ((1u << (n % 8)) - 1));
        nn = count_bits(pv_.data(), 0, pv_.size() * 8);
      } else {
        levels_.resize(n);
        col::rle_bp_decoder d(def, unsigned(std::bit_width(unsigned(l_.max_def))));
        if (d.get(levels_.data(), n) != n || !d.ok()) return fail("definition levels shorter than the page");
        nn = 0;
        for (std::size_t i = 0; i < n; ++i) {
          if (levels_[i] > std::uint32_t(l_.max_def)) return fail("definition level above the column's maximum");
          if (levels_[i] == std::uint32_t(l_.max_def)) {
            pv_[i / 8] = std::uint8_t(pv_[i / 8] | (1u << (i % 8)));
            ++nn;
          }
        }
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

  static void set_bits(std::uint8_t* bm, std::size_t start, std::size_t count) {
    std::size_t i = start, end = start + count;
    for (; i < end && i % 8; ++i) bm[i / 8] = std::uint8_t(bm[i / 8] | (1u << (i % 8)));
    if (end - i >= 8) {
      std::memset(bm + i / 8, 0xff, (end - i) / 8);
      i += ((end - i) / 8) * 8;
    }
    for (; i < end; ++i) bm[i / 8] = std::uint8_t(bm[i / 8] | (1u << (i % 8)));
  }
  static std::size_t count_bits(const std::uint8_t* bm, std::size_t start, std::size_t count) {
    std::size_t c = 0, i = start, end = start + count;
    for (; i < end && i % 64; ++i) c += (bm[i / 8] >> (i % 8)) & 1;
    for (; end - i >= 64; i += 64) {
      std::uint64_t w;
      std::memcpy(&w, bm + i / 8, 8);
      c += std::size_t(std::popcount(w));
    }
    for (; i < end; ++i) c += (bm[i / 8] >> (i % 8)) & 1;
    return c;
  }

  /// Move nn densely decoded values (at dst[0 .. nn)) to their rows in dst[0 .. n), back to front,
  /// zero-filling null slots. Whole 64-row words that are all valid / all null move as one block;
  /// mixed words take a branchless per-row step (no mispredictions at any null density).
  template <std::size_t W>
  void spread_w(std::byte* dst, std::size_t n, std::size_t nn) const {
    std::size_t j = nn, r = n;
    const std::uint8_t* bm = pv_.data();
    while (r > 0) {
      if (j == 0) {  // only nulls remain
        std::memset(dst, 0, r * W);
        return;
      }
      if (r % 64 == 0) {
        std::uint64_t word;
        std::memcpy(&word, bm + (r - 64) / 8, 8);
        if (word == ~std::uint64_t(0)) {
          j -= 64;
          r -= 64;
          if (j != r) std::memmove(dst + r * W, dst + j * W, 64 * W);
          continue;
        }
        if (word == 0) {
          r -= 64;
          std::memset(dst + r * W, 0, 64 * W);
          continue;
        }
        // mixed word: 64 branchless steps (j >= popcount(word) >= 1 keeps dst[j - 1] in range)
        if (j >= 64) {
          for (int k = 0; k < 64; ++k) {
            --r;
            const std::size_t bit = (word >> 63) & 1;
            word <<= 1;
            std::byte v[W];
            std::memcpy(v, dst + (j - 1) * W, W);
            if constexpr (W <= 8) {
              using U = std::conditional_t<W == 1, std::uint8_t, std::conditional_t<W == 2, std::uint16_t,
                        std::conditional_t<W <= 4, std::uint32_t, std::uint64_t>>>;
              U u = 0;
              std::memcpy(&u, v, W);
              u = U(u & (U(0) - U(bit)));
              std::memcpy(dst + r * W, &u, W);
            } else {
              const std::byte m = bit ? std::byte{0xff} : std::byte{0};
              for (std::size_t b = 0; b < W; ++b) v[b] &= m;
              std::memcpy(dst + r * W, v, W);
            }
            j -= bit;
          }
          continue;
        }
      }
      --r;
      if ((bm[r / 8] >> (r % 8)) & 1) {
        --j;
        if (j != r) std::memcpy(dst + r * W, dst + j * W, W);
      } else {
        std::memset(dst + r * W, 0, W);
      }
    }
  }

  void spread(std::byte* dst, std::size_t n, std::size_t nn, std::size_t w) const {
    switch (w) {
      case 1: return spread_w<1>(dst, n, nn);
      case 2: return spread_w<2>(dst, n, nn);
      case 4: return spread_w<4>(dst, n, nn);
      case 8: return spread_w<8>(dst, n, nn);
      case 16: return spread_w<16>(dst, n, nn);
      default: break;
    }
    std::size_t j = nn;
    for (std::size_t r = n; r-- > 0;) {
      if ((pv_[r / 8] >> (r % 8)) & 1) {
        --j;
        if (j != r) std::memcpy(dst + r * w, dst + j * w, w);
      } else {
        std::memset(dst + r * w, 0, w);
      }
    }
  }

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
        if (vals.empty()) return nn ? fail("dictionary indices missing") : ok();
        const unsigned bw = std::uint8_t(vals[0]);
        if (bw > 32) return fail("dictionary index bit width over 32");
        col::rle_bp_decoder d(vals.subspan(1), bw);
        std::uint32_t idx[1024];
        std::size_t done = 0;
        while (done < nn) {
          const std::size_t k = d.get(idx, std::min<std::size_t>(1024, nn - done));
          if (!k) return fail("dictionary indices shorter than the page");
          for (std::size_t i = 0; i < k; ++i)
            if (idx[i] >= dict_n_) return fail("dictionary index out of range");
          const std::byte* src = dict_fixed_.data();
          std::byte* o = dst + done * w;
          if (w == 4) for (std::size_t i = 0; i < k; ++i) std::memcpy(o + 4 * i, src + 4 * std::size_t(idx[i]), 4);
          else if (w == 8) for (std::size_t i = 0; i < k; ++i) std::memcpy(o + 8 * i, src + 8 * std::size_t(idx[i]), 8);
          else for (std::size_t i = 0; i < k; ++i) std::memcpy(o + w * i, src + w * std::size_t(idx[i]), w);
          done += k;
        }
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
      if (nn != n) spread(dst, n, nn, ow);
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
        case conv::int96_to_ns: {
          std::uint64_t nanos;
          std::uint32_t jd;
          std::memcpy(&nanos, in, 8);
          std::memcpy(&jd, in + 8, 4);
          // wrapping arithmetic: (julian_day - 2440588) * 86400e9 + nanos, as Arrow computes it
          const std::uint64_t t = (std::uint64_t(std::int64_t(jd) - 2440588) * 86400000000000ull) + nanos;
          std::memcpy(o, &t, 8);
          break;
        }
        case conv::i32_to_dec128: { std::int32_t v; std::memcpy(&v, in, 4); i64_to_dec128(v, o); break; }
        case conv::i64_to_dec128: { std::int64_t v; std::memcpy(&v, in, 8); i64_to_dec128(v, o); break; }
        case conv::be_to_dec128: be_to_dec128(in, pw, o); break;
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
      dense_bits_.assign((nn + 7) / 8 + 8, 0);
      std::uint8_t* db = dense_bits_.data();
      col::rle_bp_decoder d(vals.subspan(4, len), 1);
      col::rle_bp_decoder::run lr;
      for (std::size_t i = 0; i < nn; i += lr.count) {
        if (!d.next_run(lr, nn - i)) return fail("RLE boolean page shorter than its values");
        if (!lr.packed) {
          if (lr.value > 1) return fail("RLE boolean value above 1");
          if (lr.value) set_bits(db, i, lr.count);
        } else if (lr.bit_offset == 0) {
          if (!col::copy_bits(lr.bits, lr.count, std::as_writable_bytes(std::span(dense_bits_)), i))
            return fail("RLE boolean page shorter than its values");
        } else {
          for (std::size_t k = 0; k < lr.count; ++k) {
            const std::size_t b = lr.bit_offset + k;
            if ((std::uint8_t(lr.bits[b / 8]) >> (b % 8)) & 1) db[(i + k) / 8] = std::uint8_t(db[(i + k) / 8] | (1u << ((i + k) % 8)));
          }
        }
      }
      if (nn == n)
        return col::copy_bits(std::as_bytes(std::span(dense_bits_)), n, out, std::size_t(row_)) ? ok()
                                                                                              : fail("internal: boolean bitmap");
      dense = std::as_bytes(std::span(dense_bits_));
    } else {
      return fail(enc_name(enc) + " is not supported for BOOLEAN");
    }
    // nulls: place value j at the j-th valid row (null rows stay 0)
    std::size_t j = 0;
    auto* o = reinterpret_cast<std::uint8_t*>(bits);
    const auto* dv = reinterpret_cast<const std::uint8_t*>(dense.data());
    for (std::size_t r = 0; r < n; ++r) {
      if (!valid(r)) continue;
      const std::size_t b = std::size_t(row_) + r;
      o[b / 8] = std::uint8_t(o[b / 8] | (((dv[j / 8] >> (j % 8)) & 1u) << (b % 8)));
      ++j;
    }
    return ok();
  }

  // ---- byte arrays ----
  /// Decode nn byte-array values of the page into views_ (into the page, the dictionary or arena_).
  status decode_views(Encoding enc, bytes_span vals, std::size_t nn) {
    views_.resize(nn);
    const auto sv = [](const std::byte* p, std::size_t n) { return std::string_view(reinterpret_cast<const char*>(p), n); };
    switch (enc) {
      case Encoding::PLAIN: {
        std::size_t p = 0;
        for (std::size_t i = 0; i < nn; ++i) {
          if (vals.size() - p < 4) return fail("truncated PLAIN byte array");
          const std::uint32_t len = le32(vals.data() + p);
          p += 4;
          if (len > vals.size() - p) return fail("PLAIN byte array runs past the page");
          views_[i] = sv(vals.data() + p, len);
          p += len;
        }
        return ok();
      }
      case Encoding::PLAIN_DICTIONARY:
      case Encoding::RLE_DICTIONARY: {
        if (!have_dict_) return fail("dictionary-encoded page without a dictionary page");
        if (vals.empty()) return nn ? fail("dictionary indices missing") : ok();
        const unsigned bw = std::uint8_t(vals[0]);
        if (bw > 32) return fail("dictionary index bit width over 32");
        col::rle_bp_decoder d(vals.subspan(1), bw);
        std::uint32_t idx[1024];
        std::size_t done = 0;
        while (done < nn) {
          const std::size_t k = d.get(idx, std::min<std::size_t>(1024, nn - done));
          if (!k) return fail("dictionary indices shorter than the page");
          for (std::size_t i = 0; i < k; ++i) {
            if (idx[i] >= dict_n_) return fail("dictionary index out of range");
            const std::size_t a = dict_off_[idx[i]], b = dict_off_[idx[i] + 1];
            views_[done + i] = sv(dict_data_.data() + a, b - a);
          }
          done += k;
        }
        return ok();
      }
      case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
        lengths_.resize(nn);
        auto used = col::delta_binary_packed<std::int32_t>(vals, std::span<std::int32_t>(lengths_), nn);
        if (!used) return fail("malformed DELTA_LENGTH_BYTE_ARRAY lengths");
        std::size_t p = *used;
        for (std::size_t i = 0; i < nn; ++i) {
          if (lengths_[i] < 0 || std::size_t(lengths_[i]) > vals.size() - p) return fail("DELTA_LENGTH_BYTE_ARRAY data past the page");
          views_[i] = sv(vals.data() + p, std::size_t(lengths_[i]));
          p += std::size_t(lengths_[i]);
        }
        return ok();
      }
      case Encoding::DELTA_BYTE_ARRAY: {
        prefix_.resize(nn);
        lengths_.resize(nn);
        auto u1 = col::delta_binary_packed<std::int32_t>(vals, std::span<std::int32_t>(prefix_), nn);
        if (!u1) return fail("malformed DELTA_BYTE_ARRAY prefix lengths");
        const bytes_span rest = vals.subspan(*u1);
        auto u2 = col::delta_binary_packed<std::int32_t>(rest, std::span<std::int32_t>(lengths_), nn);
        if (!u2) return fail("malformed DELTA_BYTE_ARRAY suffix lengths");
        // size the arena once (views point into it), validating every prefix/suffix on the way
        std::size_t total = 0, prev = 0, p = *u2;
        for (std::size_t i = 0; i < nn; ++i) {
          if (prefix_[i] < 0 || lengths_[i] < 0 || std::size_t(prefix_[i]) > prev ||
              std::size_t(lengths_[i]) > rest.size() - p)
            return fail("DELTA_BYTE_ARRAY prefix/suffix out of range");
          p += std::size_t(lengths_[i]);
          prev = std::size_t(prefix_[i]) + std::size_t(lengths_[i]);
          if (prev > opt_.max_page_bytes || total > opt_.max_page_bytes - prev)
            return fail("DELTA_BYTE_ARRAY expands past read_options::max_page_bytes");
          total += prev;
        }
        arena_.resize(total);
        std::size_t at = 0, prev_at = 0;
        p = *u2;
        for (std::size_t i = 0; i < nn; ++i) {
          const std::size_t pre = std::size_t(prefix_[i]), suf = std::size_t(lengths_[i]);
          if (pre) std::memmove(arena_.data() + at, arena_.data() + prev_at, pre);
          if (suf) std::memcpy(arena_.data() + at + pre, rest.data() + p, suf);
          p += suf;
          views_[i] = std::string_view(arena_.data() + at, pre + suf);
          prev_at = at;
          at += pre + suf;
        }
        return ok();
      }
      default:
        return fail(enc_name(enc) + " is not supported for BYTE_ARRAY");
    }
  }

  static constexpr std::size_t kSlack = 16;

  /// Copy one value: a short value with 16 readable bytes behind it moves as one fixed 16-byte copy
  /// (the output carries kSlack writable bytes), anything else as an ordinary memcpy.
  static void copy_value(std::byte* dst, const std::byte* src, std::size_t len, const std::byte* src_end) {
    if (len <= 16 && src_end - src >= 16) std::memcpy(dst, src, 16);
    else if (len) std::memcpy(dst, src, len);
  }

  /// UTF-8 for the bytes this page appended, in one pass: the concatenation must be valid and every
  /// value must start on a character boundary (then each value is valid on its own).
  status check_utf8_page(std::int64_t from, std::int64_t to, std::size_t n) const {
    const auto* d = reinterpret_cast<const std::byte*>(bdata_->data);
    if (!valid_utf8(std::string_view(reinterpret_cast<const char*>(d) + from, std::size_t(to - from))))
      return fail("invalid UTF-8 in string column '" + l_.name + "'");
    const auto* offs = reinterpret_cast<const std::int32_t*>(data_->data) + row_;
    for (std::size_t r = 0; r < n; ++r) {
      const std::int32_t o = offs[r];
      if (o < to && (std::uint8_t(d[o]) & 0xc0) == 0x80)
        return fail("invalid UTF-8 in string column '" + l_.name + "'");
    }
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
        be_to_dec128(reinterpret_cast<const std::byte*>(v.data()), v.size(), dst + r * 16);
      }
      return ok();
    }
    const bool utf8 = opt_.validate_utf8 && l_.format == "u";
    auto* offs = reinterpret_cast<std::int32_t*>(data_->data);
    const std::int64_t start = bdata_->size_bytes;
    std::int64_t cur = start;
    const auto too_big = [&] {
      return fail("column '" + l_.name + "' exceeds 2 GiB of data in one row group (large_string not supported yet)");
    };
    switch (enc) {
      case Encoding::PLAIN: {  // one pass: parse each length, copy, write the offset
        if (ArrowBufferReserve(bdata_, std::int64_t(vals.size() + kSlack)) != NANOARROW_OK) return fail("out of memory");
        std::byte* out = reinterpret_cast<std::byte*>(bdata_->data);
        const std::byte* p = vals.data();
        const std::byte* const e = p + vals.size();
        for (std::size_t r = 0; r < n; ++r) {
          if (valid(r)) {
            if (e - p < 4) return fail("truncated PLAIN byte array");
            const std::uint32_t len = le32(p);
            p += 4;
            if (len > std::size_t(e - p)) return fail("PLAIN byte array runs past the page");
            copy_value(out + cur, p, len, e);
            p += len;
            cur += len;
            if (cur > INT32_MAX) return too_big();
          }
          offs[row_ + std::int64_t(r) + 1] = std::int32_t(cur);
        }
        break;
      }
      case Encoding::PLAIN_DICTIONARY:
      case Encoding::RLE_DICTIONARY: {  // indices first, then exact-size gather from the dictionary
        if (!have_dict_) return fail("dictionary-encoded page without a dictionary page");
        idx_.resize(nn);
        if (nn) {
          if (vals.empty()) return fail("dictionary indices missing");
          const unsigned bw = std::uint8_t(vals[0]);
          if (bw > 32) return fail("dictionary index bit width over 32");
          col::rle_bp_decoder d(vals.subspan(1), bw);
          if (d.get(idx_.data(), nn) != nn || !d.ok()) return fail("dictionary indices shorter than the page");
        }
        std::int64_t total = 0;
        for (std::size_t i = 0; i < nn; ++i) {
          if (idx_[i] >= dict_n_) return fail("dictionary index out of range");
          total += std::int64_t(dict_off_[idx_[i] + 1] - dict_off_[idx_[i]]);
        }
        if (start + total > INT32_MAX) return too_big();
        if (ArrowBufferReserve(bdata_, total + std::int64_t(kSlack)) != NANOARROW_OK) return fail("out of memory");
        std::byte* out = reinterpret_cast<std::byte*>(bdata_->data);
        const std::byte* dd = dict_data_.data();
        const std::byte* const dend = dd + dict_data_.size();  // includes kSlack padding
        std::size_t j = 0;
        for (std::size_t r = 0; r < n; ++r) {
          if (valid(r)) {
            const std::size_t a = dict_off_[idx_[j]], len = dict_off_[idx_[j] + 1] - a;
            ++j;
            copy_value(out + cur, dd + a, len, dend);
            cur += std::int64_t(len);
          }
          offs[row_ + std::int64_t(r) + 1] = std::int32_t(cur);
        }
        bdata_->size_bytes = cur;
        return ok();  // the dictionary was UTF-8 checked once, when it was read
      }
      default: {  // DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY: through views
        auto st = decode_views(enc, vals, nn);
        if (!st) return st;
        std::int64_t total = 0;
        for (std::size_t i = 0; i < nn; ++i) total += std::int64_t(views_[i].size());
        if (start + total > INT32_MAX) return too_big();
        if (ArrowBufferReserve(bdata_, total + std::int64_t(kSlack)) != NANOARROW_OK) return fail("out of memory");
        std::byte* out = reinterpret_cast<std::byte*>(bdata_->data);
        std::size_t j = 0;
        for (std::size_t r = 0; r < n; ++r) {
          if (valid(r)) {
            const auto v = views_[j++];
            if (!v.empty()) std::memcpy(out + cur, v.data(), v.size());
            cur += std::int64_t(v.size());
          }
          offs[row_ + std::int64_t(r) + 1] = std::int32_t(cur);
        }
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
    std::vector<std::uint32_t> levels, idx;
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
  std::vector<std::uint32_t>& levels_ = s_.levels;
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
