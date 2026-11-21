// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// s3:// inputs. The object is read by range through nanos3reader (one HEAD for its size, then range GETs):
// the footer first, then the column chunks of each row group as they are decoded, so memory holds one row
// group's projected chunks rather than the whole object. Built with -DP2N_S3=ON; without it an s3:// path is
// refused by name.
#include "internal.hpp"

#include <memory>
#include <string>

#ifdef P2N_HAVE_S3
#include "nanos3reader/s3_range.h"
#endif

namespace p2n {

bool is_s3_uri(const std::string& path) { return path.starts_with("s3://"); }

#ifdef P2N_HAVE_S3

namespace {

class s3_byte_source final : public byte_source {
 public:
  explicit s3_byte_source(std::unique_ptr<nanos3reader::RangeSource> source) : source_(std::move(source)) {}
  std::uint64_t size() const override { return source_->size(); }
  bool read(std::uint64_t offset, std::uint64_t n, std::byte* dst, std::string& err) override {
    return source_->read_at(offset, n, reinterpret_cast<std::uint8_t*>(dst), err);
  }
  std::uint64_t footer_read_size() const override { return source_->footer_read_size(); }

 private:
  std::unique_ptr<nanos3reader::RangeSource> source_;
};

}  // namespace

result<std::unique_ptr<byte_source>> open_s3_source(const std::string& uri) {
  auto client = std::make_shared<nanos3reader::S3Client>();  // credentials and endpoint from the environment
  nanos3reader::S3RangeOptions options;
  options.block_cache = false;  // each request is a column chunk or a footer: already large
  std::string err;
  auto source = nanos3reader::open_s3_range_source(client, uri, err, options);
  if (!source) return fail(err);
  return std::unique_ptr<byte_source>(new s3_byte_source(std::move(source)));
}

#else

result<std::unique_ptr<byte_source>> open_s3_source(const std::string& uri) {
  return fail("built without S3 support (configure with -DP2N_S3=ON): " + uri);
}

#endif

}  // namespace p2n
