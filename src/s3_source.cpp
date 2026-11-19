// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// s3:// inputs. The object is read whole into memory through nanos3reader's range source (one HEAD, then
// range GETs), and the reader then decodes it exactly as a local file. Built with -DP2N_S3=ON; without it an
// s3:// path is refused by name.
#include "internal.hpp"

#include <algorithm>
#include <memory>
#include <string>

#ifdef P2N_HAVE_S3
#include "nanos3reader/s3_range.h"
#endif

namespace p2n {

bool is_s3_uri(const std::string& path) { return path.starts_with("s3://"); }

#ifdef P2N_HAVE_S3

result<std::shared_ptr<mapped_file>> open_s3(const std::string& uri) {
  auto client = std::make_shared<nanos3reader::S3Client>();  // credentials and endpoint from the environment
  nanos3reader::S3RangeOptions options;
  options.block_cache = false;  // the whole object is read once, in large ranges
  std::string err;
  auto source = nanos3reader::open_s3_range_source(client, uri, err, options);
  if (!source) return fail(err);
  std::vector<std::byte> bytes(static_cast<std::size_t>(source->size()));
  constexpr std::uint64_t kChunk = std::uint64_t{8} << 20;
  for (std::uint64_t off = 0; off < bytes.size(); off += kChunk) {
    const std::uint64_t n = std::min<std::uint64_t>(kChunk, bytes.size() - off);
    if (!source->read_at(off, n, reinterpret_cast<std::uint8_t*>(bytes.data() + off), err)) return fail(err);
  }
  return mapped_file::from_bytes(std::move(bytes));
}

#else

result<std::shared_ptr<mapped_file>> open_s3(const std::string& uri) {
  return fail("built without S3 support (configure with -DP2N_S3=ON): " + uri);
}

#endif

}  // namespace p2n
