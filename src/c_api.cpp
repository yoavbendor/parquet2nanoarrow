// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
#include "parquet2nanoarrow/parquet2nanoarrow.h"
#include "parquet2nanoarrow/parquet2nanoarrow.hpp"

#include <cstring>

extern "C" int p2n_open_stream(const char* path, const char* const* columns, int n_columns, int skip_unsupported,
                               ArrowArrayStream* out, char* err, size_t err_len) {
  const auto report = [&](const std::string& msg) {
    if (err && err_len) {
      const std::size_t n = std::min(msg.size(), err_len - 1);
      std::memcpy(err, msg.data(), n);
      err[n] = '\0';
    }
    return 1;
  };
  if (!path || !out) return report("path and out must be non-null");
  p2n::read_options opt;
  opt.skip_unsupported = skip_unsupported != 0;
  for (int i = 0; columns && i < n_columns; ++i) opt.columns.emplace_back(columns[i]);
  auto r = p2n::reader::open(path, std::move(opt));
  if (!r) return report(r.error().message);
  auto st = r->to_stream(out);
  if (!st) return report(st.error().message);
  return 0;
}
