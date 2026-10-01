// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// p2n_cat — read a Parquet file with parquet2nanoarrow and print its schema and per-column
// summaries (rows, nulls) for each row group. A smoke tool: the exact value-level comparison
// against pyarrow lives in tests/differential.py.
//   p2n_cat file.parquet [column ...]
#include <parquet2nanoarrow/parquet2nanoarrow.hpp>

#include <cstdio>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: p2n_cat file.parquet [column ...]\n");
    return 2;
  }
  p2n::read_options opt;
  for (int i = 2; i < argc; ++i) opt.columns.emplace_back(argv[i]);
  opt.skip_unsupported = opt.columns.empty();
  auto r = p2n::reader::open(argv[1], opt);
  if (!r) {
    std::fprintf(stderr, "p2n_cat: %s\n", r.error().message.c_str());
    return 1;
  }
  std::printf("%lld rows, %zu row groups\n", static_cast<long long>(r->num_rows()), r->num_row_groups());
  for (const auto& c : r->columns()) std::printf("  %-24s %-12s %s\n", c.name.c_str(), c.arrow_format.c_str(), c.nullable ? "nullable" : "required");
  for (std::size_t g = 0; g < r->num_row_groups(); ++g) {
    ArrowArray a;
    auto st = r->read_row_group(g, &a);
    if (!st) {
      std::fprintf(stderr, "p2n_cat: %s\n", st.error().message.c_str());
      return 1;
    }
    std::printf("row group %zu: %lld rows;", g, static_cast<long long>(a.length));
    for (std::int64_t i = 0; i < a.n_children; ++i) std::printf(" %lld", static_cast<long long>(a.children[i]->null_count));
    std::printf(" nulls\n");
    a.release(&a);
  }
  return 0;
}
