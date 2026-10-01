// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// bench_read — time a full read (open + every row group of the projected columns into Arrow
// arrays) of a Parquet file, best of N. Single-threaded. bench/compare_pyarrow.py runs this next to
// pyarrow.parquet.read_table on the same files.
//   p2n_bench file.parquet [reps=5] [column ...]
#include <parquet2nanoarrow/parquet2nanoarrow.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: p2n_bench file.parquet [reps] [column ...]\n");
    return 2;
  }
  const int reps = argc > 2 ? std::atoi(argv[2]) : 5;
  p2n::read_options opt;
  for (int i = 3; i < argc; ++i) opt.columns.emplace_back(argv[i]);
  opt.skip_unsupported = true;
  if (const char* v = std::getenv("P2N_VALIDATE_UTF8")) opt.validate_utf8 = v[0] != '0';
  double best = 1e30;
  std::int64_t rows = 0;
  for (int rep = 0; rep < reps; ++rep) {
    const auto t0 = std::chrono::steady_clock::now();
    auto r = p2n::reader::open(argv[1], opt);
    if (!r) {
      std::fprintf(stderr, "p2n_bench: %s\n", r.error().message.c_str());
      return 1;
    }
    rows = 0;
    for (std::size_t g = 0; g < r->num_row_groups(); ++g) {
      ArrowArray a;
      auto st = r->read_row_group(g, &a);
      if (!st) {
        std::fprintf(stderr, "p2n_bench: %s\n", st.error().message.c_str());
        return 1;
      }
      rows += a.length;
      a.release(&a);
    }
    const auto t1 = std::chrono::steady_clock::now();
    best = std::min(best, std::chrono::duration<double, std::milli>(t1 - t0).count());
  }
  std::printf("{\"ms\": %.3f, \"rows\": %lld}\n", best, static_cast<long long>(rows));
  return 0;
}
