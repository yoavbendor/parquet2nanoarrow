// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Typed reading: the struct is the projection. Prints one line per row (tests/typed.py checks it
// against pyarrow), or with --sum only aggregates.
#include <parquet2nanoarrow/typed.hpp>

#include <cinttypes>
#include <cstdio>
#include <cstring>

struct trip {
  std::int64_t                    id;
  double                          fare;
  std::optional<std::int32_t>     passengers;
  std::optional<std::string_view> note;
  bool                            paid;
};
NANOM_DESCRIBE(trip, id, fare, passengers, note, paid);

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s file.parquet [--sum]\n", argv[0]);
    return 2;
  }
  const bool sum_only = argc > 2 && std::strcmp(argv[2], "--sum") == 0;
  auto r = p2n::typed_reader<trip>::open(argv[1]);
  if (!r) {
    std::printf("error: %s\n", r.error().message.c_str());
    return 1;
  }
  double fares = 0;
  std::int64_t n = 0;
  auto st = r->for_each([&](const trip& t) {
    fares += t.fare;
    ++n;
    if (sum_only) return;
    std::printf("%" PRId64 " %.17g %s %s %d\n", t.id, t.fare,
                t.passengers ? std::to_string(*t.passengers).c_str() : "null",
                t.note ? std::string(*t.note).c_str() : "null", int(t.paid));
  });
  if (!st) {
    std::printf("error: %s\n", st.error().message.c_str());
    return 1;
  }
  if (sum_only) std::printf("%" PRId64 " rows, fares %.17g\n", n, fares);
  return 0;
}
