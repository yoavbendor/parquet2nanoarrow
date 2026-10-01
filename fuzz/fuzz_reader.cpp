// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// fuzz_reader — the whole reader on corrupted files: footer, schema, page headers, levels, values,
// dictionaries and compressed streams. Any input must yield arrays or an error; never a crash, an
// out-of-bounds access, undefined behaviour or an allocation beyond the configured caps (run under
// ASan/UBSan). Successful reads are also validated by nanoarrow (ArrowArrayFinishBuildingDefault).
//
// libFuzzer:  clang++ -fsanitize=fuzzer,address,undefined ... fuzz/fuzz_reader.cpp
// standalone: -DP2N_FUZZ_STANDALONE: ./p2n_fuzz iterations seed file.parquet... — mutates the seed
//             files (bit flips, byte overwrites, truncation, splices; biased toward the footer)
#include <parquet2nanoarrow/parquet2nanoarrow.hpp>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <vector>

namespace {
std::uint64_t g_ok = 0, g_rejected = 0;
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  p2n::read_options opt;
  opt.skip_unsupported = true;
  opt.max_page_bytes = std::size_t(1) << 22;   // keep allocations small under the fuzzer
  opt.max_row_group_rows = std::int64_t(1) << 18;
  auto r = p2n::reader::open_buffer(std::span<const std::byte>(reinterpret_cast<const std::byte*>(data), size), opt);
  if (!r) { ++g_rejected; return 0; }
  ArrowSchema s;
  if (r->schema(&s)) s.release(&s);
  for (std::size_t g = 0; g < r->num_row_groups(); ++g) {
    ArrowArray a;
    auto st = r->read_row_group(g, &a);
    if (!st) { ++g_rejected; continue; }
    ++g_ok;
    a.release(&a);
  }
  return 0;
}

#ifdef P2N_FUZZ_STANDALONE
int main(int argc, char** argv) {
  if (argc < 4) {
    std::fprintf(stderr, "usage: p2n_fuzz iterations seed file.parquet...\n");
    return 2;
  }
  const long iters = std::atol(argv[1]);
  std::uint64_t s = std::strtoull(argv[2], nullptr, 10) | 1;
  auto next = [&] { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; };
  std::vector<std::vector<std::uint8_t>> seeds;
  for (int i = 3; i < argc; ++i) {
    std::ifstream f(argv[i], std::ios::binary);
    seeds.emplace_back((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    LLVMFuzzerTestOneInput(seeds.back().data(), seeds.back().size());  // seeds themselves must read
  }
  const std::uint64_t seed_ok = g_ok;
  std::vector<std::uint8_t> buf;
  for (long i = 0; i < iters; ++i) {
    buf = seeds[next() % seeds.size()];
    if (buf.empty()) continue;
    const int edits = 1 + int(next() % 6);
    for (int e = 0; e < edits; ++e) {
      // half the edits land in the last 2 KiB (footer + last pages): the densest metadata
      const std::size_t tail = std::min<std::size_t>(buf.size(), 2048);
      const std::size_t at = next() % 2 ? buf.size() - 1 - next() % tail : next() % buf.size();
      switch (next() % 5) {
        case 0: buf[at] ^= std::uint8_t(1u << (next() % 8)); break;
        case 1: buf[at] = std::uint8_t(next()); break;
        case 2: buf[at] = std::uint8_t(next() % 2 ? 0xff : 0x00); break;
        case 3: if (buf.size() > 16) buf.erase(buf.begin() + std::ptrdiff_t(next() % (buf.size() - 8)), buf.end() - 8); break;  // cut the middle, keep the trailer
        default: {
          const std::size_t len = 1 + next() % 16, from = next() % buf.size();
          for (std::size_t k = 0; k < len && at + k < buf.size() && from + k < buf.size(); ++k) buf[at + k] = buf[from + k];
        }
      }
    }
    LLVMFuzzerTestOneInput(buf.data(), buf.size());
  }
  std::printf("p2n_fuzz: %ld mutated files OK (%llu seed row groups read; %llu row groups decoded, %llu rejected)\n",
              iters, static_cast<unsigned long long>(seed_ok), static_cast<unsigned long long>(g_ok - seed_ok),
              static_cast<unsigned long long>(g_rejected));
  return seed_ok == 0 ? 1 : 0;
}
#endif
