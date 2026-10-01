// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Output-buffer allocator: large Arrow buffers come from anonymous mappings and are kept in a
// bounded pool when the consumer releases them, so the next row group reuses memory that is
// already faulted in instead of paying a page fault per 4 KiB of fresh memory.
//
// Why: with glibc malloc, a released multi-megabyte buffer is unmapped or trimmed back to the OS,
// and the next row group's buffers fault in again: measured at ~4,700 faults per 1M-row row
// group of two 8-byte columns, against ~60 when the memory is reused. A library must not retune
// the host's malloc (mallopt / GLIBC_TUNABLES are process-wide), so the reader owns this instead.
//
// Every block carries a 64-byte header (capacity), so data is 64-byte aligned (Arrow's
// recommendation) and reallocation/free know the block size. The pool is thread-safe (consumers
// may release arrays on any thread) and bounded by set_buffer_pool_limit().
#include "internal.hpp"

#include <map>
#include <mutex>
#include <sys/mman.h>  // mmap / mremap (Linux)

namespace p2n {
namespace {

constexpr std::size_t kHeader = 64;
constexpr std::size_t kLarge = std::size_t(1) << 20;      // >= 1 MiB: mapped + pooled
constexpr std::size_t kHuge = std::size_t(2) << 20;       // mapping granularity
// No MADV_HUGEPAGE: with the common defrag=madvise setting the kernel compacts synchronously for
// such regions, which measured as 2-3x latency spikes on first reads; pooling alone is stable.

struct header {
  std::size_t capacity;  // usable bytes after the header
  std::size_t mapped;    // 0: aligned_alloc block; else the mapping's length
};

struct pool {
  std::mutex mu;
  std::multimap<std::size_t, std::byte*> free_blocks;  // capacity -> block (header address)
  std::size_t held = 0;
  std::size_t limit = std::size_t(256) << 20;
};
pool& the_pool() {
  static pool* p = new pool();  // intentionally leaked: buffers may be released during exit
  return *p;
}

header* hdr(std::uint8_t* data) { return reinterpret_cast<header*>(data - kHeader); }

#if defined(__SANITIZE_ADDRESS__)
constexpr bool kAsan = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
constexpr bool kAsan = true;
#else
constexpr bool kAsan = false;
#endif
#else
constexpr bool kAsan = false;
#endif

std::uint8_t* allocate(std::size_t size) {
  if constexpr (kAsan) {
    // Under AddressSanitizer every buffer is an exact-size malloc block: pooled mappings have no
    // redzones, and the sanitized test suites must catch any write past a buffer's capacity.
    void* p = std::malloc(kHeader + size);
    if (!p) return nullptr;
    auto* h = static_cast<header*>(p);
    h->capacity = size;
    h->mapped = 0;
    return static_cast<std::uint8_t*>(p) + kHeader;
  }
  if (size < kLarge) {
    void* p = std::aligned_alloc(kHeader, ((kHeader + size + kHeader - 1) / kHeader) * kHeader);
    if (!p) return nullptr;
    auto* h = static_cast<header*>(p);
    h->capacity = ((kHeader + size + kHeader - 1) / kHeader) * kHeader - kHeader;
    h->mapped = 0;
    return static_cast<std::uint8_t*>(p) + kHeader;
  }
  pool& P = the_pool();
  {
    std::lock_guard<std::mutex> lock(P.mu);
    // best fit: the smallest pooled block that is large enough. No upper bound on the waste: a
    // buffer that starts small and grows (string data, page by page) then grows inside memory
    // that is already faulted in, instead of faulting in fresh pages at every step. The waste is
    // temporary (the block returns to the pool) and bounded by the pool limit.
    auto it = P.free_blocks.lower_bound(size);
    if (it != P.free_blocks.end()) {
      std::byte* b = it->second;
      P.held -= reinterpret_cast<header*>(b)->mapped;
      P.free_blocks.erase(it);
      return reinterpret_cast<std::uint8_t*>(b) + kHeader;
    }
  }
  const std::size_t len = ((kHeader + size + kHuge - 1) / kHuge) * kHuge;
  void* p = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) return nullptr;
  auto* h = static_cast<header*>(p);
  h->capacity = len - kHeader;
  h->mapped = len;
  return static_cast<std::uint8_t*>(p) + kHeader;
}

void release(std::uint8_t* data) {
  if (!data) return;
  header* h = hdr(data);
  if (!h->mapped) {
    std::free(h);
    return;
  }
  pool& P = the_pool();
  {
    std::lock_guard<std::mutex> lock(P.mu);
    if (P.held + h->mapped <= P.limit) {
      P.held += h->mapped;
      P.free_blocks.emplace(h->capacity, reinterpret_cast<std::byte*>(h));
      return;
    }
  }
  ::munmap(h, h->mapped);
}

std::uint8_t* pool_reallocate(ArrowBufferAllocator*, std::uint8_t* ptr, std::int64_t old_size, std::int64_t new_size) {
  if (new_size < 0) return nullptr;
  if (ptr && std::size_t(new_size) <= hdr(ptr)->capacity) return ptr;  // fits: no move
  if (ptr && hdr(ptr)->mapped) {
    // growing a mapped block: let the kernel move the pages (mremap) instead of copying the
    // contents — growing string buffers page by page would otherwise copy them over and over
    header* h = hdr(ptr);
    const std::size_t len = ((kHeader + std::size_t(new_size) + kHuge - 1) / kHuge) * kHuge;
    void* p = ::mremap(h, h->mapped, len, MREMAP_MAYMOVE);
    if (p != MAP_FAILED) {
      auto* nh = static_cast<header*>(p);
      nh->capacity = len - kHeader;
      nh->mapped = len;
      return static_cast<std::uint8_t*>(p) + kHeader;
    }
  }
  std::uint8_t* n = allocate(std::size_t(new_size));
  if (!n) return nullptr;
  if (ptr) {
    if (old_size > 0) std::memcpy(n, ptr, std::size_t(std::min(old_size, new_size)));
    release(ptr);
  }
  return n;
}
void pool_free(ArrowBufferAllocator*, std::uint8_t* ptr, std::int64_t) { release(ptr); }

}  // namespace

void use_pool(ArrowBuffer* b) {
  if (b->data == nullptr) {
    ArrowBufferAllocator a{};
    a.reallocate = pool_reallocate;
    a.free = pool_free;
    a.private_data = nullptr;
    b->allocator = a;
  }
}

void set_buffer_pool_limit(std::size_t bytes) {
  pool& P = the_pool();
  std::vector<std::pair<void*, std::size_t>> drop;
  {
    std::lock_guard<std::mutex> lock(P.mu);
    P.limit = bytes;
    while (P.held > P.limit && !P.free_blocks.empty()) {
      auto it = std::prev(P.free_blocks.end());  // largest first
      auto* h = reinterpret_cast<header*>(it->second);
      P.held -= h->mapped;
      drop.emplace_back(h, h->mapped);
      P.free_blocks.erase(it);
    }
  }
  for (auto [p, n] : drop) ::munmap(p, n);
}

}  // namespace p2n
