// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Yoav Bendor
//
// Read-only memory mapping of the input file: the footer, page headers and uncompressed pages are
// decoded straight out of the mapping, with no read() copies.
#include "internal.hpp"

#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace p2n {

result<std::shared_ptr<mapped_file>> mapped_file::open(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return fail("cannot open " + path.string() + ": " + std::strerror(errno));
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    const int e = errno;
    ::close(fd);
    return fail("cannot stat " + path.string() + ": " + std::strerror(e));
  }
  std::shared_ptr<mapped_file> m(new mapped_file());
  m->size_ = std::size_t(st.st_size);
  if (m->size_ > 0) {
    void* p = ::mmap(nullptr, m->size_, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) {
      const int e = errno;
      ::close(fd);
      return fail("cannot map " + path.string() + ": " + std::strerror(e));
    }
    m->data_ = static_cast<const std::byte*>(p);
  }
  ::close(fd);  // the mapping stays valid without the descriptor
  return m;
}

mapped_file::~mapped_file() {
  if (data_) ::munmap(const_cast<std::byte*>(data_), size_);
}

}  // namespace p2n
