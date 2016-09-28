/*
    Write Once Read Many database table backend
    Copyright (C) 2013    Morten Hustveit
    Copyright (C) 2013    eVenture Capital Partners II

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "src/table-backend-writeonce.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

#include <err.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "src/util.h"

#define MAGIC UINT64_C(0x6c6261742e692e70)
#define MAJOR_VERSION 3
#define MINOR_VERSION 0

#define TMP_SUFFIX ".tmp.XXXXXX"
#define BUFFER_SIZE (1024 * 1024)

namespace cantera {
namespace table {

using namespace internal;

namespace {

enum CA_wo_flags {
  CA_WO_FLAG_ASCENDING = 0x0001,
  CA_WO_FLAG_DESCENDING = 0x0002
};

struct CA_wo_header {
  uint64_t magic;
  uint8_t major_version;
  uint8_t minor_version;
  uint16_t flags;
  uint32_t data_reserved;
  uint64_t index_offset;
};

class WriteOnceTable : public SeekableTable {
 public:
  WriteOnceTable(const char* path, int flags, mode_t mode);

  ~WriteOnceTable();

  void Sync() override;

  void SetFlag(enum ca_table_flag flag) override;

  int IsSorted() override;

  void InsertRow(const struct iovec* value, size_t value_count) override;

  void Seek(off_t offset, int whence) override;

  void SeekToFirst() override { Seek(0, SEEK_SET); }

  bool SeekToKey(const string_view& key) override;

  off_t Offset() override;

  bool ReadRow(struct iovec* key, struct iovec* value) override;

 private:
  friend class WriteOnceTableBackend;

  void FlushKeyBuffer();

  void MAdviseIndex();

  void BuildIndex();

  void MemoryMap();

  std::string path;
  char* tmp_path = nullptr;

  int fd = -1;
  int open_flags = 0;

  FILE* write_buffer = nullptr;
  uint64_t write_offset = 0;

  void* buffer = nullptr;
  size_t buffer_size = 0, buffer_fill = 0;

  uint32_t reserved = 0;

  struct CA_wo_header* header = nullptr;

  uint64_t entry_count = 0;

  union {
    uint64_t* u64;
    uint32_t* u32;
    uint16_t* u16;
  } index;
  uint64_t index_size = 0;
  unsigned int index_bits = 0;

  bool has_madvised_index = false;

  int no_relative = 0;
  int no_fsync = 0;

  // Used for read, seek, offset and delete.
  uint64_t offset_ = 0;

  // Unflushed hash map keys.
  std::vector<std::pair<uint64_t, uint64_t>> key_buffer;
};

uint64_t CA_wo_hash(const string_view& str) {
  auto result = UINT64_C(0x2257d6803a6f1b2);

  for (auto ch : str) result = result * 31 + static_cast<unsigned char>(ch);

  return result;
}

WriteOnceTable::WriteOnceTable(const char* path, int flags, mode_t mode)
    : path(path) {
  if ((flags & O_WRONLY) == O_WRONLY) {
    flags &= ~O_WRONLY;
    flags |= O_RDWR;
  }

  fd = -1;
  buffer = MAP_FAILED;
  open_flags = flags;

  if (flags & O_CREAT) {
    struct CA_wo_header dummy_header;
    mode_t mask;

    mask = umask(0);
    umask(mask);

    KJ_REQUIRE(flags & O_TRUNC, "O_CREAT requires O_TRUNC");
    KJ_REQUIRE(flags & O_RDWR, "O_CREAT requires O_RDWR");

    KJ_SYSCALL(asprintf(&tmp_path, "%s.tmp.%u.XXXXXX", path, getpid()));

    KJ_SYSCALL(fd = mkstemp(tmp_path), tmp_path);

    KJ_SYSCALL(fchmod(fd, mode & ~mask), tmp_path);

    write_buffer = fdopen(fd, "w");
    if (!write_buffer) KJ_FAIL_SYSCALL("fdopen", errno, tmp_path);

    memset(&dummy_header, 0, sizeof(dummy_header));

    if (0 == fwrite(&dummy_header, sizeof(dummy_header), 1, write_buffer))
      KJ_FAIL_SYSCALL("fwrite", errno);

    write_offset = sizeof(dummy_header);
  } else {
    KJ_SYSCALL((flags & O_RDWR) != O_RDWR, "O_RDWR only allowed with O_CREAT");

    KJ_SYSCALL(fd = open(path, O_RDONLY | O_CLOEXEC));

    MemoryMap();
  }
}

}  // namespace

std::unique_ptr<Table> WriteOnceTableBackend::Open(const char* path, int flags,
                                                   mode_t mode) {
  return std::make_unique<WriteOnceTable>(path, flags, mode);
}

std::unique_ptr<SeekableTable> WriteOnceTableBackend::OpenSeekable(
    const char* path, int flags, mode_t mode) {
  return std::make_unique<WriteOnceTable>(path, flags, mode);
}

WriteOnceTable::~WriteOnceTable() {
  if (fd != -1) close(fd);

  if (write_buffer) fclose(write_buffer);

  if (buffer != MAP_FAILED) munmap(buffer, buffer_size);

  if (tmp_path) {
    unlink(tmp_path);
    free(tmp_path);
  }
}

void WriteOnceTable::FlushKeyBuffer() {
  MAdviseIndex();

  std::sort(key_buffer.begin(), key_buffer.end());

  for (auto& hash_offset : key_buffer) {
    uint64_t hash = hash_offset.first;

    while (index.u64[hash]) {
      if (++hash == index_size) hash = 0;
    }

    index.u64[hash] = hash_offset.second;
  }

  key_buffer.clear();
}

void WriteOnceTable::MAdviseIndex() {
  auto base = reinterpret_cast<ptrdiff_t>(buffer) + header->index_offset;
  auto end = reinterpret_cast<ptrdiff_t>(buffer) + buffer_fill;
  base &= ~0xfff;

  KJ_SYSCALL(madvise(reinterpret_cast<void*>(base), end - base, MADV_WILLNEED),
             base, end, (ptrdiff_t)buffer, buffer_size);
  has_madvised_index = true;
}

void WriteOnceTable::BuildIndex() {
  static const uint64_t kKeyBufferMax = 16 * 1024 * 1024;
  struct iovec key_iov, value;
  std::string prev_key, key;
  unsigned int flags = CA_WO_FLAG_ASCENDING | CA_WO_FLAG_DESCENDING;

  Seek(0, SEEK_SET);

  key_buffer.reserve(std::min(entry_count, kKeyBufferMax));

  KJ_SYSCALL(madvise(buffer, header->index_offset, MADV_SEQUENTIAL));

  for (;;) {
    uint64_t hash;

    int cmp;

    auto tmp_offset = offset_;

    if (!ReadRow(&key_iov, &value)) break;

    key.assign(reinterpret_cast<const char*>(key_iov.iov_base),
               key_iov.iov_len);

    if (flags && !prev_key.empty()) {
      cmp = prev_key.compare(key);

      if (cmp < 0) {
        flags &= CA_WO_FLAG_ASCENDING;
      } else if (cmp > 0) {
        flags &= CA_WO_FLAG_DESCENDING;
      }
    }

    if (header->major_version < 2) {
      hash = CA_wo_hash(key);
    } else {
      hash = Hash(key);
    }

    hash %= index_size;

    prev_key.swap(key);

    key_buffer.emplace_back(hash, tmp_offset);

    if (key_buffer.size() >= kKeyBufferMax) {
      // Discard the keys we've already read.
      if (0 != (tmp_offset & ~0xfff)) {
        KJ_SYSCALL(madvise(buffer, tmp_offset & ~0xfff, MADV_DONTNEED));
      }

      FlushKeyBuffer();
    }
  }

  FlushKeyBuffer();

  header->flags = flags;
}

void WriteOnceTable::Sync() {
  struct CA_wo_header header;

  if (!tmp_path) return;

  memset(&header, 0, sizeof(header));
  header.magic = MAGIC;  // Will implicitly store endianness
  header.major_version = MAJOR_VERSION;
  header.minor_version = MINOR_VERSION;
  header.index_offset = (write_offset + 0xfff) & ~0xfffULL;

  if (0 != fflush(write_buffer)) KJ_FAIL_SYSCALL("fflush", errno);

  header.data_reserved = reserved;

  KJ_SYSCALL(lseek(fd, 0, SEEK_SET), tmp_path);

  kj::FdOutputStream output(fd);
  output.write(&header, sizeof(header));

  index_bits = 64;
  index_size = entry_count * 2 + 1;

#if HAVE_FALLOCATE
  KJ_SYSCALL(
      fallocate(fd, 0, header.index_offset, index_size * sizeof(uint64_t)));
#else
  KJ_SYSCALL(
      ftruncate(fd, header.index_offset + index_size * sizeof(uint64_t)));
#endif

  MemoryMap();

  BuildIndex();

  if (!no_fsync) {
    // TODO(mortehu): fsync all ancestor directories too.
    KJ_SYSCALL(fsync(fd), tmp_path);
  }

  KJ_SYSCALL(rename(tmp_path, path.c_str()), tmp_path, path);

  free(tmp_path);
  tmp_path = nullptr;

  fclose(write_buffer);
  write_buffer = nullptr;
  fd = -1;
}

void WriteOnceTable::SetFlag(enum ca_table_flag flag) {
  switch (flag) {
    case CA_TABLE_NO_RELATIVE:
      no_relative = 1;
      break;

    case CA_TABLE_NO_FSYNC:
      no_fsync = 1;
      break;

    default:
      KJ_FAIL_REQUIRE("unknown flag", flag);
  }
}

int WriteOnceTable::IsSorted() {
  return 0 != (header->flags & CA_WO_FLAG_ASCENDING);
}

void WriteOnceTable::InsertRow(const struct iovec* value, size_t value_count) {
  KJ_REQUIRE(value_count == 2);

  uint8_t size_buf[10], *p = size_buf;

  uint64_t size = value[0].iov_len + value[1].iov_len + 1;
  ca_format_integer(&p, size);

  ++entry_count;

  const char nul_byte = 0;

  if (0 == fwrite(size_buf, p - size_buf, 1, write_buffer) ||
      0 == fwrite(value[0].iov_base, value[0].iov_len, 1, write_buffer) ||
      0 == fwrite(&nul_byte, 1, 1, write_buffer) ||
      0 == fwrite(value[1].iov_base, value[1].iov_len, 1, write_buffer)) {
    KJ_FAIL_SYSCALL("fwrite", errno);
  }

  write_offset += p - size_buf;
  write_offset += value[0].iov_len;
  write_offset += 1;
  write_offset += value[1].iov_len;
}

void WriteOnceTable::Seek(off_t rel_offset, int whence) {
  uint64_t new_offset;

  switch (whence) {
    case SEEK_SET:
      KJ_REQUIRE(rel_offset >= 0);
      new_offset = sizeof(struct CA_wo_header) + rel_offset;
      break;

    case SEEK_CUR:
      new_offset = offset_ + rel_offset;
      break;

    case SEEK_END:
      KJ_REQUIRE(rel_offset <= 0);
      new_offset = header->index_offset - rel_offset;
      break;

    default:
      KJ_FAIL_REQUIRE(!"Invalid 'whence' value");
  }

  KJ_REQUIRE(new_offset >= sizeof(struct CA_wo_header),
             "attempt to seek before start of table");

  KJ_REQUIRE(new_offset <= header->index_offset,
             "attempt to seek past end of table");

  offset_ = new_offset;
}

bool WriteOnceTable::SeekToKey(const string_view& key) {
  if (!has_madvised_index) MAdviseIndex();

  uint64_t hash, tmp_offset;

  if (header->major_version < 2) {
    hash = CA_wo_hash(key);
  } else {
    hash = Hash(key);
  }

  uint64_t min_offset = 0;
  uint64_t max_offset = buffer_fill;

  hash %= index_size;

  uint64_t fib[2] = {2, 1};
  unsigned int collisions = 0;

  for (;;) {
    switch (index_bits) {
      case 16:
        tmp_offset = index.u16[hash];
        break;
      case 32:
        tmp_offset = index.u32[hash];
        break;
      default:
        tmp_offset = index.u64[hash];
        break;
    }

    if (!tmp_offset) return false;

    if (tmp_offset >= min_offset && tmp_offset <= max_offset) {
      auto data = reinterpret_cast<const char*>(buffer) + tmp_offset;

      while ((*data) & 0x80) ++data;
      ++data;

      auto cmp = key.compare(data);

      if (cmp == 0) {
        offset_ = tmp_offset;
        return true;
      } else if (cmp < 0) {
        if (0 != (header->flags & CA_WO_FLAG_ASCENDING))
          max_offset = tmp_offset;
      } else {
        if (0 != (header->flags & CA_WO_FLAG_ASCENDING))
          min_offset = tmp_offset;
      }
    }

    if (header->major_version >= 3) {
      if (++hash == index_size) hash = 0;
    } else {
      ++collisions;
      hash = (hash + fib[collisions & 1]) % index_size;
      fib[collisions & 1] += fib[~collisions & 1];
    }
  }

  return false;
}

off_t WriteOnceTable::Offset() { return offset_ - sizeof(struct CA_wo_header); }

bool WriteOnceTable::ReadRow(struct iovec* key, struct iovec* value) {
  uint64_t size;
  uint8_t* p;

  KJ_REQUIRE(offset_ >= sizeof(struct CA_wo_header));

  p = reinterpret_cast<uint8_t*>(buffer) + offset_;

  if (offset_ >= header->index_offset || *p == 0) return false;

  size = ca_parse_integer((const uint8_t**)&p);

  key->iov_base = p;
  key->iov_len = strlen(reinterpret_cast<const char*>(key->iov_base));

  KJ_ASSERT(size > key->iov_len, size, key->iov_len);

  value->iov_base = p + key->iov_len + 1;
  value->iov_len = size - key->iov_len - 1;

  p += size;
  offset_ = p - reinterpret_cast<uint8_t*>(buffer);

  return true;
}

/*****************************************************************************/

void WriteOnceTable::MemoryMap() {
  off_t end = 0;
  int prot = PROT_READ;

  KJ_SYSCALL(end = lseek(fd, 0, SEEK_END), path);

  KJ_REQUIRE(static_cast<size_t>(end) > sizeof(struct CA_wo_header), end,
             sizeof(struct CA_wo_header));

  buffer_size = end;
  buffer_fill = end;

  if ((open_flags & O_RDWR) == O_RDWR) prot |= PROT_WRITE;

  if (MAP_FAILED == (buffer = mmap(NULL, end, prot, MAP_SHARED, fd, 0))) {
    KJ_FAIL_SYSCALL("mmap", errno, path);
  }

  header = (struct CA_wo_header*)buffer;

  KJ_REQUIRE(header->major_version == 3 || header->major_version == 2);

  KJ_REQUIRE(header->magic == MAGIC, header->magic, MAGIC);

  if (header->major_version >= 3)
    index_bits = 64;
  else if (!(header->index_offset & ~0xffffULL))
    index_bits = 16;
  else if (!(header->index_offset & ~0xffffffffULL))
    index_bits = 32;
  else
    index_bits = 64;

  index.u64 = reinterpret_cast<uint64_t*>(
      (reinterpret_cast<char*>(buffer) + header->index_offset));
  index_size = (buffer_size - header->index_offset) / (index_bits / CHAR_BIT);

  offset_ = sizeof(struct CA_wo_header);
}

}  // namespace table
}  // namespace cantera
