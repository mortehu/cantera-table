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
#include <numeric>

#include <err.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include <kj/debug.h>

#include <zstd.h>

#include "src/ca-table.h"
#include "src/util.h"

#include "third_party/oroch/oroch/integer_codec.h"

#define MAGIC UINT64_C(0x6c6261742e692e70)
#define MAJOR_VERSION 4
#define MINOR_VERSION 0

#define TMP_SUFFIX ".tmp.XXXXXX"
#define BUFFER_SIZE (1024 * 1024)

namespace cantera {
namespace table {

using namespace internal;

namespace {

enum CA_wo_flags {
  // v3 flags
  CA_WO_FLAG_ASCENDING = 0x01,
  CA_WO_FLAG_DESCENDING = 0x02,

  // v4 flags
  CA_WO_FLAG_SEEKABLE = 0x01,
};

struct CA_wo_header {
  uint64_t magic;
  uint8_t major_version;
  uint8_t minor_version;
  uint8_t flags;
  uint8_t compression;
  uint32_t data_reserved;
  uint64_t index_offset;
};

/*****************************************************************************/

// If a block gets larger than this value then it is closed.
static constexpr size_t kBlockSizeMax = 32 * 1024;

// If an entry is larger than this limit then it is stored in a separate
// block unless the preceding block is too small.
static constexpr size_t kEntrySizeLimit = kBlockSizeMax - 4;

// If a block is larger than this value and the next entry is larger than
// the limit above then this block is closed and the next entry will be
// really stored in a separate block.
static constexpr size_t kBlockSizeMin = 12 * 1024;

/*****************************************************************************/

class DataBuffer {
 public:
  using uchar = unsigned char;

  DataBuffer(size_t capacity = 0) { reserve(capacity); }

  DataBuffer(const DataBuffer&) = delete;
  DataBuffer& operator=(const DataBuffer&) = delete;

  size_t capacity() const { return capacity_; }

  size_t size() const { return size_; }

  char* data() { return data_.get(); }
  const char* data() const { return data_.get(); }

  uchar* udata() { return reinterpret_cast<uchar*>(data()); }
  const uchar* udata() const { return reinterpret_cast<const uchar*>(data()); }

  void clear() { size_ = 0; }

  void reserve(size_t capacity) {
    if (capacity <= capacity_) return;

    std::unique_ptr<char[]> tmp(new char[capacity]);
    if (size_) std::copy_n(data_.get(), size_, tmp.get());
    data_.swap(tmp);

    capacity_ = capacity;
  }

  void resize(size_t size) {
    reserve(size);
    size_ = size;
  }

  template <typename T>
  void append(const std::vector<T>& vector) {
    size_t offset = size();
    size_t length = vector.size() * sizeof(T);
    resize(offset + length);
    memcpy(data() + offset, vector.data(), length);
  }

 private:
  size_t size_ = 0;
  size_t capacity_ = 0;
  std::unique_ptr<char[]> data_;
};

/*****************************************************************************/

class FileIO {
 public:
  FileIO(int fd) : fd_(fd) {}

  FileIO(const FileIO&) = delete;
  FileIO& operator=(const FileIO&) = delete;

  operator int() const { return fd_; }

  void Read(void* buffer, size_t length) {
    ssize_t n = read(fd_, buffer, length);
    if (n != length) {
      if (n < 0)
        KJ_FAIL_SYSCALL("read", errno);
      else
        KJ_FAIL_REQUIRE("read incomplete");
    }
  }

  void Read(void* buffer, off_t offset, size_t length) {
    ssize_t n = pread(fd_, buffer, length, offset);
    if (n != length) {
      if (n < 0)
        KJ_FAIL_SYSCALL("pread", errno);
      else
        KJ_FAIL_REQUIRE("pread incomplete");
    }
  }

  void Write(const void* buffer, size_t length) {
    ssize_t n = write(fd_, buffer, length);
    if (n != length) {
      if (n < 0)
        KJ_FAIL_SYSCALL("write", errno);
      else
        KJ_FAIL_REQUIRE("write incomplete");
    }
  }

  void Write(const void* buffer, off_t offset, size_t length) {
    ssize_t n = pwrite(fd_, buffer, length, offset);
    if (n != length) {
      if (n < 0)
        KJ_FAIL_SYSCALL("pwrite", errno);
      else
        KJ_FAIL_REQUIRE("pwrite incomplete");
    }
  }

  void Read(DataBuffer& buffer) { Read(buffer.data(), buffer.size()); }

  void Read(DataBuffer& buffer, off_t offset) {
    Read(buffer.data(), offset, buffer.size());
  }

  void Write(const DataBuffer& buffer) { Write(buffer.data(), buffer.size()); }

  void Write(const DataBuffer& buffer, off_t offset) {
    Write(buffer.data(), offset, buffer.size());
  }

  void Write(const string_view& buffer) { Write(buffer.data(), buffer.size()); }

  void Write(const string_view& buffer, off_t offset) {
    Write(buffer.data(), offset, buffer.size());
  }

 private:
  // File descriptor.
  int fd_;
};

/*****************************************************************************/

class ZstdCompressor {
 public:
  void Go(DataBuffer& dst, const DataBuffer& src, int level) {
    size_t size = ZSTD_compressCCtx(context_.get(), dst.data(), dst.capacity(),
                                    src.data(), src.size(), level);
    if (ZSTD_isError(size))
      KJ_FAIL_REQUIRE("compression error", ZSTD_getErrorName(size));
    dst.resize(size);
  }

 private:
  // Compression context.
  typedef std::unique_ptr<ZSTD_CCtx, decltype(ZSTD_freeCCtx)*> ContextPtr;
  ContextPtr context_ = ContextPtr(CreateContext(), ZSTD_freeCCtx);

  static ZSTD_CCtx* CreateContext() {
    ZSTD_CCtx* ctx = ZSTD_createCCtx();
    if (ctx == NULL) KJ_FAIL_REQUIRE("out of memory");
    return ctx;
  }
};

class ZstdDecompressor {
 public:
  void Go(DataBuffer& dst, const DataBuffer& src) {
    size_t size = ZSTD_decompressDCtx(context_.get(), dst.data(),
                                      dst.capacity(), src.data(), src.size());
    if (ZSTD_isError(size))
      KJ_FAIL_REQUIRE("decompression error", ZSTD_getErrorName(size));
    dst.resize(size);
  }

 private:
  // Decompression context.
  using ContextPtr = std::unique_ptr<ZSTD_DCtx, decltype(ZSTD_freeDCtx)*>;
  ContextPtr context_ = ContextPtr(CreateContext(), ZSTD_freeDCtx);

  static ZSTD_DCtx* CreateContext() {
    ZSTD_DCtx* ctx = ZSTD_createDCtx();
    if (ctx == NULL) KJ_FAIL_REQUIRE("out of memory");
    return ctx;
  }
};

/*****************************************************************************/

class WriteOnceBlock {
 public:
  size_t num_entries() const { return key_size_.size(); }

  size_t EstimateSize() const {
    size_t size = key_data_.size() + value_data_.size();
    size += oroch::varint_codec<size_t>::value_space(num_entries());
    size += oroch::varint_codec<uint32_t>::space(key_size_.begin(),
                                                 key_size_.end());
    size += oroch::varint_codec<uint32_t>::space(value_size_.begin(),
                                                 value_size_.end());
    return size;
  }

  const string_view GetFirstKey() const {
    KJ_REQUIRE(num_entries());
    const size_t size = key_size_.front();
    const char* data = key_data_.data();
    return string_view(data, size);
  }

  const string_view GetLaskKey() const {
    KJ_REQUIRE(num_entries());
    const size_t size = key_size_.back();
    const char* data = key_data_.data() + key_data_.size() - size;
    return string_view(data, size);
  }

  void Add(const string_view& key, const string_view& value) {
    key_size_.push_back(key.size());
    key_data_.insert(key_data_.end(), key.begin(), key.end());
    value_size_.push_back(value.size());
    value_data_.insert(value_data_.end(), value.begin(), value.end());
  }

  void Clear() {
    key_size_.clear();
    key_data_.clear();
    value_size_.clear();
    value_data_.clear();
  }

  void Marshal(DataBuffer& buffer) const {
    buffer.clear();
    const size_t num = num_entries();
    if (!num) return;

    buffer.reserve(EstimateSize());
    unsigned char* ptr = buffer.udata();
    oroch::varint_codec<size_t>::value_encode(ptr, num);
    oroch::varint_codec<uint32_t>::encode(ptr, key_size_.begin(),
                                          key_size_.end());
    oroch::varint_codec<uint32_t>::encode(ptr, value_size_.begin(),
                                          value_size_.end());
    buffer.resize(ptr - buffer.udata());
    buffer.append(key_data_);
    buffer.append(value_data_);
  }

  void Unmarshal(DataBuffer& buffer) { Clear(); }

 private:
  // Accumulated key data.
  std::vector<uint32_t> key_size_;
  std::vector<char> key_data_;

  // Accumulated value data.
  std::vector<uint32_t> value_size_;
  std::vector<char> value_data_;
};

/*****************************************************************************/

class WriteOnceIndex {
 public:
  void Clear() {}

  size_t num_blocks() const { return key_size_.size(); }

  size_t EstimateSize() const {
    size_t size = key_data_.size();
    size += oroch::varint_codec<size_t>::value_space(num_blocks());
    size += oroch::varint_codec<size_t>::space(block_size_.begin(),
                                               block_size_.end());
    size += oroch::varint_codec<uint32_t>::space(key_size_.begin(),
                                                 key_size_.end());
    return size;
  }

  uint64_t GetIndexOffset() const {
    uint64_t offset = sizeof(struct CA_wo_header);
    return std::accumulate(block_size_.begin(), block_size_.end(), offset);
  }

  void Add(uint32_t block_size, const string_view& last_key) {
    block_size_.push_back(block_size);
    key_size_.push_back(last_key.size());
    key_data_.insert(key_data_.end(), last_key.begin(), last_key.end());
  }

  void Marshal(DataBuffer& buffer) const {
    buffer.clear();
    const size_t num = num_blocks();
    if (!num) return;

    buffer.reserve(EstimateSize());
    unsigned char* ptr = buffer.udata();
    oroch::varint_codec<size_t>::value_encode(ptr, num);
    oroch::varint_codec<size_t>::encode(ptr, block_size_.begin(),
                                        block_size_.end());
    oroch::varint_codec<uint32_t>::encode(ptr, key_size_.begin(),
                                          key_size_.end());
    buffer.resize(ptr - buffer.udata());
    buffer.append(key_data_);
  }

  void Unmarshal(DataBuffer& buffer) { Clear(); }

 private:
  // Block sizes.
  std::vector<size_t> block_size_;

  // Last key in a block data.
  std::vector<uint32_t> key_size_;
  std::vector<char> key_data_;
};

/*****************************************************************************/

class WriteOnceBuilder {
 public:
  WriteOnceBuilder(const char* path, const TableOptions& options)
      : path_(path), options_(options) {
    KJ_REQUIRE((options.GetFileFlags() & ~(O_EXCL | O_CLOEXEC)) == 0);

    compression_ = options.GetCompression();
    if (compression_ == kTableCompressionDefault)
      compression_ = kTableCompressionNone;
    KJ_REQUIRE(compression_ <= kTableCompressionLast,
               "unsupported compression method");

    compression_level_ = options.GetCompressionLevel();
    if (compression_level_ == 0 && compression_ != kTableCompressionNone)
      compression_level_ = 3;

    std::string dir(".");
    if (const char* last_slash = strrchr(path, '/')) {
      KJ_REQUIRE(path != last_slash);
      dir = std::string(path, last_slash - 1);
    }

    raw_fd_ = AnonTemporaryFile(dir.c_str());

    raw_stream_ = fdopen(raw_fd_.get(), "w");
    if (!raw_stream_) KJ_FAIL_SYSCALL("fdopen", errno, path);

    index_.reserve(12 * 1024 * 1024);
  }

  ~WriteOnceBuilder() {
    raw_fd_ = nullptr;
    if (raw_stream_) fclose(raw_stream_);
  }

  const std::string& path() const { return path_; }

  uint64_t GetIndexOffset() const { return index_offset_; }

  void NoFSync(bool value = true) { no_fsync_ = value; }

  void Add(const string_view& key, const string_view& value) {
    AddEntry(key, value);
    WriteEntryData(key, value);
  }

  void Build() {
    FlushEntryData();
    SortEntries();

    CreateFile();
    try {
      WriteFinalData();
      CommitFile();
    } catch (...) {
      RemoveFile();
      throw;
    }
  }

 private:
  struct Entry {
    uint64_t offset;
    uint32_t value_size;
    uint32_t key_size;
    char prefix[24];
  };

  void AddEntry(const string_view& key, const string_view& value) {
    KJ_REQUIRE(key.size() <= std::numeric_limits<uint32_t>::max(),
               "too long key");
    KJ_REQUIRE(value.size() <= std::numeric_limits<uint32_t>::max(),
               "too long value");

    struct Entry entry;
    size_t count = std::min(sizeof entry.prefix, key.size());
    entry.offset = offset_;
    entry.key_size = key.size();
    entry.value_size = value.size();
    std::copy_n(key.begin(), count, entry.prefix);
    index_.push_back(entry);

    offset_ += key.size() + value.size();
    if (key_size_max_ < key.size()) key_size_max_ = key.size();
  }

  void WriteEntryData(const string_view& key, const string_view& value) {
    KJ_REQUIRE(raw_stream_ != nullptr);

    if (0 == fwrite(key.data(), key.size(), 1, raw_stream_) ||
        0 == fwrite(value.data(), value.size(), 1, raw_stream_))
      KJ_FAIL_SYSCALL("fwrite", errno);
  }

  void FlushEntryData() {
    KJ_REQUIRE(raw_stream_ != nullptr);
    int ret = fflush(raw_stream_);
    if (ret) KJ_FAIL_SYSCALL("fflush", errno);
  }

  bool Compare(const Entry& lhs, const Entry& rhs) {
    size_t lhs_count = std::min(sizeof lhs.prefix, size_t{lhs.key_size});
    size_t rhs_count = std::min(sizeof rhs.prefix, size_t{rhs.key_size});
    const string_view lhs_prefix(lhs.prefix, lhs_count);
    const string_view rhs_prefix(rhs.prefix, rhs_count);

    int fast_result = lhs_prefix.compare(rhs_prefix);
    if (fast_result) return fast_result < 0;

    FileIO fd(raw_fd_.get());
    fd.Read(lhs_buffer_.get(), lhs.offset, lhs.key_size);
    fd.Read(rhs_buffer_.get(), rhs.offset, rhs.key_size);
    read_count_ += 2;

    string_view lhs_view(lhs_buffer_.get(), lhs.key_size);
    string_view rhs_view(rhs_buffer_.get(), rhs.key_size);
    return lhs_view < rhs_view;
  }

  void SortEntries() {
    lhs_buffer_ = std::make_unique<char[]>(key_size_max_);
    rhs_buffer_ = std::make_unique<char[]>(key_size_max_);

    // Use stable_sort here. It is usually implemented using merge sort
    // algorithm that is preferred over quick sort when disk access is
    // involved.
    std::stable_sort(index_.begin(), index_.end(),
                     [this](const auto& lhs, const auto& rhs) {
                       return this->Compare(lhs, rhs);
                     });

    KJ_LOG(DBG, index_.size(), read_count_);

    lhs_buffer_.reset();
    rhs_buffer_.reset();
  }

  void CreateFile() {
    char* temp = NULL;
    KJ_SYSCALL(asprintf(&temp, "%s.tmp.%u.XXXXXX", path_.c_str(), getpid()));
    std::unique_ptr<char[], decltype(free)*> temp_name_free(temp, free);

    int fd;
    KJ_SYSCALL(fd = mkstemp(temp), temp);
    fd_ = kj::AutoCloseFd(fd);
    tmp_path_ = std::string(temp);

    int nflags = options_.GetFileFlags();
    nflags &= O_CLOEXEC;

    int oflags;
    KJ_SYSCALL(oflags = fcntl(fd, F_GETFD));
    if ((oflags & nflags) != nflags)
      KJ_SYSCALL(fcntl(fd, F_SETFD, oflags | nflags));

    WriteHeader(0);
  }

  void CommitFile() {
    auto mode = options_.GetFileMode();
    auto mask = umask(0);
    umask(mask);

    KJ_SYSCALL(fchmod(fd_.get(), mode & ~mask), tmp_path_);
    KJ_SYSCALL(rename(tmp_path_.c_str(), path_.c_str()), tmp_path_, path_);

    if (!no_fsync_) {
      // TODO(mortehu): fsync all ancestor directories too.
      KJ_SYSCALL(fsync(fd_.get()), path_);
    }
  }

  void RemoveFile() { unlink(tmp_path_.c_str()); }

  void WriteHeader(uint64_t index_offset) {
    struct CA_wo_header header;
    header.magic = MAGIC;  // Will implicitly store endianness
    header.major_version = MAJOR_VERSION;
    header.minor_version = MINOR_VERSION;
    header.flags = 0;
    header.compression = compression_;
    header.data_reserved = 0;
    header.index_offset = index_offset;

    FileIO fd(fd_);
    KJ_SYSCALL(lseek(fd, 0, SEEK_SET), tmp_path_);
    fd.Write(&header, sizeof(header));
  }

  void WriteFinalData() {
    WriteOnceIndex index;
    WriteOnceBlock block;

    DataBuffer buffer;
    for (const Entry& entry : index_) {
      const size_t size = entry.key_size + entry.value_size;
      if (size > kEntrySizeLimit && block.EstimateSize() > kBlockSizeMin) {
        WriteBlock(block, index);
        block.Clear();
      }

      buffer.resize(size);
      FileIO(raw_fd_).Read(buffer, entry.offset);

      const char* data = buffer.data();
      const string_view key(data, entry.key_size);
      const string_view value(data + entry.key_size, entry.value_size);
      block.Add(key, value);

      if (block.EstimateSize() > kBlockSizeMax) {
        WriteBlock(block, index);
        block.Clear();
      }
    }

    WriteBlock(block, index);
    WriteIndex(index);
  }

  void WriteBlock(const WriteOnceBlock& block, WriteOnceIndex& index) {
    block.Marshal(marshal_buffer_);
    if (!marshal_buffer_.size()) return;

    DataBuffer& buffer = CompressMarshalBuffer();
    FileIO(fd_).Write(buffer);

    index.Add(buffer.size(), block.GetLaskKey());

    // KJ_LOG(DBG, block.num_entries(), buffer.size());
  }

  void WriteIndex(const WriteOnceIndex& index) {
    index.Marshal(marshal_buffer_);
    if (!marshal_buffer_.size()) return;

    DataBuffer& buffer = CompressMarshalBuffer();
    FileIO(fd_).Write(buffer);

    index_offset_ = index.GetIndexOffset();
    WriteHeader(index_offset_);

    KJ_LOG(DBG, index.num_blocks(), buffer.size());
  }

  DataBuffer& CompressMarshalBuffer() {
    if (compression_ == TableCompression::kTableCompressionNone)
      return marshal_buffer_;

    compress_buffer_.reserve(ZSTD_compressBound(marshal_buffer_.size()));
    compressor_.Go(compress_buffer_, marshal_buffer_, compression_level_);

    if (compress_buffer_.size() > marshal_buffer_.size())
      KJ_LOG(DBG, compress_buffer_.size() - marshal_buffer_.size());

    return compress_buffer_;
  }

  // Final file path.
  const std::string path_;
  // Temporary file name.
  std::string tmp_path_;

  // Saved table creation options.
  const TableOptions options_;
  TableCompression compression_ = TableCompression::kTableCompressionNone;
  int compression_level_ = 0;

  // Whether to fsync the data.
  bool no_fsync_ = false;

  // Result file.
  kj::AutoCloseFd fd_;

  // Temporary file for all added key/value pairs.
  kj::AutoCloseFd raw_fd_;
  FILE* raw_stream_ = nullptr;

  // Index of all added key/value pairs.
  std::vector<Entry> index_;

  // Running file offset.
  uint64_t offset_ = 0;

  uint64_t index_offset_ = 0;

  // Longest encountered key size.
  uint32_t key_size_max_ = 0;
  // Buffers for key comparison.
  std::unique_ptr<char[]> lhs_buffer_;
  std::unique_ptr<char[]> rhs_buffer_;

  // A buffer for block marshaling.
  DataBuffer marshal_buffer_;
  // A buffer for block compression.
  DataBuffer compress_buffer_;
  // Compression context.
  ZstdCompressor compressor_;

  // Read statistics.
  uint64_t read_count_ = 0;
};

/*****************************************************************************/

class WriteOnceReader {
 public:
  WriteOnceReader(const std::string& path, kj::AutoCloseFd&& fd,
                  uint64_t index_offset)
      : path_(path), fd_(std::move(fd)), index_offset_(index_offset) {}

  virtual ~WriteOnceReader() {}

  virtual int IsSorted() = 0;

  virtual bool SeekToKey(const string_view& key) = 0;

  virtual bool ReadRow(struct iovec* key, struct iovec* value) = 0;

  off_t Offset() const { return offset_ - sizeof(struct CA_wo_header); }

  void Seek(off_t rel_offset, int whence) {
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
        new_offset = index_offset_ - rel_offset;
        break;

      default:
        KJ_FAIL_REQUIRE(!"Invalid 'whence' value");
    }

    KJ_REQUIRE(new_offset >= sizeof(struct CA_wo_header),
               "attempt to seek before start of table");
    KJ_REQUIRE(new_offset <= index_offset_,
               "attempt to seek past end of table");

    offset_ = new_offset;
  }

 protected:
  const std::string path_;

  kj::AutoCloseFd fd_;

  uint64_t index_offset_;

  // Used for read, seek, offset and delete.
  uint64_t offset_ = 0;
};

/*****************************************************************************/

class WriteOnceReader_v4 : public WriteOnceReader {
 public:
  WriteOnceReader_v4(const std::string& path, kj::AutoCloseFd&& fd,
                     uint64_t index_offset)
      : WriteOnceReader(path, std::move(fd), index_offset) {}

  int IsSorted() override { return 1; }

  bool SeekToKey(const string_view& key) {
    KJ_UNIMPLEMENTED();
    return false;
  }

  bool ReadRow(struct iovec* key, struct iovec* value) {
    KJ_UNIMPLEMENTED();
    return false;
  }

 private:
};

/*****************************************************************************/

uint64_t CA_wo_hash(const string_view& str) {
  auto result = UINT64_C(0x2257d6803a6f1b2);

  for (auto ch : str) result = result * 31 + static_cast<unsigned char>(ch);

  return result;
}

class WriteOnceReader_v3 : public WriteOnceReader {
 public:
  WriteOnceReader_v3(const std::string& path, kj::AutoCloseFd&& fd,
                     uint64_t index_offset)
      : WriteOnceReader(path, std::move(fd), index_offset) {
    MemoryMap();
  }

  virtual ~WriteOnceReader_v3() {
    if (buffer_ != MAP_FAILED) munmap(buffer_, buffer_size_);
  }

  int IsSorted() override {
    return 0 != (header_->flags & CA_WO_FLAG_ASCENDING);
  }

  bool SeekToKey(const string_view& key) {
    if (!has_madvised_index_) MAdviseIndex();

    uint64_t hash, tmp_offset;

    if (header_->major_version < 2) {
      hash = CA_wo_hash(key);
    } else {
      hash = Hash(key);
    }

    uint64_t min_offset = 0;
    uint64_t max_offset = buffer_fill_;

    hash %= index_size_;

    uint64_t fib[2] = {2, 1};
    unsigned int collisions = 0;

    for (;;) {
      switch (index_bits_) {
        case 16:
          tmp_offset = index_.u16[hash];
          break;
        case 32:
          tmp_offset = index_.u32[hash];
          break;
        default:
          tmp_offset = index_.u64[hash];
          break;
      }

      if (!tmp_offset) return false;

      if (tmp_offset >= min_offset && tmp_offset <= max_offset) {
        auto data = reinterpret_cast<const char*>(buffer_) + tmp_offset;

        while ((*data) & 0x80) ++data;
        ++data;

        auto cmp = key.compare(data);

        if (cmp == 0) {
          offset_ = tmp_offset;
          return true;
        } else if (cmp < 0) {
          if (0 != (header_->flags & CA_WO_FLAG_ASCENDING))
            max_offset = tmp_offset;
        } else {
          if (0 != (header_->flags & CA_WO_FLAG_ASCENDING))
            min_offset = tmp_offset;
        }
      }

      if (header_->major_version == 3) {
        if (++hash == index_size_) hash = 0;
      } else {
        ++collisions;
        hash = (hash + fib[collisions & 1]) % index_size_;
        fib[collisions & 1] += fib[~collisions & 1];
      }
    }

    return false;
  }

  bool ReadRow(struct iovec* key, struct iovec* value) override {
    uint64_t size;
    uint8_t* p;

    KJ_REQUIRE(offset_ >= sizeof(struct CA_wo_header));

    p = reinterpret_cast<uint8_t*>(buffer_) + offset_;

    if (offset_ >= header_->index_offset || *p == 0) return false;

    size = ca_parse_integer((const uint8_t**)&p);

    key->iov_base = p;
    key->iov_len = strlen(reinterpret_cast<const char*>(key->iov_base));

    KJ_ASSERT(size > key->iov_len, size, key->iov_len);

    value->iov_base = p + key->iov_len + 1;
    value->iov_len = size - key->iov_len - 1;

    p += size;
    offset_ = p - reinterpret_cast<uint8_t*>(buffer_);

    return true;
  }

 private:
  void MemoryMap() {
    off_t end = 0;
    KJ_SYSCALL(end = lseek(fd_, 0, SEEK_END), path_);

    KJ_REQUIRE(static_cast<size_t>(end) > sizeof(struct CA_wo_header), end,
               sizeof(struct CA_wo_header));

    buffer_size_ = end;
    buffer_fill_ = end;

    if (MAP_FAILED ==
        (buffer_ = mmap(NULL, end, PROT_READ, MAP_SHARED, fd_, 0))) {
      KJ_FAIL_SYSCALL("mmap", errno, path_);
    }

    header_ = (struct CA_wo_header*)buffer_;

    if (header_->major_version >= 3)
      index_bits_ = 64;
    else if (!(header_->index_offset & ~0xffffULL))
      index_bits_ = 16;
    else if (!(header_->index_offset & ~0xffffffffULL))
      index_bits_ = 32;
    else
      index_bits_ = 64;

    index_.u64 = reinterpret_cast<uint64_t*>(
        (reinterpret_cast<char*>(buffer_) + header_->index_offset));
    index_size_ =
        (buffer_size_ - header_->index_offset) / (index_bits_ / CHAR_BIT);

    offset_ = sizeof(struct CA_wo_header);
  }

  void MAdviseIndex() {
    auto base = reinterpret_cast<ptrdiff_t>(buffer_) + header_->index_offset;
    auto end = reinterpret_cast<ptrdiff_t>(buffer_) + buffer_fill_;
    base &= ~0xfff;

    KJ_SYSCALL(
        madvise(reinterpret_cast<void*>(base), end - base, MADV_WILLNEED), base,
        end, (ptrdiff_t)buffer_, buffer_size_);
    has_madvised_index_ = true;
  }

  // Entire mmap()-ed file.
  void* buffer_ = MAP_FAILED;
  size_t buffer_size_ = 0, buffer_fill_ = 0;

  struct CA_wo_header* header_ = nullptr;

  union {
    uint64_t* u64;
    uint32_t* u32;
    uint16_t* u16;
  } index_;

  uint64_t index_size_ = 0;
  unsigned int index_bits_ = 0;

  bool has_madvised_index_ = false;
};

/*****************************************************************************/

class WriteOnceTable : public SeekableTable {
 public:
  WriteOnceTable(const char* path, const TableOptions& options) {
    builder_ = std::make_unique<WriteOnceBuilder>(path, options);
  }

  WriteOnceTable(const char* path) {
    kj::AutoCloseFd fd = OpenFile(path);

    struct CA_wo_header header;
    FileIO(fd).Read(&header, sizeof header);
    KJ_REQUIRE(header.magic == MAGIC, header.magic, MAGIC);
    KJ_REQUIRE(header.major_version <= MAJOR_VERSION ||
               header.major_version >= 2);
    KJ_REQUIRE(header.compression <= kTableCompressionLast,
               "unsupported compression method", header.compression);

    if (header.major_version <= 3)
      reader_ = std::make_unique<WriteOnceReader_v3>(path, std::move(fd),
                                                     header.index_offset);
    else
      reader_ = std::make_unique<WriteOnceReader_v4>(path, std::move(fd),
                                                     header.index_offset);
  }

  void Sync() override {
    if (builder_) {
      builder_->Build();
      std::string path = builder_->path();
      uint64_t index_offset = builder_->GetIndexOffset();
      builder_.reset();

      kj::AutoCloseFd fd = OpenFile(path.c_str());
      reader_ = std::make_unique<WriteOnceReader_v4>(path, std::move(fd),
                                                     index_offset);
    }
  }

  void SetFlag(enum ca_table_flag flag) override {
    switch (flag) {
      case CA_TABLE_NO_RELATIVE:
        // FIXME(ademakov): what is this?
        // no_relative = 1;
        break;

      case CA_TABLE_NO_FSYNC:
        if (builder_) builder_->NoFSync();
        break;

      default:
        KJ_FAIL_REQUIRE("unknown flag", flag);
    }
  }

  int IsSorted() override { return reader_ && reader_->IsSorted(); }

  void InsertRow(const struct iovec* value, size_t value_count) override {
    KJ_REQUIRE(value_count == 2);

    if (builder_) {
      const string_view k(reinterpret_cast<const char*>(value[0].iov_base),
                          value[0].iov_len);
      const string_view v(reinterpret_cast<const char*>(value[1].iov_base),
                          value[1].iov_len);
      builder_->Add(k, v);
    }
  }

  void SeekToFirst() override { Seek(0, SEEK_SET); }

  void Seek(off_t offset, int whence) override {
    if (reader_) reader_->Seek(offset, whence);
  }

  off_t Offset() override { return reader_ ? reader_->Offset() : 0; }

  bool SeekToKey(const string_view& key) override {
    return reader_ ? reader_->SeekToKey(key) : false;
  }

  bool ReadRow(struct iovec* key, struct iovec* value) override {
    return reader_ ? reader_->ReadRow(key, value) : false;
  }

 private:
  static kj::AutoCloseFd OpenFile(const char* path) {
    int fd;
    KJ_SYSCALL(fd = open(path, O_RDONLY | O_CLOEXEC));
    return kj::AutoCloseFd(fd);
  }

  // Table reader.
  std::unique_ptr<WriteOnceReader> reader_;
  // Table builder.
  std::unique_ptr<WriteOnceBuilder> builder_;
};

}  // namespace

/*****************************************************************************/

std::unique_ptr<Table> WriteOnceTableBackend::Create(
    const char* path, const TableOptions& options) {
  return std::make_unique<WriteOnceTable>(path, options);
}

std::unique_ptr<Table> WriteOnceTableBackend::Open(const char* path) {
  return std::make_unique<WriteOnceTable>(path);
}

std::unique_ptr<SeekableTable> WriteOnceTableBackend::OpenSeekable(
    const char* path) {
  return std::make_unique<WriteOnceTable>(path);
}

}  // namespace table
}  // namespace cantera
