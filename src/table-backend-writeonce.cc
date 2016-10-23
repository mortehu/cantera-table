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

#include "src/util.h"

#include "third_party/oroch/oroch/integer_codec.h"

#define MAGIC UINT64_C(0x6c6261742e692e70)
#define MAJOR_VERSION 4
#define MINOR_VERSION 0

namespace cantera {
namespace table {
namespace internal {

enum CA_wo_flags {
  // v3 flags
  CA_WO_FLAG_ASCENDING = 0x01,
  CA_WO_FLAG_DESCENDING = 0x02,

  // v4 flags
  CA_WO_FLAG_SEEKABLE = 0x01,
  CA_WO_FLAG_EXTENDED = 0x02,
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

  void append(const void* buffer, size_t length) {
    size_t offset = size();
    resize(offset + length);
    memcpy(data() + offset, buffer, length);
  }

  void append(const std::string& string) {
    append(string.data(), string.size());
  }

  void append(const string_view& string) {
    append(string.data(), string.size());
  }

  template <typename T>
  void append(const std::vector<T>& vector) {
    append(vector.data(), vector.size() * sizeof(T));
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
  using array_codec = oroch::varint_codec<uint32_t>;
  using value_codec = oroch::varint_codec<uint32_t>;

  bool empty() const { return !num_entries(); }

  size_t num_entries() const { return key_size_.size(); }

  size_t EstimateSize() const {
    size_t size = key_data_.size() + value_data_.size();
    size += array_codec::space(key_size_.begin(), key_size_.end());
    size += array_codec::space(value_size_.begin(), value_size_.end());
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

  void Marshal(DataBuffer& buffer, bool seekable) const {
    buffer.clear();

    const size_t num = num_entries();
    if (!num) return;

    buffer.reserve(EstimateSize());
    if (!seekable) {
      unsigned char* ptr = buffer.udata();
      array_codec::encode(ptr, key_size_.begin(), key_size_.end());
      array_codec::encode(ptr, value_size_.begin(), value_size_.end());
      buffer.resize(ptr - buffer.udata());
      buffer.append(key_data_);
      buffer.append(value_data_);
    } else {
      size_t k_offset = 0, v_offset = 0;
      for (size_t i = 0; i < num; i++) {
        unsigned char* ptr = buffer.udata() + buffer.size();
        size_t k_size = key_size_[i], v_size = value_size_[i];
        value_codec::value_encode(ptr, k_size);
        value_codec::value_encode(ptr, v_size);
        buffer.resize(ptr - buffer.udata());
        buffer.append(key_data_.data() + k_offset, k_size);
        buffer.append(value_data_.data() + v_offset, v_size);
        k_offset += k_size;
        v_offset += v_size;
      }
    }
  }

  void Unmarshal(DataBuffer& buffer, size_t num, bool seekable) {
    Clear();

    if (!num) return;

    const unsigned char* ptr = buffer.udata();
    if (!seekable) {
      key_size_.resize(num);
      value_size_.resize(num);

      array_codec::decode(key_size_.begin(), key_size_.end(), ptr);
      array_codec::decode(value_size_.begin(), value_size_.end(), ptr);

      size_t k_total =
          std::accumulate(key_size_.begin(), key_size_.end(), size_t(0));
      size_t v_total =
          std::accumulate(value_size_.begin(), value_size_.end(), size_t(0));
      KJ_REQUIRE((ptr + k_total + v_total) <= (buffer.udata() + buffer.size()));

      key_data_.insert(key_data_.end(), ptr, ptr + k_total);
      ptr += k_total;
      value_data_.insert(value_data_.end(), ptr, ptr + v_total);
    } else {
      for (size_t i = 0; i < num; i++) {
        size_t k_size = value_codec::value_decode(ptr);
        size_t v_size = value_codec::value_decode(ptr);
        key_size_.push_back(k_size);
        value_size_.push_back(v_size);
        key_data_.insert(key_data_.end(), ptr, ptr + k_size);
        ptr += k_size;
        value_data_.insert(value_data_.end(), ptr, ptr + v_size);
        ptr += v_size;
      }
    }
  }

  // NB: This requires seekable block format.
  size_t GetEntryOffset(uint32_t num) {
    size_t offset = 0;
    for (uint32_t i = 0; i < num; i++) {
      uint32_t ks = key_size_[i], vs = value_size_[i];
      offset += value_codec::value_space(ks) + ks;
      offset += value_codec::value_space(vs) + vs;
    }
    return offset;
  }

  // NB: This requires seekable block format.
  uint32_t GetEntryNumber(ssize_t offset) {
    uint32_t n = 0;
    for (; offset > 0; n++) {
      uint32_t ks = key_size_[n], vs = value_size_[n];
      offset -= value_codec::value_space(ks) + ks;
      offset -= value_codec::value_space(vs) + vs;
    }
    return n;
  }

 private:
  // Accumulated key data.
  std::vector<uint32_t> key_size_;
  std::vector<char> key_data_;

  // Accumulated value data.
  std::vector<uint32_t> value_size_;
  std::vector<char> value_data_;

 public:
  class Cache {
   public:
    Cache(const WriteOnceBlock& block) : block_(block) {}

    void Clear() {
      keys_.clear();
      values_.clear();
    }

    uint32_t FindEntryByKey(const string_view& key) {
      if (keys_.empty()) InitializeKeys();
      auto pos = std::lower_bound(keys_.begin(), keys_.end(), key);
      return std::distance(keys_.begin(), pos);
    }

    string_view GetKey(uint32_t num) {
      if (keys_.empty()) InitializeKeys();
      return keys_[num];
    }

    string_view GetValue(uint32_t num) {
      if (values_.empty()) InitializeValues();
      return values_[num];
    }

   private:
    void InitializeKeys() {
      size_t num = block_.num_entries();
      if (num == 0) return;

      keys_.resize(num);

      size_t offset = 0;
      for (size_t i = 0; i < num; i++) {
        size_t size = block_.key_size_[i];
        keys_[i] = string_view(block_.key_data_.data() + offset, size);
        offset += size;
      }
    }

    void InitializeValues() {
      size_t num = block_.num_entries();
      if (num == 0) return;

      values_.resize(num);

      size_t offset = 0;
      for (size_t i = 0; i < num; i++) {
        size_t size = block_.value_size_[i];
        values_[i] = string_view(block_.value_data_.data() + offset, size);
        offset += size;
      }
    }

    const WriteOnceBlock& block_;

    std::vector<string_view> keys_;
    std::vector<string_view> values_;
  };
};

/*****************************************************************************/

class WriteOnceIndex {
 public:
  void Clear() {
    size_.clear();
    num_entries_.clear();
    key_size_.clear();
    key_data_.clear();
  }

  size_t num_blocks() const { return key_size_.size(); }

  size_t EstimateSize() const {
    size_t size = key_data_.size();
    size += oroch::varint_codec<size_t>::value_space(num_blocks());
    size += oroch::varint_codec<size_t>::space(size_.begin(), size_.end());
    size += oroch::varint_codec<uint32_t>::space(num_entries_.begin(),
                                                 num_entries_.end());
    size += oroch::varint_codec<uint32_t>::space(key_size_.begin(),
                                                 key_size_.end());
    return size;
  }

  uint64_t GetIndexOffset() const {
    uint64_t offset = sizeof(struct CA_wo_header);
    return std::accumulate(size_.begin(), size_.end(), offset);
  }

  uint64_t GetBlockOffset(size_t num) const {
    uint64_t offset = sizeof(struct CA_wo_header);
    return std::accumulate(size_.begin(), size_.begin() + num, offset);
  }

  size_t GetBlockSize(size_t block) { return size_[block]; }

  uint32_t GetNumEntries(size_t block) { return num_entries_[block]; }

  void Add(const WriteOnceBlock& block, uint32_t size) {
    string_view last_key = block.GetLaskKey();
    size_.push_back(size);
    num_entries_.push_back(block.num_entries());
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
    oroch::varint_codec<size_t>::encode(ptr, size_.begin(), size_.end());
    oroch::varint_codec<uint32_t>::encode(ptr, num_entries_.begin(),
                                          num_entries_.end());
    oroch::varint_codec<uint32_t>::encode(ptr, key_size_.begin(),
                                          key_size_.end());
    buffer.resize(ptr - buffer.udata());
    buffer.append(key_data_);
  }

  void Unmarshal(DataBuffer& buffer) {
    Clear();

    const unsigned char* ptr = buffer.udata();
    size_t num = oroch::varint_codec<size_t>::value_decode(ptr);
    if (num == 0) return;

    size_.resize(num);
    num_entries_.resize(num);
    key_size_.resize(num);

    oroch::varint_codec<size_t>::decode(size_.begin(), size_.end(), ptr);
    oroch::varint_codec<uint32_t>::decode(num_entries_.begin(),
                                          num_entries_.end(), ptr);
    oroch::varint_codec<uint32_t>::decode(key_size_.begin(), key_size_.end(),
                                          ptr);

    size_t key_size =
        std::accumulate(key_size_.begin(), key_size_.end(), size_t(0));
    KJ_REQUIRE((ptr + key_size) <= (buffer.udata() + buffer.size()));
    key_data_.insert(key_data_.end(), ptr, ptr + key_size);
  }

 private:
  // Block sizes.
  std::vector<size_t> size_;

  // Number of entries in blocks.
  std::vector<uint32_t> num_entries_;

  // Last keys in blocks.
  std::vector<uint32_t> key_size_;
  std::vector<char> key_data_;

 public:
  class Cache {
   public:
    Cache(const WriteOnceIndex& index) : index_(index) {}

    uint64_t FindBlockByKey(const string_view& key) {
      if (keys_.empty()) InitializeKeys();
      auto pos = std::lower_bound(keys_.begin(), keys_.end(), key);
      return std::distance(keys_.begin(), pos);
    }

    uint64_t GetBlockOffset(size_t num) {
      if (blocks_.empty()) InitializeBlocks();
      return blocks_[num];
    }

   private:
    void InitializeKeys() {
      size_t num = index_.num_blocks();
      if (num == 0) return;

      keys_.resize(num);

      size_t offset = 0;
      for (size_t i = 0; i < num; i++) {
        size_t size = index_.key_size_[i];
        keys_[i] = string_view(index_.key_data_.data() + offset, size);
        offset += size;
      }
    }

    void InitializeBlocks() {
      size_t num = index_.num_blocks();
      if (num == 0) return;

      blocks_.resize(num);

      blocks_[0] = sizeof(struct CA_wo_header);
      for (size_t i = 1; i < num; i++)
        blocks_[i] = blocks_[i - 1] + index_.size_[i - 1];
    }

    const WriteOnceIndex& index_;

    std::vector<string_view> keys_;
    std::vector<uint64_t> blocks_;
  };
};

/*****************************************************************************/

class WriteOnceBuilder : private PendingFile, public TableBuilder {
 public:
  WriteOnceBuilder(const char* path, const TableOptions& options)
      : PendingFile(path, options.GetFileFlags(), options.GetFileMode()),
        seekable_(options.GetOutputSeekable()),
        no_fsync_(options.GetNoFSync()) {
    KJ_REQUIRE((options.GetFileFlags() & ~(O_EXCL | O_CLOEXEC)) == 0);

    compression_ = options.GetCompression();
    if (compression_ == kTableCompressionDefault)
      compression_ = kTableCompressionNone;
    KJ_REQUIRE(compression_ <= kTableCompressionLast,
               "unsupported compression method");

    compression_level_ = options.GetCompressionLevel();
    if (compression_level_ == 0 && compression_ != kTableCompressionNone)
      compression_level_ = 3;

    WriteHeader(0);
  }

  void InsertRow(const string_view& key, const string_view& value) override {
    KJ_REQUIRE(block_.empty() || block_.GetLaskKey() <= key,
               "unsorted input data");

    const size_t size = key.size() + value.size();
    const size_t block_size = block_.EstimateSize();
    if ((block_size > kBlockSizeMax) ||
        (block_size > kBlockSizeMin && size > kEntrySizeLimit)) {
      WriteBlock(block_, index_);
      block_.Clear();
    }

    block_.Add(key, value);
  }

  void Sync() override {
    WriteBlock(block_, index_);
    WriteIndex(index_);
  }

 private:
  void WriteHeader(uint64_t index_offset) {
    struct CA_wo_header header;
    header.magic = MAGIC;  // Will implicitly store endianness
    header.major_version = MAJOR_VERSION;
    header.minor_version = MINOR_VERSION;
    header.flags = seekable_ ? CA_WO_FLAG_SEEKABLE : 0;
    header.compression = compression_;
    header.data_reserved = 0;
    header.index_offset = index_offset;

    KJ_SYSCALL(lseek(get(), 0, SEEK_SET));
    FileIO(get()).Write(&header, sizeof(header));
  }

  void WriteBlock(const WriteOnceBlock& block, WriteOnceIndex& index) {
    block.Marshal(marshal_buffer_, seekable_);
    if (!marshal_buffer_.size()) return;

    DataBuffer& buffer = seekable_ ? marshal_buffer_ : GetWriteBuffer();
    FileIO(get()).Write(buffer);

    index.Add(block, buffer.size());

    // KJ_DBG(block.num_entries(), buffer.size());
  }

  uint64_t WriteIndex(const WriteOnceIndex& index) {
    index.Marshal(marshal_buffer_);
    if (!marshal_buffer_.size()) return 0;

    DataBuffer& buffer = GetWriteBuffer();
    FileIO(get()).Write(buffer);

    uint64_t index_offset = index.GetIndexOffset();
    WriteHeader(index_offset);
    PendingFile::Finish();

    if (!no_fsync_) {
      // TODO(mortehu): fsync all ancestor directories too.
      KJ_SYSCALL(fsync(get()), path());
    }

    // KJ_DBG(index.num_blocks(), buffer.size());
    return index_offset;
  }

  DataBuffer& GetWriteBuffer() {
    if (compression_ == TableCompression::kTableCompressionNone)
      return marshal_buffer_;

    compress_buffer_.reserve(ZSTD_compressBound(marshal_buffer_.size()));
    compressor_.Go(compress_buffer_, marshal_buffer_, compression_level_);

    // if (compress_buffer_.size() > marshal_buffer_.size())
    //  KJ_DBG(compress_buffer_.size() - marshal_buffer_.size());

    return compress_buffer_;
  }

  // Saved table creation options.
  TableCompression compression_ = TableCompression::kTableCompressionNone;
  int compression_level_ = 0;
  const bool seekable_;
  const bool no_fsync_;

  // Result data.
  WriteOnceIndex index_;
  WriteOnceBlock block_;

  // A buffer for block marshaling.
  DataBuffer marshal_buffer_;
  // A buffer for block compression.
  DataBuffer compress_buffer_;
  // Compression context.
  ZstdCompressor compressor_;
};

/*****************************************************************************/

class WriteOnceSortingBuilder final : public WriteOnceBuilder {
 public:
  WriteOnceSortingBuilder(const char* path, const TableOptions& options)
      : WriteOnceBuilder(path, options) {
    std::string dir(".");
    if (const char* last_slash = strrchr(path, '/')) {
      KJ_REQUIRE(path != last_slash);
      dir = std::string(path, last_slash);
    }

    raw_fd_ = AnonTemporaryFile(dir.c_str());

    raw_stream_ = fdopen(raw_fd_.get(), "w");
    if (!raw_stream_) KJ_FAIL_SYSCALL("fdopen", errno, path);

    index_.reserve(12 * 1024 * 1024);
  }

  virtual ~WriteOnceSortingBuilder() noexcept {
    try {
      raw_fd_ = nullptr;
    } catch (...) {
    }
    if (raw_stream_) fclose(raw_stream_);
  }

  void InsertRow(const string_view& key, const string_view& value) override {
    AddEntry(key, value);
    WriteEntryData(key, value);
  }

  void Sync() override {
    FlushEntryData();
    SortEntries();

    DataBuffer buffer;
    for (const Entry& entry : index_) {
      buffer.resize(entry.key_size + entry.value_size);
      FileIO(raw_fd_).Read(buffer, entry.offset);

      const char* data = buffer.data();
      const string_view key(data, entry.key_size);
      const string_view value(data + entry.key_size, entry.value_size);
      WriteOnceBuilder::InsertRow(key, value);
    }

    WriteOnceBuilder::Sync();
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

    // KJ_LOG(DBG, index_.size(), read_count_);

    lhs_buffer_.reset();
    rhs_buffer_.reset();
  }

  // Temporary file for all added key/value pairs.
  kj::AutoCloseFd raw_fd_;
  FILE* raw_stream_ = nullptr;

  // Index of all added key/value pairs.
  std::vector<Entry> index_;

  // Running file offset.
  uint64_t offset_ = 0;

  // Longest encountered key size.
  uint32_t key_size_max_ = 0;
  // Buffers for key comparison.
  std::unique_ptr<char[]> lhs_buffer_;
  std::unique_ptr<char[]> rhs_buffer_;

  // Read statistics.
  uint64_t read_count_ = 0;
};

/*****************************************************************************/

class WriteOnceTableBase {
 public:
  WriteOnceTableBase(kj::AutoCloseFd fd, uint64_t index_offset)
      : fd_(std::move(fd)), index_offset_(index_offset) {}

  virtual ~WriteOnceTableBase() noexcept {
    try {
      fd_ = nullptr;
    } catch (...) {
    }
  }

 protected:
  kj::AutoCloseFd fd_;

  uint64_t index_offset_;
};

class WriteOnceTable : public WriteOnceTableBase, public Table {
 public:
  WriteOnceTable(kj::AutoCloseFd fd, const struct stat& st,
                 uint64_t index_offset)
      : WriteOnceTableBase(std::move(fd), index_offset), Table(st) {}
};

class WriteOnceSeekableTable : public WriteOnceTableBase, public SeekableTable {
 public:
  WriteOnceSeekableTable(kj::AutoCloseFd fd, const struct stat& st,
                         uint64_t index_offset)
      : WriteOnceTableBase(std::move(fd), index_offset), SeekableTable(st) {}

  off_t Offset() final { return offset_ - sizeof(struct CA_wo_header); }

  void SeekToFirst() final { offset_ = sizeof(struct CA_wo_header); }

  void Seek(off_t offset, int whence) final {
    switch (whence) {
      case SEEK_SET:
        offset += sizeof(struct CA_wo_header);
        break;

      case SEEK_CUR:
        offset += offset_;
        break;

      case SEEK_END:
        offset = index_offset_ - offset;
        break;

      default:
        KJ_FAIL_REQUIRE(!"Invalid 'whence' value");
    }

    KJ_REQUIRE(offset >= sizeof(struct CA_wo_header),
               "attempt to seek before start of table");
    KJ_REQUIRE(offset <= index_offset_, "attempt to seek past end of table");

    offset_ = offset;
  }

  bool Skip(size_t count) final {
    string_view key, value;
    while (count--) {
      if (!ReadRow(key, value)) return false;
    }
    return true;
  }

 protected:
  // Used for read, seek, offset.
  uint64_t offset_ = sizeof(struct CA_wo_header);
};

/*****************************************************************************/

class WriteOnceTable_v4 final : public WriteOnceTable {
 public:
  WriteOnceTable_v4(kj::AutoCloseFd fd, const struct stat& st,
                    uint64_t index_offset, TableCompression compression)
      : WriteOnceTable(std::move(fd), st, index_offset),
        compression_(compression),
        index_cache_(index_),
        block_cache_(block_) {
    ReadIndex();
  }

  int IsSorted() override { return 1; }

  void SeekToFirst() override {
    block_num_ = 0;
    entry_num_ = 0;
  }

  bool SeekToKey(const string_view& key) override {
    uint64_t block_num = index_cache_.FindBlockByKey(key);
    if (block_num >= index_.num_blocks()) return NotFound();
    if (block_num != block_read_num_) ReadBlock(block_num);

    block_num_ = block_num;
    entry_num_ = block_cache_.FindEntryByKey(key);
    return block_cache_.GetKey(entry_num_) == key;
  }

  bool Skip(size_t count) override {
    uint64_t block_num = block_num_;
    if (block_num == UINT64_MAX) {
      SeekToFirst();
      block_num = 0;
    }

    uint32_t entry_num = entry_num_;
    while (count) {
      if (block_num >= index_.num_blocks()) return NotFound();

      uint32_t avail = index_.GetNumEntries(block_num) - entry_num;
      if (count < avail) {
        entry_num += count;
        count = 0;
      } else {
        block_num++;
        entry_num = 0;
        count -= avail;
      }
    }

    block_num_ = block_num;
    entry_num_ = entry_num;
    return true;
  }

  bool ReadRow(string_view& key, string_view& value) override {
    if (block_num_ == UINT64_MAX) SeekToFirst();
    if (block_num_ >= index_.num_blocks()) return false;
    if (block_num_ != block_read_num_) ReadBlock(block_num_);

    key = block_cache_.GetKey(entry_num_);
    value = block_cache_.GetValue(entry_num_);

    if (++entry_num_ >= index_.GetNumEntries(block_num_)) {
      ++block_num_;
      entry_num_ = 0;
    }

    return true;
  }

 private:
  void ReadIndex() {
    uint64_t size = st.st_size - index_offset_;
    bool compressed = (compression_ != kTableCompressionNone);
    index_.Unmarshal(Read(index_offset_, size, compressed));
  }

  void ReadBlock(size_t num) {
    KJ_REQUIRE(num < index_.num_blocks());

    uint64_t offset = index_cache_.GetBlockOffset(num);
    size_t size = index_.GetBlockSize(num);
    uint32_t num_entries = index_.GetNumEntries(num);
    bool compressed = (compression_ != kTableCompressionNone);
    block_.Unmarshal(Read(offset, size, compressed), num_entries, false);

    block_read_num_ = num;
    block_cache_.Clear();
  }

  bool NotFound() {
    block_num_ = index_.num_blocks();
    entry_num_ = 0;
    return false;
  }

  DataBuffer& Read(uint64_t offset, size_t size, bool compressed) {
    read_buffer_.resize(size);
    FileIO(fd_).Read(read_buffer_, offset);

    if (!compressed) return read_buffer_;

    size_t decomp_size = ZSTD_getDecompressedSize(read_buffer_.data(), size);
    decompress_buffer_.resize(decomp_size);
    decompressor_.Go(decompress_buffer_, read_buffer_);

    return decompress_buffer_;
  }

  const TableCompression compression_;

  WriteOnceIndex index_;
  WriteOnceIndex::Cache index_cache_;

  WriteOnceBlock block_;
  WriteOnceBlock::Cache block_cache_;

  uint64_t block_read_num_ = UINT64_MAX;
  uint64_t block_num_ = UINT64_MAX;
  uint32_t entry_num_ = UINT32_MAX;

  DataBuffer read_buffer_;
  DataBuffer decompress_buffer_;
  // Decompression context.
  ZstdDecompressor decompressor_;
};

/*****************************************************************************/

class WriteOnceSeekableTable_v4 final : public WriteOnceSeekableTable {
 public:
  WriteOnceSeekableTable_v4(const std::string& path, kj::AutoCloseFd fd,
                            const struct stat& st, uint64_t index_offset,
                            TableCompression compression)
      : WriteOnceSeekableTable(std::move(fd), st, index_offset),
        index_cache_(index_) {
    uint64_t size = st.st_size - index_offset_;

    DataBuffer read_buffer;
    read_buffer.resize(size);
    FileIO(fd_).Read(read_buffer, index_offset_);

    if (compression == kTableCompressionNone) {
      index_.Unmarshal(read_buffer);
    } else {
      size = ZSTD_getDecompressedSize(read_buffer.data(), size);

      DataBuffer decompress_buffer;
      decompress_buffer.resize(size);

      ZstdDecompressor decompressor;
      decompressor.Go(decompress_buffer, read_buffer);

      index_.Unmarshal(decompress_buffer);
    }

    map_ = mmap(NULL, index_offset_, PROT_READ, MAP_SHARED, fd_, 0);
    if (MAP_FAILED == map_) KJ_FAIL_SYSCALL("mmap", errno, path);
  }

  virtual ~WriteOnceSeekableTable_v4() {
    if (map_ != MAP_FAILED) munmap(map_, index_offset_);
  }

  int IsSorted() override { return 1; }

  bool SeekToKey(const string_view& key) override {
    uint64_t block_num = index_cache_.FindBlockByKey(key);

    if (block_num < index_.num_blocks()) {
      const unsigned char* base = reinterpret_cast<unsigned char*>(map_);
      const unsigned char* ptr = base + index_cache_.GetBlockOffset(block_num);
      const unsigned char* end = base + index_offset_;

      while (ptr < end) {
        const unsigned char* start_ptr = ptr;
        uint32_t k_size = oroch::varint_codec<uint32_t>::value_decode(ptr);
        uint32_t v_size = oroch::varint_codec<uint32_t>::value_decode(ptr);

        string_view cur(reinterpret_cast<const char*>(ptr), k_size);
        int result = cur.compare(key);
        if (result >= 0) {
          offset_ = start_ptr - base;
          return result == 0;
        }

        ptr += k_size + v_size;
      }
    }

    offset_ = index_offset_;
    return false;
  }

  bool ReadRow(string_view& key, string_view& value) override {
    if (offset_ >= index_offset_) return false;

    const unsigned char* base = reinterpret_cast<unsigned char*>(map_);
    const unsigned char* ptr = base + offset_;

    uint32_t k_size = oroch::varint_codec<uint32_t>::value_decode(ptr);
    uint32_t v_size = oroch::varint_codec<uint32_t>::value_decode(ptr);

    key = string_view(reinterpret_cast<const char*>(ptr), k_size);
    ptr += k_size;

    value = string_view(reinterpret_cast<const char*>(ptr), v_size);
    ptr += v_size;

    offset_ = ptr - base;
    KJ_REQUIRE(offset_ <= index_offset_);

    return true;
  }

 private:
  void* map_ = MAP_FAILED;

  WriteOnceIndex index_;
  WriteOnceIndex::Cache index_cache_;
};

/*****************************************************************************/

uint64_t CA_wo_hash(const string_view& str) {
  auto result = UINT64_C(0x2257d6803a6f1b2);

  for (auto ch : str) result = result * 31 + static_cast<unsigned char>(ch);

  return result;
}

class WriteOnceTable_v3 final : public WriteOnceSeekableTable {
 public:
  WriteOnceTable_v3(const std::string& path, kj::AutoCloseFd fd,
                    const struct stat& st, uint64_t index_offset)
      : WriteOnceSeekableTable(std::move(fd), st, index_offset) {
    MemoryMap(path);
  }

  virtual ~WriteOnceTable_v3() {
    if (buffer_ != MAP_FAILED) munmap(buffer_, buffer_size_);
  }

  int IsSorted() override {
    return 0 != (header_->flags & CA_WO_FLAG_ASCENDING);
  }

  bool SeekToKey(const string_view& key) override {
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

  bool ReadRow(string_view& key, string_view& value) override {
    KJ_REQUIRE(offset_ >= sizeof(struct CA_wo_header));

    uint8_t* p = reinterpret_cast<uint8_t*>(buffer_) + offset_;
    if (offset_ >= header_->index_offset || *p == 0) return false;

    uint64_t size = ca_parse_integer((const uint8_t**)&p);

    key = string_view(reinterpret_cast<char*>(p),
                      strlen(reinterpret_cast<char*>(p)));

    KJ_ASSERT(size > key.size(), size, key.size());

    value = string_view(reinterpret_cast<char*>(p) + key.size() + 1,
                        size - key.size() - 1);

    offset_ = p + size - reinterpret_cast<uint8_t*>(buffer_);

    return true;
  }

 private:
  void MemoryMap(const std::string& path) {
    uint64_t size = st.st_size;

    KJ_REQUIRE(static_cast<size_t>(size) > sizeof(struct CA_wo_header), size,
               sizeof(struct CA_wo_header));

    buffer_size_ = size;
    buffer_fill_ = size;

    if (MAP_FAILED ==
        (buffer_ = mmap(NULL, size, PROT_READ, MAP_SHARED, fd_, 0))) {
      KJ_FAIL_SYSCALL("mmap", errno, path);
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

static void ReadHeader(struct CA_wo_header& header, int fd) {
  FileIO(fd).Read(&header, sizeof header);
  KJ_REQUIRE(header.magic == MAGIC, header.magic, MAGIC);
  KJ_REQUIRE(header.major_version <= MAJOR_VERSION ||
             header.major_version >= 2);

  if (header.major_version <= 3) {
    KJ_REQUIRE(header.compression == 0, "unsupported compression method",
               header.compression);
  } else {
    KJ_REQUIRE(header.compression <= kTableCompressionLast,
               "unsupported compression method", header.compression);
    if ((header.flags & CA_WO_FLAG_EXTENDED) != 0)
      KJ_UNIMPLEMENTED("extended write-once tables are not supported yet");
  }
}

/*****************************************************************************/

std::unique_ptr<TableBuilder> WriteOnceTableBackend::Create(
    const char* path, const TableOptions& options) {
  return options.GetInputUnsorted()
             ? std::make_unique<WriteOnceSortingBuilder>(path, options)
             : std::make_unique<WriteOnceBuilder>(path, options);
}

std::unique_ptr<Table> WriteOnceTableBackend::Open(const char* path,
                                                   kj::AutoCloseFd fd,
                                                   const struct stat& st) {
  struct CA_wo_header header;
  ReadHeader(header, fd);

  if (header.major_version <= 3)
    return std::make_unique<WriteOnceTable_v3>(path, std::move(fd), st,
                                               header.index_offset);

  TableCompression compression = TableCompression(header.compression);
  if ((header.flags & CA_WO_FLAG_SEEKABLE) == 0)
    return std::make_unique<WriteOnceTable_v4>(
        std::move(fd), st, header.index_offset, compression);

  return std::make_unique<WriteOnceSeekableTable_v4>(
      path, std::move(fd), st, header.index_offset, compression);
}

std::unique_ptr<SeekableTable> WriteOnceTableBackend::OpenSeekable(
    const char* path, kj::AutoCloseFd fd, const struct stat& st) {
  struct CA_wo_header header;
  ReadHeader(header, fd);

  if (header.major_version <= 3)
    return std::make_unique<WriteOnceTable_v3>(path, std::move(fd), st,
                                               header.index_offset);

  if ((header.flags & CA_WO_FLAG_SEEKABLE) == 0)
    KJ_FAIL_REQUIRE("the write-once table is not seekable", path);

  TableCompression compression = TableCompression(header.compression);
  return std::make_unique<WriteOnceSeekableTable_v4>(
      path, std::move(fd), st, header.index_offset, compression);
}

}  // namespace internal
}  // namespace table
}  // namespace cantera
