/*
    LevelDB table backend for Cantera Table
    Copyright (C) 2014    Morten Hustveit
    Copyright (C) 2014    eVenture Capital Partners II

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

#include "src/table-backend-leveldb-table.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include <kj/debug.h>
#include <leveldb/env.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/table.h>
#include <leveldb/table_builder.h>

#include "src/ca-table.h"
#include "src/util.h"

#if !HAVE_FDATASYNC || !HAVE_DECL_FDATASYNC
# define fdatasync fsync
#endif

#define TMP_SUFFIX ".tmp.XXXXXX"

#define CHECK_STATUS(expr)                                       \
  do {                                                           \
    auto status = (expr);                                        \
    if (!status.ok()) KJ_FAIL_REQUIRE(#expr, status.ToString()); \
  } while (0)

namespace cantera {
namespace table {

using namespace internal;

namespace {

class LevelDBWriter : public PendingFile, public leveldb::WritableFile {
 public:
  LevelDBWriter(const char* path, int flags, mode_t mode)
      : PendingFile(path, flags, mode) {}

  leveldb::Status Append(const leveldb::Slice& data) override {
    auto start = reinterpret_cast<const char*>(data.data());
    size_t offset = 0;
    while (offset < data.size()) {
      auto amount_written = write(get(), start + offset, data.size() - offset);
      if (amount_written < 0)
        return leveldb::Status::IOError("write failed", strerror(errno));
      if (static_cast<size_t>(amount_written) == 0)
        return leveldb::Status::IOError("short write");
      offset += amount_written;
    }
    return leveldb::Status::OK();
  }

  leveldb::Status Close() override {
    PendingFile::Close();
    return leveldb::Status::OK();
  }

  leveldb::Status Flush() override { return leveldb::Status::OK(); }

  leveldb::Status Sync() override {
    if (-1 == fdatasync(get()))
      return leveldb::Status::IOError("fdatasync failed", strerror(errno));
    return leveldb::Status::OK();
  }
};

class LevelDBReader : public leveldb::RandomAccessFile {
 public:
  LevelDBReader(const char* path) : fd_(OpenFile(path, O_RDONLY | O_CLOEXEC)) {}

  ~LevelDBReader() noexcept(true) {
    try {
      fd_ = nullptr;
    } catch (...) {
    }
  }

  leveldb::Status Read(uint64_t offset, size_t n, leveldb::Slice* result,
                       char* scratch) const override {
    auto amount_read = pread(fd_, scratch, n, offset);
    if (amount_read < 0)
      return leveldb::Status::IOError("pread failed", strerror(errno));
    *result = leveldb::Slice(scratch, static_cast<size_t>(n));
    return leveldb::Status::OK();
  }

  uint64_t FileSize() const { return cantera::table::internal::FileSize(fd_); }

 private:
  kj::AutoCloseFd fd_;
};

/*****************************************************************************/

class LevelDBTable : public Table {
 public:
  LevelDBTable(const char* path, const TableOptions& options) {
    leveldb::Options leveldb_options;
    switch (options.GetCompression()) {
      case kTableCompressionNone:
        leveldb_options.compression = leveldb::kNoCompression;
        break;
      case kTableCompressionDefault:
        leveldb_options.compression = leveldb::kSnappyCompression;
        break;
      default:
        KJ_FAIL_REQUIRE("LevelDB tables do not support given compression method");
    }

    writable_file_ = std::make_unique<LevelDBWriter>(
        path, options.GetFileFlags(), options.GetFileMode());
    table_builder_ = std::make_unique<leveldb::TableBuilder>(
        leveldb_options, writable_file_.get());
  }

  LevelDBTable(const char* path) {
    file_ = std::make_unique<LevelDBReader>(path);

    leveldb::Table* table;
    CHECK_STATUS(leveldb::Table::Open(leveldb::Options(), file_.get(),
                                      file_->FileSize(), &table));
    table_.reset(table);

    iterator_.reset(table_->NewIterator(leveldb::ReadOptions()));
    iterator_->SeekToFirst();
    if (!iterator_->Valid()) eof_ = true;
  }

  void Sync() override {
    KJ_REQUIRE(table_builder_ != nullptr);
    CHECK_STATUS(table_builder_->Finish());
    writable_file_->Finish();
  }

  int IsSorted() override { return true; }

  void InsertRow(const struct iovec* value, size_t value_count) override {
    KJ_REQUIRE(value_count == 2);

    Add(leveldb::Slice(reinterpret_cast<const char*>(value[0].iov_base),
                       value[0].iov_len),
        leveldb::Slice(reinterpret_cast<const char*>(value[1].iov_base),
                       value[1].iov_len));
  }

  void SeekToFirst() override {
    iterator_->SeekToFirst();
    need_seek_ = false;
    eof_ = !iterator_->Valid();
    KJ_REQUIRE(!eof_);
  }

  bool SeekToKey(const string_view& key) override {
    return SeekToKey(leveldb::Slice(key.data(), key.size()));
  }

  bool Skip(size_t count) override {
    while (count-- > 0) {
      if (eof_) return false;

      iterator_->Next();

      if (!iterator_->Valid()) {
        need_seek_ = false;
        eof_ = true;
      }
    }

    return true;
  }

  bool ReadRow(struct iovec* key, struct iovec* value) override {
    return ReadRow(const_cast<const void**>(&key->iov_base), &key->iov_len,
                   const_cast<const void**>(&value->iov_base), &value->iov_len);
  }

  void Add(const leveldb::Slice& key, const leveldb::Slice& value) {
    KJ_REQUIRE(table_builder_ != nullptr);
    auto key_begin = reinterpret_cast<const uint8_t*>(key.data());
    auto key_end = key_begin + key.size();
    if (!std::lexicographical_compare(prev_key_.begin(), prev_key_.end(),
                                      key_begin, key_end)) {
      KJ_FAIL_REQUIRE("keys inserted out of order",
                      std::string(prev_key_.begin(), prev_key_.end()),
                      std::string(key_begin, key_end));
    }
    table_builder_->Add(key, value);
    prev_key_.assign(key_begin, key_end);
    CHECK_STATUS(table_builder_->status());
  }

  bool SeekToKey(const leveldb::Slice& key) {
    KJ_REQUIRE(iterator_ != nullptr);

    iterator_->Seek(key);

    need_seek_ = false;

    eof_ = !iterator_->Valid();

    if (eof_) return false;

    return 0 == key.compare(iterator_->key());
  }

  // Reads one row.  Returns true if a value was read successfully, or false if
  // end of file was reached instead.
  bool ReadRow(const void** out_key, size_t* out_key_size,
               const void** out_data, size_t* out_data_size) {
    KJ_REQUIRE(iterator_ != nullptr);

    if (need_seek_) {
      iterator_->Next();
      need_seek_ = false;
      if (!iterator_->Valid()) eof_ = true;
    }

    if (eof_) return false;

    auto value = iterator_->value();
    *out_data = reinterpret_cast<const void*>(value.data());
    *out_data_size = value.size();

    auto key = iterator_->key();
    *out_key = reinterpret_cast<const void*>(key.data());
    *out_key_size = key.size();

    need_seek_ = true;

    return true;
  }

 private:
  // Writing
  std::unique_ptr<LevelDBWriter> writable_file_;
  std::unique_ptr<leveldb::TableBuilder> table_builder_;

  // Used to ensure rows are inserted in lexicographical order.
  std::vector<uint8_t> prev_key_;

  // Reading
  std::unique_ptr<LevelDBReader> file_;
  std::unique_ptr<leveldb::Table> table_;
  std::unique_ptr<leveldb::Iterator> iterator_;
  bool need_seek_ = false;
  bool eof_ = false;
};

}  // namespace

std::unique_ptr<Table> LevelDBTableBackend::Create(
    const char* path, const TableOptions& options) {
  return std::make_unique<LevelDBTable>(path, options);
}

std::unique_ptr<Table> LevelDBTableBackend::Open(const char* path) {
  return std::make_unique<LevelDBTable>(path);
}

std::unique_ptr<SeekableTable> LevelDBTableBackend::OpenSeekable(
    const char* path) {
  KJ_FAIL_REQUIRE("LevelDB tables are not seekable");
  return nullptr;
}

}  // namespace table
}  // namespace cantera
