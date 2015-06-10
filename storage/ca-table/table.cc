#include <cstring>
#include <memory>

#include <kj/debug.h>

#include "base/file.h"
#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/error.h"
#include "storage/ca-table/table-backend-leveldb-table.h"
#include "storage/ca-table/table-backend-writeonce.h"

uint64_t ca_xid;

/*****************************************************************************/

namespace {

const char* detect_table_format(const char* path) {
  kj::AutoCloseFd fd(ev::OpenFile(path, O_RDONLY));

  uint64_t magic;
  ev::PRead(fd.get(), &magic, sizeof(magic), 0);

  if (magic == 0x6c6261742e692e70ULL) return "write-once";

  off_t length;
  KJ_SYSCALL(length = lseek(fd.get(), 0, SEEK_END));

  ev::PRead(fd.get(), &magic, sizeof(magic), length - 8);

  if (magic == 0xdb4775248b80fb57ULL) return "leveldb-table";

  KJ_FAIL_REQUIRE("Unrecognized table format", path);
}

std::unique_ptr<ca_table::Backend> leveldb_table_backend;
std::unique_ptr<ca_table::Backend> writeonce_backend;

}  // namespace

namespace ca_table {

Table::Table() {}

Table::~Table() {}

Backend::~Backend() {}

}  // namespace ca_table

ca_offset_score::ca_offset_score(uint64_t offset, const ca_score& score)
    : offset(offset),
      score(score.score),
      score_pct5(score.score_pct5),
      score_pct25(score.score_pct25),
      score_pct75(score.score_pct75),
      score_pct95(score.score_pct95) {}

/*****************************************************************************/

ca_table::Backend* ca_table_backend(const char* name) {
  if (!strcmp(name, "leveldb-table")) {
    if (!leveldb_table_backend)
      leveldb_table_backend = std::make_unique<ca_table::LevelDBTableBackend>();
    return leveldb_table_backend.get();
  }

  if (!strcmp(name, "write-once")) {
    if (!writeonce_backend)
      writeonce_backend = std::make_unique<ca_table::WriteOnceTableBackend>();
    return writeonce_backend.get();
  }

  KJ_FAIL_REQUIRE("Unknown table backend", name);
}

/*****************************************************************************/

std::unique_ptr<ca_table::Table> ca_table_open(const char* backend_name,
                                               const char* path, int flags,
                                               mode_t mode) {
  struct stat st;
  memset(&st, 0, sizeof(st));

  if (!(flags & O_CREAT)) {
    KJ_SYSCALL(stat(path, &st), path);
  }

  if (!backend_name) backend_name = detect_table_format(path);

  auto result = ca_table_backend(backend_name)->Open(path, flags, mode);

  result->st = st;

  return result;
}
