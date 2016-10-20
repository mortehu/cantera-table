#include <cstring>
#include <memory>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "src/table-backend-leveldb-table.h"
#include "src/table-backend-writeonce.h"
#include "src/util.h"

namespace cantera {
namespace table {

using namespace internal;

namespace {

const char* detect_table_format(const char* path) {
  kj::AutoCloseFd fd(OpenFile(path, O_RDONLY));

  uint64_t magic;
  ReadWithOffset(fd.get(), &magic, sizeof(magic), 0);

  if (magic == UINT64_C(0x6c6261742e692e70)) return "write-once";

  off_t length;
  KJ_SYSCALL(length = lseek(fd.get(), 0, SEEK_END));

  ReadWithOffset(fd.get(), &magic, sizeof(magic), length - 8);

  if (magic == UINT64_C(0xdb4775248b80fb57)) return "leveldb-table";

  KJ_FAIL_REQUIRE("Unrecognized table format", path);
}

static void init_stat(struct stat& st, const char* path) {
  std::memset(&st, 0, sizeof(st));
  KJ_SYSCALL(stat(path, &st), path);
}

static Backend* get_backend(const char* backend_name, const char* path) {
  if (!backend_name) backend_name = detect_table_format(path);
  return ca_table_backend(backend_name);
}

std::unique_ptr<Backend> leveldb_table_backend;
std::unique_ptr<Backend> writeonce_backend;

}  // namespace

Table::Table() {}

Table::~Table() {}

TableBuilder::~TableBuilder() {}

Backend::~Backend() {}

ca_offset_score::ca_offset_score(uint64_t offset, const ca_score& score)
    : offset(offset),
      score(score.score),
      score_pct5(score.score_pct5),
      score_pct25(score.score_pct25),
      score_pct75(score.score_pct75),
      score_pct95(score.score_pct95) {}

/*****************************************************************************/

Backend* ca_table_backend(const char* name) {
  if (!strcmp(name, "leveldb-table")) {
    if (!leveldb_table_backend)
      leveldb_table_backend = std::make_unique<LevelDBTableBackend>();
    return leveldb_table_backend.get();
  }

  if (!strcmp(name, "write-once")) {
    if (!writeonce_backend)
      writeonce_backend = std::make_unique<WriteOnceTableBackend>();
    return writeonce_backend.get();
  }

  KJ_FAIL_REQUIRE("Unknown table backend", name);
}

/*****************************************************************************/

std::unique_ptr<TableBuilder> TableFactory::Create(
    const char* backend_name, const char* path, const TableOptions& options) {
  return get_backend(backend_name, path)->Create(path, options);
}

std::unique_ptr<Table> TableFactory::Open(const char* backend_name,
                                          const char* path) {
  struct stat st;
  init_stat(st, path);

  Backend* backend = get_backend(backend_name, path);
  auto result = backend->Open(path);

  result->st = st;

  return result;
}

std::unique_ptr<SeekableTable> TableFactory::OpenSeekable(
    const char* backend_name, const char* path) {
  struct stat st;
  init_stat(st, path);

  Backend* backend = get_backend(backend_name, path);
  auto result = backend->OpenSeekable(path);

  result->st = st;

  return result;
}

}  // namespace table
}  // namespace cantera
