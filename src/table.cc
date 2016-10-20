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

static const char* detect_table_format(const char* path, int fd, off_t length) {
  uint64_t magic;
  ReadWithOffset(fd, &magic, sizeof(magic), 0);

  if (magic == UINT64_C(0x6c6261742e692e70)) return "write-once";

  ReadWithOffset(fd, &magic, sizeof(magic), length - 8);

  if (magic == UINT64_C(0xdb4775248b80fb57)) return "leveldb-table";

  KJ_FAIL_REQUIRE("Unrecognized table format", path);
}

// Do not use kj::AutoCloseFd here as the fd is to be passed through the
// external API and we do not want to pollute it with extra dependencies.
static int open_and_stat(struct stat& st, const char* path) {
  const int fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd == -1) KJ_FAIL_SYSCALL("open", errno, path);

  // A bit of ugly C-style code.
  if (fstat(fd, &st) == -1) {
    int errno_fstat = errno;
    close(fd);
    KJ_FAIL_SYSCALL("fstat", errno_fstat, path);
  }

  return fd;
}

static Backend* get_backend(const char* backend_name, const char* path) {
  if (!backend_name) {
    //
    // FIXME: Perhaps a wiser approach is to raise an error immediately.
    //
    // Infer the backend from a previous instance of the file if any.
    kj::AutoCloseFd fd = OpenFile(path, O_RDONLY | O_CLOEXEC);
    backend_name = detect_table_format(path, fd.get(), FileSize(fd.get()));
  }
  return ca_table_backend(backend_name);
}

static Backend* get_backend(const char* backend_name, const char* path, int fd,
                            const struct stat& st) {
  if (!backend_name) backend_name = detect_table_format(path, fd, st.st_size);
  return ca_table_backend(backend_name);
}

std::unique_ptr<Backend> leveldb_table_backend;
std::unique_ptr<Backend> writeonce_backend;

}  // namespace

TableBuilder::~TableBuilder() {}

Table::Table(const struct stat& s) : st(s) {}

Table::~Table() {}

SeekableTable::SeekableTable(const struct stat& s) : Table(s) {}

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
  int fd = open_and_stat(st, path);
  return get_backend(backend_name, path, fd, st)->Open(path, fd, st);
}

std::unique_ptr<SeekableTable> TableFactory::OpenSeekable(
    const char* backend_name, const char* path) {
  struct stat st;
  int fd = open_and_stat(st, path);
  return get_backend(backend_name, path, fd, st)->OpenSeekable(path, fd, st);
}

}  // namespace table
}  // namespace cantera
