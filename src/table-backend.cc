
#include "src/table-backend.h"
#include "src/table-backend-leveldb-table.h"
#include "src/table-backend-writeonce.h"

#include <kj/debug.h>

namespace cantera {
namespace table {
namespace internal {

Backend* ca_table_backend(const char* name) {
  static std::unique_ptr<Backend> leveldb_table_backend;
  static std::unique_ptr<Backend> writeonce_backend;

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
  return nullptr;
}

}  // namespace internal
}  // namespace table
}  // namespace cantera
