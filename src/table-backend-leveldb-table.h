#ifndef STORAGE_CA_TABLE_TABLE_BACKEND_LEVELDB_TABLE_H_
#define STORAGE_CA_TABLE_TABLE_BACKEND_LEVELDB_TABLE_H_ 1

#include "src/ca-table.h"

namespace cantera {
namespace table {

class LevelDBTableBackend : public Backend {
  std::unique_ptr<Table> Open(const char* path, int flags,
                              mode_t mode) override;
};

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_TABLE_BACKEND_LEVELDB_TABLE_H_
