#ifndef STORAGE_CA_TABLE_TABLE_BACKEND_LEVELDB_TABLE_H_
#define STORAGE_CA_TABLE_TABLE_BACKEND_LEVELDB_TABLE_H_ 1

#include "src/table-backend.h"

namespace cantera {
namespace table {

class LevelDBTableBackend final : public Backend {
 public:
  std::unique_ptr<TableBuilder> Create(const char* path,
                                       const TableOptions& options) override;

  std::unique_ptr<Table> Open(const char* path, kj::AutoCloseFd fd,
                              const struct stat& st) override;

  std::unique_ptr<SeekableTable> OpenSeekable(const char* path,
                                              kj::AutoCloseFd fd,
                                              const struct stat& st) override;
};

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_TABLE_BACKEND_LEVELDB_TABLE_H_
