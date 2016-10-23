#ifndef STORAGE_CA_TABLE_TABLE_BACKEND_TABLE_H_
#define STORAGE_CA_TABLE_TABLE_BACKEND_TABLE_H_ 1

#include "src/ca-table.h"

#include <kj/io.h>

namespace cantera {
namespace table {

class Backend {
 public:
  virtual ~Backend();

  virtual std::unique_ptr<TableBuilder> Create(const char* path,
                                               const TableOptions& options) = 0;

  virtual std::unique_ptr<Table> Open(const char* path, kj::AutoCloseFd fd,
                                      const struct stat& st) = 0;

  virtual std::unique_ptr<SeekableTable> OpenSeekable(
      const char* path, kj::AutoCloseFd fd, const struct stat& st) = 0;
};

Backend* ca_table_backend(const char* name);

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_TABLE_BACKEND_TABLE_H_
