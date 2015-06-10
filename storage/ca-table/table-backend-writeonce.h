#ifndef STORAGE_CA_TABLE_TABLE_BACKEND_WRITEONCE_H_
#define STORAGE_CA_TABLE_TABLE_BACKEND_WRITEONCE_H_ 1

#include "storage/ca-table/ca-table.h"

namespace ca_table {

class WriteOnceTableBackend : public Backend {
  std::unique_ptr<Table> Open(const char* path, int flags,
                              mode_t mode) override;
};

}  // namespace ca_table

#endif  // !STORAGE_CA_TABLE_TABLE_BACKEND_WRITEONCE_H_
