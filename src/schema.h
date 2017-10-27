#ifndef STORAGE_CA_TABLE_SCHEMA_H_
#define STORAGE_CA_TABLE_SCHEMA_H_ 1

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "src/ca-table.h"

#include "third_party/evenk/evenk/synch.h"

namespace cantera {
namespace table {

class Table;
class SeekableTable;

class TableWithLock {
 public:
  TableWithLock() = default;
  TableWithLock(TableWithLock&& tab) : table(std::move(tab.table)) {}
  TableWithLock(std::unique_ptr<Table>&& tab) : table(std::move(tab)) {}

  std::unique_ptr<Table> table;
  mutable evenk::default_synch::lock_type lock;
};

class Schema {
 public:
  Schema(std::string path);
  ~Schema();

  void Load();

  std::vector<std::pair<uint64_t, std::unique_ptr<SeekableTable>>>
      summary_tables;

  std::vector<std::unique_ptr<Table>> summary_override_tables;

  // Lazy-loads the index tables.
  std::vector<TableWithLock>& IndexTables();

 private:
  std::string path_;

  bool loaded_ = false;

  std::vector<std::string> index_table_paths_;
  std::vector<TableWithLock> index_tables_;
};

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_SCHEMA_H_
