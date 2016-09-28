#ifndef STORAGE_CA_TABLE_SCHEMA_H_
#define STORAGE_CA_TABLE_SCHEMA_H_ 1

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace cantera {
namespace table {

class Table;
class SeekableTable;

class Schema {
 public:
  Schema(std::string path);
  ~Schema();

  void Load();

  std::vector<std::pair<uint64_t, std::unique_ptr<SeekableTable>>>
      summary_tables;

  std::vector<std::unique_ptr<Table>> summary_override_tables;

  // Lazy-loads the index tables.
  std::vector<std::unique_ptr<Table>>& IndexTables();

 private:
  std::string path_;

  bool loaded_ = false;

  std::vector<std::string> index_table_paths_;
  std::vector<std::unique_ptr<Table>> index_tables_;
};

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_SCHEMA_H_
