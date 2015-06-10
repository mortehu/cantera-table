#ifndef STORAGE_CA_TABLE_SELECT_H_
#define STORAGE_CA_TABLE_SELECT_H_ 1

struct ca_schema;
struct select_statement;

namespace ca_table {

void Select(struct ca_schema* schema, const struct select_statement& select);

}  // namesapce ca_table

#endif  // !STORAGE_CA_TABLE_SELECT_H_
