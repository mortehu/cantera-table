#ifndef STORAGE_CA_TABLE_SELECT_H_
#define STORAGE_CA_TABLE_SELECT_H_ 1

namespace cantera {
namespace table {

class Schema;
struct select_statement;

void SetSelectHashAlgo(bool enable);
void SetSelectParallel(int nthreads);

void Select(Schema* schema, const struct select_statement& select);

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_SELECT_H_
