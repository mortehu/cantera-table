#include <cstring>

#include "src/ca-table.h"
#include "src/query.h"
#include "src/select.h"

#include <kj/debug.h>

namespace cantera {
namespace table {

void CA_process_statement(QueryParseContext* context, Statement* stmt) {
  /* Execute the statement itself */

  switch (stmt->type) {
    case kStatementQuery:
      ca_schema_query(context->schema.get(), stmt->u.query);
      break;

    case kStatementCorrelate:
      ca_schema_query_correlate(context->schema.get(),
                                stmt->u.query_correlate.query_A,
                                stmt->u.query_correlate.query_B);
      break;

    case kStatementParse:
      PrintQuery(stmt->u.parse.query);
      printf("\n");
      break;

    case kStatementSelect:
      Select(context->schema.get(), stmt->u.select);
      break;

    case kStatementSet:
      switch (stmt->u.set.parameter) {
        case CA_PARAM_OUTPUT_FORMAT:
          CA_output_format = stmt->u.set.v.enum_value;
          break;

        case CA_PARAM_TIME_FORMAT:
          KJ_REQUIRE(std::strlen(stmt->u.set.v.string_value) + 1 <=
                     sizeof(CA_time_format));

          strcpy(CA_time_format, stmt->u.set.v.string_value);

          break;
      }
      break;
  }
}

}  // namespace table
}  // namespace cantera
