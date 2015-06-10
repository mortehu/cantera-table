#include <string.h>

#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/error.h"
#include "storage/ca-table/query.h"
#include "storage/ca-table/select.h"

void CA_process_statement(struct ca_query_parse_context* context,
                          struct statement* stmt) {
  /* Execute the statement itself */

  switch (stmt->type) {
    case kStatementSelect:
      ca_table::Select(context->schema, stmt->u.select);
      break;

    case kStatementSet:
      switch (stmt->u.set.parameter) {
        case CA_PARAM_OUTPUT_FORMAT:
          CA_output_format = stmt->u.set.v.enum_value;
          break;

        case CA_PARAM_TIME_FORMAT:
          if (strlen(stmt->u.set.v.string_value) + 1 > sizeof(CA_time_format)) {
            ca_set_error("TIME FORMAT string too long");
            context->error = 1;

            break;
          }

          strcpy(CA_time_format, stmt->u.set.v.string_value);

          break;
      }
      break;

    case kStatementQuery:
      if (-1 == ca_schema_query(context->schema, stmt->u.query))
        context->error = 1;
      break;

    case kStatementCorrelate:
      ca_schema_query_correlate(context->schema,
                                stmt->u.query_correlate.query_A,
                                stmt->u.query_correlate.query_B);
      break;
  }
}
