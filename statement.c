#include <string.h>

#include "ca-table.h"
#include "query.h"

void
CA_process_statement (struct ca_query_parse_context *context,
                      struct statement *stmt)
{
  /* Execute the statement itself */

  switch (stmt->type)
    {
    case CA_SQL_SAMPLE:

      if (-1 == ca_schema_sample (context->schema,
                                  stmt->u.sample.key))
        context->error = 1;

      break;

    case CA_SQL_SET:

      switch (stmt->u.set.parameter)
        {
        case CA_PARAM_OUTPUT_FORMAT:

          CA_output_format = stmt->u.set.v.enum_value;

          break;

        case CA_PARAM_TIME_FORMAT:

          if (strlen (stmt->u.set.v.string_value) + 1 > sizeof (CA_time_format))
            {
              ca_set_error ("TIME FORMAT string too long");
              context->error = 1;

              break;
            }

          strcpy (CA_time_format, stmt->u.set.v.string_value);

          break;
        }

      break;

    case CA_SQL_QUERY:

      if (-1 == ca_schema_query (context->schema,
                                 stmt->u.query.query,
                                 stmt->u.query.limit))
        context->error = 1;

      break;

    case CA_SQL_QUERY_CORRELATE:

      if (-1 == ca_schema_query_correlate (context->schema,
                                           stmt->u.query_correlate.query_A,
                                           stmt->u.query_correlate.query_B))
        context->error = 1;

      break;
    }
}
