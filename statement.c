#include <string.h>

#include "ca-table.h"
#include "query.h"

void
CA_process_statement (struct ca_query_parse_context *context,
                      struct statement *stmt)
{
  switch (stmt->type)
    {
    case CA_SQL_BEGIN:

      break;

    case CA_SQL_COMMIT:

      if (-1 == ca_lock_release ())
        context->error = 1;

      break;

    case CA_SQL_CREATE_TABLE:

      if (-1 == ca_schema_create_table (context->schema, stmt->u.create_table.name, &stmt->u.create_table.declaration))
        context->error = 1;

      break;

    case CA_SQL_DROP_TABLE:

      if (-1 == ca_schema_drop_table (context->schema, stmt->u.drop_table.name))
        context->error = 1;

      break;

    case CA_SQL_INSERT:

      ca_set_error ("INSERT is not yet supported");
      context->error = 1;

      break;

    case CA_SQL_LOCK:

      ca_lock_grab_global ();

      break;

    case CA_SQL_SELECT:

      if (-1 == CA_select (context, &stmt->u.select))
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
                                 stmt->u.query.index_table_name,
                                 stmt->u.query.summary_table_name,
                                 stmt->u.query.limit))
        context->error = 1;

      break;
    }
}
