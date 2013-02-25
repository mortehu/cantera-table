#include <string.h>

#include "ca-table.h"
#include "query.h"

enum ca_param_value CA_isolation_level = CA_PARAM_VALUE_SERIALIZABLE;

void
CA_process_statement (struct ca_query_parse_context *context,
                      struct statement *stmt)
{
  int need_commit = 0;

  /* Determine whether statement needs locking and committing */

  if (!context->in_transaction_block)
    {
      switch (stmt->type)
        {
        case CA_SQL_COMMIT:
        case CA_SQL_LOCK:

          ca_set_error ("Statement requires a transaction block");
          context->error = 1;

          return;

        case CA_SQL_CREATE_TABLE:
        case CA_SQL_DROP_TABLE:
        case CA_SQL_INSERT:
        case CA_SQL_UPDATE:

          need_commit = 1;

          if (-1 == ca_lock_grab_global ())
            {
              context->error = 1;

              return;
            }

          /* Fall through */

        case CA_SQL_SELECT:
        case CA_SQL_QUERY:

          /* XXX: Allocate XID between lock and reload */

          if (-1 == ca_schema_reload (context->schema))
            {
              context->error = 1;

              return;
            }

          context->transaction_id_allocated = 1;

          break;

        case CA_SQL_BEGIN:
        case CA_SQL_SET:

          ; /* No commit needed */
        }
    }
  else /* in transaction block */
    {
      switch (stmt->type)
        {
        case CA_SQL_CREATE_TABLE:
        case CA_SQL_DROP_TABLE:
        case CA_SQL_INSERT:
        case CA_SQL_SELECT:
        case CA_SQL_UPDATE:
        case CA_SQL_QUERY:

          if (!context->transaction_id_allocated)
            {
              /* Before we first access data we need to ensure that we're
               * working on the most recent committed version of the database.
               * Currently held locks should ensure that no other transaction
               * can alter data relevant to us before our transaction block is
               * finished. */

              if (-1 == ca_schema_reload (context->schema))
                {
                  context->error = 1;

                  return;
                }

              context->transaction_id_allocated = 1;
            }

          break;

        case CA_SQL_LOCK:

          if (!context->transaction_id_allocated)
            break;

          ca_set_error ("Cannot acquire locks after data has been accessed");

          context->error = 1;

          return;

        case CA_SQL_BEGIN:

          ca_set_error ("BEGIN is not supported inside a transaction block");

          context->error = 1;

          return;

        case CA_SQL_COMMIT:
        case CA_SQL_SET:

          /* Nothing to do */

          break;
        }
    }

  /* Execute the statement itself */

  switch (stmt->type)
    {
    case CA_SQL_BEGIN:

      if (-1 == ca_lock_grab_global ())
        context->error = 1;
      else
        {
          context->in_transaction_block = 1;
          context->transaction_id_allocated = 0;
        }

      break;

    case CA_SQL_COMMIT:

      need_commit = 1;

      break;

    case CA_SQL_CREATE_TABLE:

      if (!ca_schema_create_table (context->schema, stmt->u.create_table.name, &stmt->u.create_table.declaration))
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

        case CA_PARAM_ISOLATION_LEVEL:

          CA_isolation_level = stmt->u.set.v.enum_value;

          break;
        }

      break;

    case CA_SQL_UPDATE:

      if (-1 == CA_update (context, &stmt->u.update))
        context->error = 1;

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

  if (!need_commit)
    return;

  /* Commit */

  if (-1 == ca_schema_save (context->schema)
      || -1 == ca_lock_release ())
    {
      context->error = 1;
    }

  context->in_transaction_block = 0;
}
