#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "error.h"
#include "schema.h"
#include "query.h"

static const char *
expr_oid_filter (struct expression *expr)
{
  if (!expr)
    return NULL;

  switch (expr->type)
    {
    case EXPR_EQUAL:

      if (expr->lhs->type == EXPR_OID
          && expr->rhs->type == EXPR_STRING_LITERAL)
        {
          return expr->rhs->d.string_literal;
        }

      if (expr->rhs->type == EXPR_OID
          && expr->lhs->type == EXPR_STRING_LITERAL)
        {
          return expr->lhs->d.string_literal;
        }

      return NULL;

    case EXPR_AND:

        {
          const char *lhs, *rhs;

          lhs = expr_oid_filter (expr->lhs);
          rhs = expr_oid_filter (expr->rhs);

          if (lhs)
            {
              if (rhs && strcmp (lhs, rhs))
                return NULL;

              return lhs;
            }

          return rhs;
        }

    case EXPR_OR:

        {
          const char *lhs, *rhs;

          lhs = expr_oid_filter (expr->lhs);
          rhs = expr_oid_filter (expr->rhs);

          if (lhs && rhs && !strcmp (lhs, rhs))
            return lhs;

          return NULL;
        }

      break;

    case EXPR_PARENTHESIS:

      return expr_oid_filter (expr->lhs);

    default:

      return NULL;
    }
}

static void
format_output (const struct ca_data *data, void *opaque)
{
  size_t i;

  switch (data->type)
    {
    case CA_TIME_SERIES:

      for (i = 0; i < data->v.time_series.count; ++i)
        {
          printf ("%llu\t%.7g\n",
                  (unsigned long long) data->v.time_series.start_time + i * data->v.time_series.interval,
                  data->v.time_series.values[i]);
        }

      break;

    default:

      assert (!"unhandled data type");
    }
}

void
CA_select (struct select_statement *stmt)
{
  struct table *t;
  const char *oid_filter;

  ca_clear_error ();

  if (!(t = ca_schema_table_with_name (stmt->from)))
    fprintf (stderr, "Failed to open table '%s': %s\n", stmt->from, ca_last_error ());

  if (table_is_sorted (t)
      && NULL != (oid_filter = expr_oid_filter (stmt->where)))
    {
      const void *data;
      size_t size;

      if (!(data = table_lookup (t, oid_filter, &size)))
        return;

      ca_data_iterate (data, size,
                       format_output, stmt->list);
    }
}
