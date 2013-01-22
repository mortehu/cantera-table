#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ca-table.h"
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
format_output (enum ca_value_type type,
               const uint8_t *begin, const uint8_t *end)
{
  size_t i;

  switch (type)
    {
    case CA_TEXT:

      fwrite (begin, 1, end - begin, stdout);

      break;

    case CA_TIME_SERIES:

      while (begin != end)
        {
          uint64_t start_time;
          uint32_t interval, sample_count;
          const float *sample_values;

          ca_data_parse_time_float4 (&begin,
                                     &start_time, &interval,
                                     &sample_values, &sample_count);

          for (i = 0; i < sample_count; ++i)
            {
              printf ("%llu\t%.7g\n",
                      (unsigned long long) start_time + i * interval,
                      sample_values[i]);
            }
        }

      break;

    default:

      assert (!"unhandled data type");
    }
}

void
CA_select (struct ca_schema *schema, struct select_statement *stmt)
{
  struct ca_table *table;
  const char *oid_filter;
  int ret;

  ca_clear_error ();

  if (!(table = ca_schema_table (schema, stmt->from)))
    fprintf (stderr, "Failed to open table '%s': %s\n", stmt->from, ca_last_error ());

  if (NULL != (oid_filter = expr_oid_filter (stmt->where)))
    {
      const char *key;
      struct iovec value;

      if (-1 == (ret = ca_table_seek_to_key (table, oid_filter)))
        {
          fprintf (stderr, "Could not look up '%s' in '%s': %s\n", oid_filter, stmt->from, ca_last_error ());

          return;
        }

      if (!ret)
        return; /* Key does not exist in table */

      if (1 != (ret = ca_table_read_row (table, &key, &value)))
        {
          if (ret < 0)
            fprintf (stderr, "Failed to read row data from '%s': %s\n", stmt->from, ca_last_error ());
          else if (!ret) /* Key both exists and does not exist? */
            fprintf (stderr, "ca_table_read_row on '%s' unexpectedly returned 0\n", stmt->from);

          return;
        }

      format_output (CA_TIME_SERIES,
                     value.iov_base,
                     (const uint8_t *) value.iov_base + value.iov_len);
    }
}
