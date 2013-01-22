#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "ca-table.h"
#include "hashmap.h"
#include "query.h"

static const char *
expr_primary_key_filter (struct expression *expr)
{
  if (!expr)
    return NULL;

  switch (expr->type)
    {
    case EXPR_EQUAL:

      if (expr->lhs->type == EXPR_FIELD
          && (expr->lhs->d.field->flags & CA_FIELD_PRIMARY_KEY)
          && expr->rhs->type == EXPR_STRING_LITERAL)
        {
          return expr->rhs->d.string_literal;
        }

      if (expr->rhs->type == EXPR_FIELD
          && (expr->rhs->d.field->flags & CA_FIELD_PRIMARY_KEY)
          && expr->lhs->type == EXPR_STRING_LITERAL)
        {
          return expr->lhs->d.string_literal;
        }

      return NULL;

    case EXPR_AND:

        {
          const char *lhs, *rhs;

          lhs = expr_primary_key_filter (expr->lhs);
          rhs = expr_primary_key_filter (expr->rhs);

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

          lhs = expr_primary_key_filter (expr->lhs);
          rhs = expr_primary_key_filter (expr->rhs);

          if (lhs && rhs && !strcmp (lhs, rhs))
            return lhs;

          return NULL;
        }

      break;

    case EXPR_PARENTHESIS:

      return expr_primary_key_filter (expr->lhs);

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
              time_t time;
              char timebuf[32];

              time = start_time + i * interval;

              strftime (timebuf, sizeof (timebuf), "%Y-%m-%dT%H:%M:%S",
                        gmtime (&time));

              printf ("%s\t%.7g\n", timebuf, sample_values[i]);
            }
        }

      break;

    default:

      assert (!"unhandled data type");
    }
}

static int
CA_query_resolve_fields (struct expression *expression,
                         const struct ca_hashmap *fields)
{
  struct ca_field *field;

  while (expression)
    {
      switch (expression->type)
        {
        case EXPR_IDENTIFIER:

          if (!(field = ca_hashmap_get (fields, expression->d.identifier)))
            {
              ca_set_error ("Unknown field name '%s'", expression->d.identifier);

              return -1;
            }

          expression->type = EXPR_FIELD;
          expression->d.field = field;

          return 0;

        case EXPR_ADD:
        case EXPR_AND:
        case EXPR_DIV:
        case EXPR_EQUAL:
        case EXPR_GREATER_EQUAL:
        case EXPR_GREATER_THAN:
        case EXPR_LESS_EQUAL:
        case EXPR_LESS_THAN:
        case EXPR_LIKE:
        case EXPR_MUL:
        case EXPR_NOT_EQUAL:
        case EXPR_NOT_LIKE:
        case EXPR_OR:
        case EXPR_SUB:

          if (-1 == CA_query_resolve_fields (expression->rhs, fields))
            return -1;

          /* Fall through */

        case EXPR_DISTINCT:
        case EXPR_NEGATIVE:
        case EXPR_PARENTHESIS:

          expression = expression->lhs;

          continue;

        case EXPR_STRING_LITERAL:
        case EXPR_INTEGER:
        case EXPR_NUMERIC:

          return 0;

        default:

          fprintf (stderr, "Type %d not implemented\n", expression->type);

          assert (!"fix me");
        }
    }

  return 0;
}

int
CA_select (struct ca_schema *schema, struct select_statement *stmt)
{
  struct ca_table *table;
  struct ca_table_declaration *declaration;

  struct ca_hashmap *fields = NULL;

  size_t i;
  int result = -1;

  ca_clear_error ();

  if (!(table = ca_schema_table (schema, stmt->from, &declaration)))
    goto done;

  fields = ca_hashmap_create (63);

  for (i = 0; i < declaration->field_count; ++i)
    {
      if (-1 == ca_hashmap_insert (fields, declaration->fields[i].name, &declaration->fields[i])
          && errno != EEXIST)
        goto done;
    }

  if (stmt->where
      && -1 == CA_query_resolve_fields (stmt->where, fields))
    goto done;

  const char *primary_key_filter;
  int ret;

  if (NULL != (primary_key_filter = expr_primary_key_filter (stmt->where)))
    {
      struct iovec value;

      if (-1 == (ret = ca_table_seek_to_key (table, primary_key_filter)))
        goto done;

      if (!ret)
        return 0; /* Key does not exist in table */

      if (1 != (ret = ca_table_read_row (table, NULL, &value)))
        {
          if (!ret) /* Key both exists and does not exist? */
            ca_set_error ("ca_table_read_row on '%s' unexpectedly returned 0", stmt->from);

          goto done;
        }

      format_output (CA_TIME_SERIES,
                     value.iov_base,
                     (const uint8_t *) value.iov_base + value.iov_len);
    }

  result = 0;

done:

  ca_hashmap_free (fields);

  return result;
}
