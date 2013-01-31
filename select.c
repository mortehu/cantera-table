#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "ca-table.h"
#include "query.h"

static const char *
expr_primary_key_filter (struct expression *expr, unsigned int primary_key_field)
{
  if (!expr)
    return NULL;

  switch (expr->type)
    {
    case EXPR_EQUAL:

      if (expr->lhs->type == EXPR_FIELD
          && expr->lhs->value.d.field.index == primary_key_field
          && expr->rhs->type == EXPR_CONSTANT
          && expr->rhs->value.type == CA_TEXT)
        {
          return expr->rhs->value.d.string_literal;
        }

      if (expr->rhs->type == EXPR_FIELD
          && expr->rhs->value.d.field.index == primary_key_field
          && expr->lhs->type == EXPR_CONSTANT
          && expr->lhs->value.type == CA_TEXT)
        {
          return expr->lhs->value.d.string_literal;
        }

      return NULL;

    case EXPR_AND:

        {
          const char *lhs, *rhs;

          lhs = expr_primary_key_filter (expr->lhs, primary_key_field);
          rhs = expr_primary_key_filter (expr->rhs, primary_key_field);

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

          lhs = expr_primary_key_filter (expr->lhs, primary_key_field);
          rhs = expr_primary_key_filter (expr->rhs, primary_key_field);

          if (lhs && rhs && !strcmp (lhs, rhs))
            return lhs;

          return NULL;
        }

      break;

    case EXPR_PARENTHESIS:

      return expr_primary_key_filter (expr->lhs, primary_key_field);

    default:

      return NULL;
    }
}

static size_t
format_output (const struct ca_field *fields, size_t field_count,
               const uint8_t *begin, const uint8_t *end)
{
  size_t i, field_index;

  for (field_index = 0; field_index < field_count && begin != end; ++field_index)
    {
      assert (begin < end);

      if (field_index)
        putchar ('\t');

      switch (fields[field_index].type)
        {
        case CA_TEXT:

            {
              const char *str_end;

              /* XXX: Too many casts */

              str_end = strchr ((const char *) begin, 0);

              fwrite (begin, 1, str_end - (const char *) begin, stdout);

              begin = (const uint8_t *) str_end + 1;
            }

          break;

        case CA_TIME_FLOAT4:

            {
              uint64_t start_time;
              uint32_t interval, sample_count;
              const float *sample_values;

              ca_parse_time_float4 (&begin,
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

        case CA_INT64:

            {
              int64_t buffer;

              memcpy (&buffer, begin, sizeof (buffer));
              begin += sizeof (buffer);

              printf ("%lld", (long long int) buffer);
            }

          break;

        case CA_BOOLEAN:

          switch (*begin++)
            {
            case 0:  printf ("FALSE"); break;
            default: printf ("TRUE"); break;
            }

          break;

        default:

          assert (!"unhandled data type");
        }
    }

  assert (begin == end);

  return field_index;
}

static int
CA_query_resolve_variables (struct expression *expression,
                            const struct ca_hashmap *variables)
{
  struct select_variable *variable;

  while (expression)
    {
      switch (expression->type)
        {
        case EXPR_IDENTIFIER:

          if (!(variable = ca_hashmap_get (variables, expression->value.d.identifier)))
            {
              ca_set_error ("Unknown field name '%s'", expression->value.d.identifier);

              return -1;
            }

          expression->type = EXPR_FIELD;
          expression->value.d.field.index = variable->field_index;
          expression->value.d.field.type = variable->type;

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

          if (-1 == CA_query_resolve_variables (expression->rhs, variables))
            return -1;

          /* Fall through */

        case EXPR_DISTINCT:
        case EXPR_NEGATIVE:
        case EXPR_PARENTHESIS:

          expression = expression->lhs;

          continue;

        case EXPR_CONSTANT:

          return 0;

        default:

          fprintf (stderr, "Type %d not implemented\n", expression->value.type);

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

  struct select_variable *first_variable = NULL;
  struct select_variable **last_variable = &first_variable;
  struct ca_hashmap *variables = NULL;

  size_t i;
  int ret, result = -1;

  unsigned int primary_key_index = 0;
  int has_unique_primary_key = -1;

  struct iovec value[2];

  ca_clear_error ();

  if (!(table = ca_schema_table (schema, stmt->from, &declaration)))
    goto done;

  variables = ca_hashmap_create (63);

  for (i = 0; i < declaration->field_count; ++i)
    {
      struct select_variable *new_variable;

      /* XXX: Use arena allocator */

      new_variable = calloc (1, sizeof (*new_variable));
      new_variable->name = declaration->fields[i].name;
      new_variable->field_index = i;
      new_variable->type = declaration->fields[i].type;

      if (declaration->fields[i].flags & CA_FIELD_PRIMARY_KEY)
        {
          if (has_unique_primary_key == -1)
            {
              has_unique_primary_key = 1;
              primary_key_index = i;
            }
          else
            has_unique_primary_key = 0;
        }

      if (-1 == ca_hashmap_insert (variables, declaration->fields[i].name, new_variable))
        goto done;

      *last_variable = new_variable;
      last_variable = &new_variable->next;
    }

  if (stmt->where)
    {
      if (-1 == CA_query_resolve_variables (stmt->where, variables))
        goto done;
    }

  const char *primary_key_filter;
  size_t field_index = 0;

  if (1 == has_unique_primary_key
      && NULL != (primary_key_filter = expr_primary_key_filter (stmt->where, primary_key_index)))
    {
      if (-1 == (ret = ca_table_seek_to_key (table, primary_key_filter)))
        goto done;

      if (!ret)
        return 0; /* Key does not exist in table */

      if (0 < (ret = ca_table_read_row (table, value, 2)))
        {
          if (!ret)
            ca_set_error ("ca_table_read_row on '%s' unexpectedly returned %d",
                          stmt->from, (int) ret);

          goto done;
        }

      for (i = 0; i < ret; ++i)
        {
          if (i)
            putchar ('\t');

          field_index +=
            format_output (&declaration->fields[field_index],
                           declaration->field_count - field_index,
                           value[i].iov_base,
                           (const uint8_t *) value[i].iov_base + value[i].iov_len);
        }

      putchar ('\n');

      result = 0;
    }
  else
    {
      if (-1 == (ret = ca_table_seek (table, 0, SEEK_SET)))
        goto done;

      while (0 < (ret = ca_table_read_row (table, value, 2)))
        {
          field_index = 0;

          for (i = 0; i < ret; ++i)
            {
              if (i)
                putchar ('\t');

              field_index +=
                format_output (&declaration->fields[field_index],
                               declaration->field_count - field_index,
                               value[i].iov_base,
                               (const uint8_t *) value[i].iov_base + value[i].iov_len);
            }

          putchar ('\n');
        }

      if (ret == -1)
        goto done;
    }

  result = 0;

done:

  ca_hashmap_free (variables);

  return result;
}
