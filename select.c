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

      if (expr->lhs->type == EXPR_VARIABLE
          && (expr->lhs->value.d.variable->field->flags & CA_FIELD_PRIMARY_KEY)
          && expr->rhs->type == EXPR_CONSTANT
          && expr->rhs->value.type == CA_TEXT)
        {
          return expr->rhs->value.d.string_literal;
        }

      if (expr->rhs->type == EXPR_VARIABLE
          && (expr->rhs->value.d.variable->field->flags & CA_FIELD_PRIMARY_KEY)
          && expr->lhs->type == EXPR_CONSTANT
          && expr->lhs->value.type == CA_TEXT)
        {
          return expr->lhs->value.d.string_literal;
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
format_output (enum ca_type type,
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

    case CA_TABLE_DECLARATION:

        {
          struct ca_table_declaration decl;

          ca_data_parse_table_declaration (&begin, &decl);

          putchar ('(');

          for (i = 0; i < decl.field_count; ++i)
            {
              if (i)
                printf (", ");

              printf ("%s %s", decl.fields[i].name, ca_type_to_string (decl.fields[i].type));
            }

          putchar (')');

          printf (" WITH (PATH = '%s')", decl.path);
        }

      break;

    default:

      assert (!"unhandled data type");
    }
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

          expression->type = EXPR_VARIABLE;
          expression->value.d.variable = variable;

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
      new_variable->field = &declaration->fields[i];
      new_variable->value.type = declaration->fields[i].type;

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

      format_output (declaration->fields[1].type,
                     value.iov_base,
                     (const uint8_t *) value.iov_base + value.iov_len);
    }
  else
    {
      const char *key;
      struct iovec value;

      while (1 == (ret = ca_table_read_row (table, &key, &value)))
        {
          printf ("%s\t", key);

          format_output (declaration->fields[1].type,
                         value.iov_base,
                         (const uint8_t *) value.iov_base + value.iov_len);

          printf ("\n");
        }

      if (ret == -1)
        goto done;
    }

  result = 0;

done:

  ca_hashmap_free (variables);

  return result;
}
