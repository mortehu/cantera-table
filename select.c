/*
    SQL SELECT query processor
    Copyright (C) 2013    Morten Hustveit
    Copyright (C) 2013    eVenture Capital Partners II

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

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
          && expr->lhs->value.d.field_index == primary_key_field
          && expr->rhs->type == EXPR_CONSTANT
          && expr->rhs->value.type == CA_TEXT)
        {
          return expr->rhs->value.d.string_literal;
        }

      if (expr->rhs->type == EXPR_FIELD
          && expr->rhs->value.d.field_index == primary_key_field
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

    default:

      return NULL;
    }
}

static size_t
collect_field_values (struct iovec *output,
                      const struct ca_field *fields, size_t field_count,
                      const uint8_t *begin, const uint8_t *end)
{
  size_t field_index;

  for (field_index = 0; field_index < field_count && begin != end; ++field_index)
    {
      assert (begin < end);

      output->iov_base = (void *) begin;

      switch (fields[field_index].type)
        {
        case CA_TEXT:

          /* XXX: Too many casts */

          begin = (const uint8_t *) strchr ((const char *) begin, 0) + 1;

          break;

        case CA_TIME_FLOAT4:

          do
            {
              uint64_t start_time;
              uint32_t interval, sample_count;
              const float *sample_values;

              ca_parse_time_float4 (&begin,
                                    &start_time, &interval,
                                    &sample_values, &sample_count);
            }
          while (begin != end);

          break;

        case CA_BOOLEAN:
        case CA_INT8:
        case CA_UINT8:

          ++begin;

          break;

        case CA_INT16:
        case CA_UINT16:

          begin += 2;

          break;

        case CA_FLOAT4:
        case CA_INT32:
        case CA_UINT32:

          begin += 4;

          break;

        case CA_FLOAT8:
        case CA_INT64:
        case CA_UINT64:
        case CA_TIMESTAMPTZ:

          begin += 8;

          break;

        default:

          assert (!"unhandled data type");
        }

      output->iov_len = begin - (const uint8_t *) output->iov_base;

      ++output;
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
          expression->value.type = variable->type;
          expression->value.d.field_index = variable->field_index;

          break;

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

          if (-1 == CA_query_resolve_variables (expression->lhs, variables))
            return -1;

          break;

        case EXPR_ASTERISK:
        case EXPR_CONSTANT:

          break;

        default:

          fprintf (stderr, "Type %d not implemented\n", expression->type);

          assert (!"fix me");
        }

      expression = expression->next;
    }

  return 0;
}

void
ca_format_uint64 (char **output,
                  uint64_t number,
                  unsigned int min_width)
{
  char *begin, *o;

  begin = o = *output;

  while (number > 0)
    {
      *o++ = '0' + (number % 10);
      number /= 10;
    }

  while (o - begin < min_width)
    *o++ = '0';

  *output = o--;

  while (begin < o)
    {
      char tmp;

      tmp = *o;
      *o = *begin;
      *begin = tmp;

      --o;
      ++begin;
    }
}

const char *
ca_cast_to_text (struct ca_query_parse_context *context,
                 const struct expression_value *value)
{
  char *result = NULL;
  size_t result_alloc = 0, result_size = 0;

  switch (value->type)
    {
    case CA_BOOLEAN:

      return value->d.integer ? "TRUE" : "FALSE";

    case CA_FLOAT4:

      return ca_arena_sprintf (&context->arena, "%.7g", value->d.float4);

    case CA_FLOAT8:

      return ca_arena_sprintf (&context->arena, "%.7g", value->d.float8);

    case CA_INT8:
    case CA_INT16:
    case CA_INT32:
    case CA_INT64:

      return ca_arena_sprintf (&context->arena, "%lld", (long long) value->d.integer);

    case CA_UINT8:
    case CA_UINT16:
    case CA_UINT32:
    case CA_UINT64:

      return ca_arena_sprintf (&context->arena, "%llu", (long long) value->d.integer);

    case CA_NUMERIC:

      return value->d.numeric;

    case CA_TEXT:

      return value->d.string_literal;

    case CA_TIMESTAMPTZ:

        {
          time_t t;

          result = ca_arena_alloc (&context->arena, 24);

          t = value->d.integer;

          strftime (result, 24, "%Y-%m-%dT%H:%M:%S", gmtime (&t));
        }

      break;

    case CA_TIME_FLOAT4:

        {
          const uint8_t *begin, *end;
          size_t i;
          int first = 1;

          begin = value->d.iov.iov_base;
          end = begin + value->d.iov.iov_len;

          assert (value->d.iov.iov_len > 0);

          while (begin != end)
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
                  struct tm tm;
                  char *o;

                  /*   13 bytes %.7g formatted -FLT_MAX
                   * + 19 bytes for ISO 8601 date/time
                   * + 1 byte for LF
                   * + 1 byte for TAB
                   */
                  if (result_size + 34 >= result_alloc
                      && -1 == CA_ARRAY_GROW_N (&result, &result_alloc, 34))
                    return NULL;

                  if (!first)
                    result[result_size++] = '\n';

                  time = start_time + i * interval;

                  gmtime_r (&time, &tm);

                  o = result + result_size;

                  if (!context->time_format[0])
                    {
                      ca_format_uint64 (&o, tm.tm_year + 1900, 4);
                      *o++ = '-';
                      ca_format_uint64 (&o, tm.tm_mon + 1, 2);
                      *o++ = '-';
                      ca_format_uint64 (&o, tm.tm_mday, 2);
                      *o++ = 'T';
                      ca_format_uint64 (&o, tm.tm_hour, 2);
                      *o++ = ':';
                      ca_format_uint64 (&o, tm.tm_min, 2);
                      *o++ = ':';
                      ca_format_uint64 (&o, tm.tm_sec, 2);
                    }
                  else
                    {
                      /* XXX Support varying length date/time */

                      strftime (o, 20, context->time_format, &tm);
                      o = strchr (o, 0);
                    }

                  *o++ = '\t';

                  o += sprintf (o, "%.7g", sample_values[i]);

                  result_size = o - result;

                  first = 0;
                }
            }

          if (result_size == result_alloc
              && -1 == CA_ARRAY_GROW_N (&result, &result_alloc, 1))
            return NULL;

          result[result_size] = 0;

          if (-1 == ca_arena_add_pointer (&context->arena, result))
            {
              free (result);
              result = NULL;
            }
        }

      break;

    default:

      ca_set_error ("Unsupported type %d", value->type);

      break;
    }

  return result;
}

const char *
ca_cast_to_json (struct ca_query_parse_context *context,
                 const struct expression_value *value)
{
  char *result = NULL;
  size_t result_alloc = 0, result_size = 0;

  switch (value->type)
    {
    case CA_BOOLEAN:

      return value->d.integer ? "true" : "false";

    case CA_FLOAT4:

      return ca_arena_sprintf (&context->arena, "%.7g", value->d.float4);

    case CA_FLOAT8:

      return ca_arena_sprintf (&context->arena, "%.7g", value->d.float8);

    case CA_INT8:
    case CA_INT16:
    case CA_INT32:
    case CA_INT64:

      return ca_arena_sprintf (&context->arena, "%lld", (long long) value->d.integer);

    case CA_UINT8:
    case CA_UINT16:
    case CA_UINT32:
    case CA_UINT64:

      return ca_arena_sprintf (&context->arena, "%llu", (long long) value->d.integer);

    case CA_NUMERIC:

      return value->d.numeric;

    case CA_TEXT:

      /* XXX: Escape special characters */
      return ca_arena_sprintf (&context->arena, "\"%s\"", value->d.string_literal);

    case CA_TIMESTAMPTZ:

        {
          time_t t;

          result = ca_arena_alloc (&context->arena, 22);

          t = value->d.integer;

          strftime (result, 22, "\"%Y-%m-%dT%H:%M:%S\"", gmtime (&t));
        }

      break;

    case CA_TIME_FLOAT4:

        {
          const uint8_t *begin, *end;
          size_t i;
          int first = 1;

          begin = value->d.iov.iov_base;
          end = begin + value->d.iov.iov_len;

          assert (value->d.iov.iov_len > 0);

          if (-1 == CA_ARRAY_GROW_N (&result, &result_alloc, 2))
            {
              free (result);

              return NULL;
            }

          result[result_size++] = '[';

          while (begin != end)
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
                  struct tm tm;
                  char *o;

                  if (result_size + 128 >= result_alloc
                      && -1 == CA_ARRAY_GROW_N (&result, &result_alloc, 128))
                    return NULL;

                  if (!first)
                    result[result_size++] = ',';

                  time = start_time + i * interval;

                  gmtime_r (&time, &tm);

                  o = result + result_size;

                  /* XXX: Check for overflow */

                  o += sprintf (o, "{\"time\":\"");
                  o += strftime (o, result + result_alloc - o, context->time_format, &tm);
                  o += sprintf (o, "\",\"value\":%.7g}", sample_values[i]);

                  result_size = o - result;

                  first = 0;
                }
            }

          if (result_size == result_alloc
              && -1 == CA_ARRAY_GROW (&result, &result_alloc))
            return NULL;

          result[result_size++] = ']';
          result[result_size++] = 0;

          if (-1 == ca_arena_add_pointer (&context->arena, result))
            {
              free (result);
              result = NULL;
            }
        }

      break;

    default:

      ca_set_error ("Unsupported type %d", value->type);

      break;
    }

  return result;
}

int
ca_compare_equal (struct expression_value *result,
                  const struct expression_value *lhs,
                  const struct expression_value *rhs)
{
  result->type = CA_BOOLEAN;

  if (lhs->type != rhs->type)
    {
      ca_set_error ("Attempt to compare values of different types");

      return -1;
    }

  switch (lhs->type)
    {
    case CA_TEXT:

      result->d.integer = !strcmp (lhs->d.string_literal, rhs->d.string_literal);

      return 0;

    default:

      ca_set_error ("Comparing values of type %d is not yet supported", lhs->type);

      return -1;
    }
}

int
ca_compare_like (struct expression_value *result,
                 const struct expression_value *lhs,
                 const struct expression_value *rhs)
{
  const char *haystack, *filter;

  result->type = CA_BOOLEAN;

  if (lhs->type != CA_TEXT || rhs->type != CA_TEXT)
    {
      ca_set_error ("Both operands to LIKE must be of type TEXT");

      return -1;
    }

  haystack = lhs->d.string_literal;
  filter = rhs->d.string_literal;

  result->d.integer = 0;

  /* Process everything up to the first '%' */
  while (*haystack && *filter)
    {
      if (*filter == '%')
        break;

      if (*haystack != *filter && *filter != '_')
        return 0;

      ++haystack;
      ++filter;
    }

  if (!*filter)
    {
      result->d.integer = !*haystack;

      return 0;
    }

  if (filter[0] == '%' && !filter[1])
    {
      result->d.integer = 1;

      return 0;
    }

  ca_set_error ("LIKE expression is too complex");

  return -1;
}

static int
output_plain (const char *field_name, const char *value,
              uint32_t field_index, uint32_t field_count)
{
  if (field_index)
    putchar ('\t');

  fwrite (value, 1, strlen (value), stdout);

  return 0;
}

static int
output_csv (const char *field_name, const char *value,
            uint32_t field_index, uint32_t field_count)
{
  if (field_index)
    putchar ('\t');

  for (; *value; ++value)
    {
      switch (*value)
        {
        case '\t':

          putchar ('\\');
          putchar ('t');

          break;

        case '\r':

          putchar ('\\');
          putchar ('r');

          break;

        case '\n':

          putchar ('\\');
          putchar ('n');

          break;

        default:

          putchar (*value);
        }
    }

  return 0;
}

static int
output_json (const char *field_name, const char *value,
             uint32_t field_index, uint32_t field_count)
{
  if (!field_index)
    putchar ('{');
  else
    putchar (',');

  printf ("\"%s\":", field_name);

  fwrite (value, 1, strlen (value), stdout);

  if (field_index + 1 == field_count)
    putchar ('}');

  return 0;
}

int
CA_select (struct ca_query_parse_context *context, struct select_statement *stmt)
{
  ca_output_function output_function;

  struct ca_schema *schema;
  struct ca_table *table;
  struct ca_table_declaration *declaration;

  struct iovec *field_values = NULL;

  struct select_variable *first_variable = NULL;
  struct select_variable **last_variable = &first_variable;
  struct ca_hashmap *variables = NULL;

  struct select_item *si;

  ca_expression_function output = NULL, where = NULL;

  size_t i;

  int ret, result = -1;

  unsigned int primary_key_index = 0;
  int has_unique_primary_key = -1;

  struct iovec value;

  struct ca_arena_info arena;

  ca_clear_error ();

  ca_arena_init (&arena);

  schema = context->schema;

  if (!(table = ca_schema_table (schema, stmt->from, &declaration)))
    goto done;

  switch (context->output_format)
    {
    case CA_PARAM_VALUE_CSV:

      output_function = output_csv;

      break;

    case CA_PARAM_VALUE_JSON:

      output_function = output_json;

      break;

    default:
    case CA_PARAM_VALUE_PLAIN:

      output_function = output_plain;

      break;
    }

  /*** Replace identifiers with pointers to the table field ***/

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

  if (-1 == CA_query_resolve_variables (&stmt->list->expression, variables))
    goto done;

  /*** Resolve names of columns ***/

  for (si = stmt->list; si; si = (struct select_item *) si->expression.next)
    {
      if (si->alias)
        continue;

      if (si->expression.type == EXPR_FIELD)
        si->alias = declaration->fields[si->expression.value.d.field_index].name;
      else
        si->alias = "?column?";
    }

  if (!(output = ca_expression_compile (context,
                                        "output",
                                        &stmt->list->expression,
                                        declaration->fields,
                                        declaration->field_count,
                                        output_function)))
    {
      goto done;
    }

  if (stmt->where)
    {
      if (-1 == CA_query_resolve_variables (stmt->where, variables))
        goto done;

      if (!(where = ca_expression_compile (context,
                                           "where",
                                           stmt->where,
                                           declaration->fields,
                                           declaration->field_count,
                                           NULL)))
        goto done;
    }

  if (!stmt->limit)
    {
      result = 0;

      goto done;
    }

  if (!(field_values = ca_malloc (sizeof (*field_values) * declaration->field_count)))
    goto done;

  const char *primary_key_filter;
  size_t field_index = 0;

  if (1 == has_unique_primary_key
      && NULL != (primary_key_filter = expr_primary_key_filter (stmt->where, primary_key_index)))
    {
      struct expression_value tmp_value;

      if (-1 == (ret = ca_table_seek_to_key (table, primary_key_filter)))
        goto done;

      if (!ret)
        return 0; /* Key does not exist in table */

      if (0 >= (ret = ca_table_read_row (table, &value)))
        {
          if (!ret)
            ca_set_error ("ca_table_read_row on '%s' unexpectedly returned %d",
                          stmt->from, (int) ret);

          goto done;
        }

      collect_field_values (field_values + field_index,
                            declaration->fields,
                            declaration->field_count,
                            value.iov_base,
                            (const uint8_t *) value.iov_base + value.iov_len);

      if (where)
        {
          if (-1 == where (&tmp_value, context, field_values))
            goto done;

          if (tmp_value.type != CA_BOOLEAN)
            {
              ca_set_error ("WHERE expression is not boolean");

              goto done;
            }
        }

      if (!where || tmp_value.d.integer)
        {
          if (-1 == output (&tmp_value, context, field_values))
            goto done;

          putchar ('\n');
        }
    }
  else
    {
      if (-1 == (ret = ca_table_seek (table, 0, SEEK_SET)))
        goto done;

      while (0 < (ret = ca_table_read_row (table, &value)))
        {
          struct expression_value tmp_value;

          field_index = 0;

          collect_field_values (field_values,
                                declaration->fields,
                                declaration->field_count,
                                value.iov_base,
                                (const uint8_t *) value.iov_base + value.iov_len);

          if (where)
            {
              if (-1 == where (&tmp_value, context, field_values))
                goto done;

              if (tmp_value.type != CA_BOOLEAN)
                {
                  ca_set_error ("WHERE expression is not boolean");

                  goto done;
                }

              if (!tmp_value.d.integer)
                continue;
            }

          if (stmt->offset > 0 && stmt->offset--)
            continue;

          if (-1 == output (&tmp_value, context, field_values))
            goto done;

          putchar ('\n');

          if (stmt->limit > 0 && !--stmt->limit)
            break;

          ca_arena_reset (&arena);
        }

      if (ret == -1)
        goto done;
    }

  result = 0;

done:

  ca_arena_free (&arena);

  free (field_values);
  ca_hashmap_free (variables);

  return result;
}
