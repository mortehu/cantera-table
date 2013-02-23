/*
    SQL UPDATE query processor
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

int
CA_update (struct ca_query_parse_context *context,
           struct update_statement *stmt)
{
  struct ca_schema *schema;
  struct ca_table *table = NULL;
  struct ca_table_declaration *declaration = NULL;
  struct ca_table_declaration new_declaration;

  struct ca_field *fields = NULL;
  size_t field_count = 0;
  struct iovec *field_values = NULL;

  struct select_variable *first_variable = NULL;
  struct select_variable **last_variable = &first_variable;
  struct ca_hashmap *variables = NULL;

  ca_expression_function where = NULL;
  ca_output_function expression = NULL;
  ca_collect_function collect = NULL;

  size_t i, update_field_index;
  enum ca_type return_type;
  struct iovec tmp_iovec;
  char tmp_buffer[16];

  int ret, result = -1;

  struct iovec value;

  struct ca_arena_info arena;

  struct ca_table *output_table = NULL;

  ca_clear_error ();

  ca_arena_init (&arena);

  schema = context->schema;

  if (!(table = ca_schema_table (schema, stmt->table, &declaration)))
    goto done;

  fields = declaration->fields;
  field_count = declaration->field_count;

  collect = CA_collect_compile (fields, field_count);

  /*** Replace identifiers with pointers to the table field ***/

  variables = ca_hashmap_create (63);
  update_field_index = field_count;

  for (i = 0; i < field_count; ++i)
    {
      struct select_variable *new_variable;

      /* XXX: Use arena allocator */

      new_variable = calloc (1, sizeof (*new_variable));
      new_variable->name = fields[i].name;
      new_variable->field_index = i;
      new_variable->type = fields[i].type;

      if (-1 == ca_hashmap_insert (variables, fields[i].name, new_variable))
        goto done;

      *last_variable = new_variable;
      last_variable = &new_variable->next;

      if (!strcmp (fields[i].name, stmt->column))
        update_field_index = i;
    }

  if (update_field_index == field_count)
    {
      ca_set_error ("Field '%s' not found in table '%s'",
                    stmt->column, stmt->table);

      goto done;
    }

  if (-1 == CA_query_resolve_variables (stmt->expression, variables, NULL))
    goto done;

  if (!(expression = CA_output_compile (stmt->expression, fields, field_count, &return_type)))
    {
      ca_set_error ("Failed to compile expression: %s", ca_last_error ());

      goto done;
    }

  if (return_type != fields[update_field_index].type)
    {
      ca_set_error ("Return type (%s) does not match field type (%s)",
                    ca_type_to_string (return_type),
                    ca_type_to_string (fields[update_field_index].type));

      goto done;
    }

  if (stmt->where)
    {
      if (-1 == CA_query_resolve_variables (stmt->where, variables, NULL))
        goto done;

      if (!(where = CA_expression_compile ("where",
                                           stmt->where,
                                           fields, field_count,
                                           CA_EXPRESSION_RETURN_BOOL)))
        {
          ca_set_error ("Failed to compile WHERE expression: %s", ca_last_error ());

          goto done;
        }
    }

  if (field_count
      && !(field_values = ca_malloc (sizeof (*field_values) * field_count)))
    goto done;

  if (-1 == (ret = ca_table_seek (table, 0, SEEK_SET)))
    goto done;

  /* WORM tables: DROP old table and CREATE new one in its place */

  if (-1 == ca_schema_drop_table (schema, stmt->table))
    goto done;

  new_declaration = *declaration;
  new_declaration.path = NULL;

  if (!(output_table = ca_schema_create_table (schema, stmt->table, &new_declaration)))
    goto done;

  tmp_iovec.iov_base = tmp_buffer;
  tmp_iovec.iov_len = 0;

  while (0 < (ret = ca_table_read_row (table, &value)))
    {
      collect (field_values, value.iov_base,
               (const uint8_t *) value.iov_base + value.iov_len);

      if (where && where (context, field_values))
        {
          if (-1 == expression (&tmp_iovec, context, field_values))
            goto done;

          field_values[update_field_index] = tmp_iovec;

          if (-1 == ca_table_insert_row (output_table, field_values, field_count))
            goto done;
        }
      else
        {
          if (-1 == ca_table_insert_row (output_table, &value, 1))
            goto done;
        }

      ca_arena_reset (&arena);
    }

  if (ret == -1)
    goto done;

  if (-1 == ca_table_sync (output_table))
    goto done;

  result = 0;

done:

  ca_arena_free (&arena);

  free (field_values);
  ca_hashmap_free (variables);

  return result;
}
