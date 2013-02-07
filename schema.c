/*
    Database structure management routines
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

#include <fcntl.h>
#include <unistd.h>

#include "ca-table.h"

struct schema_table
{
  char *name;
  char *backend;
  struct ca_table_declaration declaration;

  struct ca_table *handle;
};

struct ca_schema
{
  char *path;

  /* XXX: Switch to hashmap */

  struct schema_table *tables;
  size_t table_alloc, table_count;
};

static int
CA_schema_save (struct ca_schema *schema);

static int
CA_schema_create (struct ca_schema *schema, const char *path)
{
  int result = -1;

  if (schema->table_count == schema->table_alloc
      && -1 == CA_ARRAY_GROW (&schema->tables, &schema->table_alloc))
    goto fail;

  schema->table_count = 2;

  if (!(schema->tables[0].name = ca_strdup ("ca_catalog.ca_tables")))
    goto fail;

  if (!(schema->tables[0].backend = ca_strdup ("write-once")))
    goto fail;

  if (-1 == asprintf (&schema->tables[0].declaration.path, "%s/ca_catalog.ca_tables", path))
    goto fail;

  schema->tables[0].declaration.field_count = 3;
  if (!(schema->tables[0].declaration.fields = ca_malloc (3 * sizeof (struct ca_field))))
    goto fail;

  strcpy (schema->tables[0].declaration.fields[0].name, "table_name");
  schema->tables[0].declaration.fields[0].flags = CA_FIELD_PRIMARY_KEY | CA_FIELD_NOT_NULL;
  schema->tables[0].declaration.fields[0].type = CA_TEXT;

  strcpy (schema->tables[0].declaration.fields[1].name, "path");
  schema->tables[0].declaration.fields[1].flags = CA_FIELD_NOT_NULL;
  schema->tables[0].declaration.fields[1].type = CA_TEXT;

  strcpy (schema->tables[0].declaration.fields[2].name, "backend");
  schema->tables[0].declaration.fields[2].flags = CA_FIELD_NOT_NULL;
  schema->tables[0].declaration.fields[2].type = CA_TEXT;

  if (!(schema->tables[1].name = ca_strdup ("ca_catalog.ca_columns")))
    goto fail;

  if (!(schema->tables[1].backend = ca_strdup ("write-once")))
    goto fail;

  if (-1 == asprintf (&schema->tables[1].declaration.path, "%s/ca_catalog.ca_columns", path))
    goto fail;

  schema->tables[1].declaration.field_count = 5;
  if (!(schema->tables[1].declaration.fields = ca_malloc (5 * sizeof (struct ca_field))))
    goto fail;

  strcpy (schema->tables[1].declaration.fields[0].name, "table_name");
  schema->tables[1].declaration.fields[0].flags = CA_FIELD_PRIMARY_KEY | CA_FIELD_NOT_NULL;
  schema->tables[1].declaration.fields[0].type = CA_TEXT;

  strcpy (schema->tables[1].declaration.fields[1].name, "column_name");
  schema->tables[1].declaration.fields[1].flags = CA_FIELD_PRIMARY_KEY | CA_FIELD_NOT_NULL;
  schema->tables[1].declaration.fields[1].type = CA_TEXT;

  strcpy (schema->tables[1].declaration.fields[2].name, "type");
  schema->tables[1].declaration.fields[2].flags = CA_FIELD_NOT_NULL;
  schema->tables[1].declaration.fields[2].type = CA_INT64;

  strcpy (schema->tables[1].declaration.fields[3].name, "not_null");
  schema->tables[1].declaration.fields[3].flags = CA_FIELD_NOT_NULL;
  schema->tables[1].declaration.fields[3].type = CA_BOOLEAN;

  strcpy (schema->tables[1].declaration.fields[4].name, "primary_key");
  schema->tables[1].declaration.fields[4].flags = CA_FIELD_NOT_NULL;
  schema->tables[1].declaration.fields[4].type = CA_BOOLEAN;

  result = 0;

fail:

  return result;
}

struct ca_schema *
ca_schema_load (const char *path)
{
  struct ca_schema *result;

  char *ca_tables_path = NULL, *ca_columns_path = NULL;
  struct ca_table *ca_tables = NULL, *ca_columns = NULL;

  int ok = 0;

  if (path[0] != '/')
    {
      ca_set_error ("Schema path must be absolute");

      return NULL;
    }

  if (path[strlen (path) - 1] == '/')
    {
      ca_set_error ("Schema path must not end with a slash (/)");

      return NULL;
    }

  if (!(result = ca_malloc (sizeof (*result))))
    return NULL;

  if (!(result->path = ca_strdup (path)))
    goto fail;

  if (-1 == asprintf (&ca_tables_path, "%s/ca_catalog.ca_tables", path))
    goto fail;

  if (-1 == asprintf (&ca_columns_path, "%s/ca_catalog.ca_columns", path))
    goto fail;

  if (!(ca_tables = ca_table_open ("write-once", ca_tables_path, O_RDONLY, 0)))
    {
      if (errno == ENOENT
          && 0 == CA_schema_create (result, path)
          && 0 == CA_schema_save (result))
        {
          free (ca_tables_path);
          free (ca_columns_path);

          return result;
        }

      goto fail;
    }

  if (!(ca_columns = ca_table_open ("write-once", ca_columns_path, O_RDONLY, 0)))
    goto fail;

  for (;;)
    {
      struct schema_table *table;
      const char *tmp;
      struct iovec value[2];
      ssize_t ret;

      if (2 != (ret = ca_table_read_row (ca_tables, value, 2)))
        {
          if (ret == 0)
            break;

          if (ret == 1)
            ca_set_error ("Got only one iovec from ca_table_read_row, but not supported in code yet");

          goto fail;
        }

      if (result->table_count == result->table_alloc
          && -1 == CA_ARRAY_GROW (&result->tables, &result->table_alloc))
        goto fail;

      table = &result->tables[result->table_count++];
      memset (table, 0, sizeof (*table));

      if (!(table->name = ca_strdup (value[0].iov_base)))
        goto fail;

      tmp = value[1].iov_base;

      if (!(table->declaration.path = ca_strdup (tmp)))
        goto fail;

      tmp = strchr (tmp, 0) + 1;

      if (!(table->backend = ca_strdup (tmp)))
        goto fail;

      tmp = strchr (tmp, 0) + 1;

      assert (tmp == (const char *) value[1].iov_base + value[1].iov_len);

      if (1 != (ret = ca_table_seek_to_key (ca_columns, table->name)))
        {
          const char *recursive_error;

          if (ret == 0)
            recursive_error = "No column was found for table";
          else
            recursive_error = ca_last_error ();

          ca_set_error ("Seek to column information row failed: %s", recursive_error);

          goto fail;
        }

      for (;;)
        {
          struct ca_field *new_fields, *field;
          int64_t type;

          if (2 != (ret = ca_table_read_row (ca_columns, value, 2)))
            {
              if (ret == 0)
                break;

              if (ret == 1)
                ca_set_error ("Got only one iovec from ca_table_read_row, but not supported in code yet");

              goto fail;
            }

          if (strcmp (value[0].iov_base, table->name))
            break;

          if (!(new_fields = realloc (table->declaration.fields, sizeof (*table->declaration.fields) * ++table->declaration.field_count)))
            goto fail;

          table->declaration.fields = new_fields;
          field = &table->declaration.fields[table->declaration.field_count - 1];
          memset (field, 0, sizeof (*field));

          tmp = value[1].iov_base;

          assert (strlen (tmp) + 1 < sizeof (field->name));

          strcpy (field->name, tmp);

          tmp = strchr (tmp, 0) + 1;

          memcpy (&type, tmp, sizeof (type));
          tmp += sizeof (type);

          if (*tmp++)
            field->flags |= CA_FIELD_NOT_NULL;

          if (*tmp++)
            field->flags |= CA_FIELD_PRIMARY_KEY;

          assert (tmp == (const char *) value[1].iov_base + value[1].iov_len);

          field->type = type;
        }

      if (!table->declaration.field_count)
        {
          ca_set_error ("Table '%s' has no fields", table->name);

          goto fail;
        }
    }

  ok = 1;

fail:

  if (!ok)
    {
      ca_schema_close (result);
      result = NULL;
    }

  free (ca_tables_path);
  free (ca_columns_path);

  ca_table_close (ca_columns);
  ca_table_close (ca_tables);

  return result;
}

void
ca_schema_close (struct ca_schema *schema)
{
  size_t i;

  for (i = 0; i < schema->table_count; ++i)
    {
      free (schema->tables[i].declaration.path);
      free (schema->tables[i].declaration.fields);
      free (schema->tables[i].backend);
      free (schema->tables[i].name);
    }

  free (schema->tables);
  free (schema->path);
  free (schema);
}

static void
iov_from_string (struct iovec *target, const char *string)
{
  target->iov_base = (void *) string;
  target->iov_len = strlen (string) + 1;
}

static int
CA_schema_save (struct ca_schema *schema)
{
  struct ca_table *ca_tables, *ca_columns;
  size_t i, j;
  int result = -1;

  struct iovec iov[5];

  assert (schema->table_count >= 2);
  assert (!strcmp (schema->tables[0].name, "ca_catalog.ca_tables"));
  assert (!strcmp (schema->tables[1].name, "ca_catalog.ca_columns"));

  if (!(ca_tables = ca_table_open ("write-once", schema->tables[0].declaration.path, O_WRONLY | O_CREAT | O_TRUNC, 0666)))
    return -1;

  if (!(ca_columns = ca_table_open ("write-once", schema->tables[1].declaration.path, O_WRONLY | O_CREAT | O_TRUNC, 0666)))
    return -1;

  for (i = 0; i < schema->table_count; ++i)
    {
      const struct ca_table_declaration *decl;

      decl = &schema->tables[i].declaration;

      iov_from_string (&iov[0], schema->tables[i].name);
      iov_from_string (&iov[1], decl->path);
      iov_from_string (&iov[2], schema->tables[i].backend);

      if (-1 == ca_table_insert_row (ca_tables, iov, 3))
        goto done;

      for (j = 0; j < decl->field_count; ++j)
        {
          int64_t type;
          uint8_t flag_null, flag_primary_key;

          type = decl->fields[j].type;
          flag_null = 0 != (decl->fields[j].flags & CA_FIELD_NOT_NULL);
          flag_primary_key = 0 != (decl->fields[j].flags & CA_FIELD_PRIMARY_KEY);

          iov_from_string (&iov[0], schema->tables[i].name);
          iov_from_string (&iov[1], decl->fields[j].name);

          iov[2].iov_base = &type;
          iov[2].iov_len  = sizeof (type);

          iov[3].iov_base = &flag_null;
          iov[3].iov_len  = sizeof (flag_null);

          iov[4].iov_base = &flag_primary_key;
          iov[4].iov_len  = sizeof (flag_primary_key);

          if (-1 == ca_table_insert_row (ca_columns, iov, 5))
            goto done;
        }
    }

  /* XXX: We need journalling to ensure atomic replace of both ca_tables and
   *      ca_columns */

  if (-1 == ca_table_sync (ca_columns))
    goto done;

  if (-1 == ca_table_sync (ca_tables))
    goto done;

  result = 0;

done:

  ca_table_close (ca_columns);
  ca_table_close (ca_tables);

  return result;
}

int
ca_schema_create_table (struct ca_schema *schema, const char *name,
                        struct ca_table_declaration *declaration)
{
  struct schema_table *table;

  if (!declaration->field_count)
    {
      ca_set_error ("Table must have at least one column");

      return -1;
    }

  if (schema->table_count == schema->table_alloc
      && -1 == CA_ARRAY_GROW (&schema->tables, &schema->table_alloc))
    return -1;

  table = &schema->tables[schema->table_count];

  memset (table, 0, sizeof (*table));

  if (!(table->name = ca_strdup (name)))
    goto fail;

  if (!(table->declaration.path = ca_strdup (declaration->path)))
    goto fail;

  if (!(table->backend = ca_strdup ("write-once")))
    goto fail;

  table->declaration.field_count = declaration->field_count;

  if (!(table->declaration.fields = ca_memdup (declaration->fields, sizeof (struct ca_field) * declaration->field_count)))
    goto fail;

  ++schema->table_count;

  /* XXX: Defer until COMMIT */

  if (-1 == CA_schema_save (schema))
    {
      --schema->table_count;

      goto fail;
    }

  return 0;

fail:

  free (table->name);
  free (table->declaration.path);
  free (table->declaration.fields);

  return -1;
}

struct ca_table *
ca_schema_table (struct ca_schema *schema, const char *table_name,
                 struct ca_table_declaration **declaration)
{
  struct ca_table *result;
  size_t i;

  for (i = 0; i < schema->table_count; ++i)
    {
      if (strcmp (schema->tables[i].name, table_name))
        continue;

      if (!schema->tables[i].handle)
        {
          /* XXX: O_RDONLY only applies to WORM tables */
          result = ca_table_open (schema->tables[i].backend, schema->tables[i].declaration.path, O_RDONLY, 0);

          /* XXX: Table should be created by ca_schema_create_table() */
          if (!result)
            {
              if (errno != ENOENT)
                return NULL;

              if (!(result = ca_table_open (schema->tables[i].backend, schema->tables[i].declaration.path, O_CREAT | O_TRUNC | O_RDWR, 0666)))
                return NULL;

              if (-1 == ca_table_sync (result))
                return NULL;
            }

          schema->tables[i].handle = result;
        }

      if (declaration)
        *declaration = &schema->tables[i].declaration;

      return schema->tables[i].handle;
    }

  ca_set_error ("Table '%s' does not exist", table_name);

  return NULL;
}
