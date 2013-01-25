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
#include "memory.h"
#include "smalltable-internal.h"

struct schema_table
{
  char *name;
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

struct ca_schema *
ca_schema_load (const char *path)
{
  struct ca_schema *result;
  struct ca_table *schema_table;
  size_t i;

  if (path[0] != '/')
    {
      ca_set_error ("Schema path must be absolute");

      return NULL;
    }

  if (!(result = safe_malloc (sizeof (*result))))
    return NULL;

  if (!(result->path = safe_strdup (path)))
    return NULL;

  if (!(schema_table = ca_table_open ("write-once", path, O_RDONLY, 0)))
    {
      if (errno != ENOENT)
        goto fail;

      if (result->table_count == result->table_alloc
          && -1 == ARRAY_GROW (&result->tables, &result->table_alloc))
        goto fail;

      result->tables[0].name = safe_strdup ("ca_catalog.ca_tables");
      result->tables[0].declaration.path = safe_strdup (path);
      result->tables[0].declaration.field_count = 2;
      result->tables[0].declaration.fields = safe_malloc (2 * sizeof (struct ca_field));

      strcpy (result->tables[0].declaration.fields[0].name, "table_name");
      result->tables[0].declaration.fields[0].flags = CA_FIELD_PRIMARY_KEY | CA_FIELD_NOT_NULL;
      result->tables[0].declaration.fields[0].type = CA_TEXT;

      strcpy (result->tables[0].declaration.fields[1].name, "table_declaration");
      result->tables[0].declaration.fields[1].flags = CA_FIELD_NOT_NULL;
      result->tables[0].declaration.fields[1].type = CA_TABLE_DECLARATION;

      result->table_count = 1;

      return result;
    }

  for (;;)
    {
      const char *key;
      struct iovec value;
      ssize_t ret;

      if (1 != (ret = ca_table_read_row (schema_table, &key, &value)))
        {
          if (ret == 0)
            return result;

          goto fail;
        }

      const uint8_t *data;

      data = value.iov_base;

      if (result->table_count == result->table_alloc
          && -1 == ARRAY_GROW (&result->tables, &result->table_alloc))
        goto fail;

      struct schema_table *table;

      table = &result->tables[result->table_count++];

      memset (table, 0, sizeof (*table));

      if (!(table->name = safe_strdup (key)))
        goto fail;

      table->declaration.field_count = ca_data_parse_integer (&data);

      if (!(table->declaration.path = safe_strdup ((const char *) data)))
        goto fail;

      data += strlen ((const char *) data) + 1;

      if (!(table->declaration.fields = safe_memdup (data, sizeof (struct ca_field) * table->declaration.field_count)))
        goto fail;
    }

  ca_table_close (schema_table);

  return result;

fail:

  for (i = 0; i < result->table_count; ++i)
    {
      free (result->tables[i].declaration.path);
      free (result->tables[i].declaration.fields);
      free (result->tables[i].name);
    }

  free (result->tables);
  free (result->path);
  free (result);

  ca_table_close (schema_table);

  return NULL;
}

void
ca_schema_close (struct ca_schema *schema)
{
  size_t i;

  for (i = 0; i < schema->table_count; ++i)
    {
      free (schema->tables[i].declaration.path);
      free (schema->tables[i].declaration.fields);
    }

  free (schema->tables);
  free (schema->path);
  free (schema);
}

static int
CA_schema_save (struct ca_schema *schema)
{
  struct ca_table *new_schema;
  size_t i;
  int result = -1;

  if (!(new_schema = ca_table_open ("write-once", schema->path, O_WRONLY | O_CREAT | O_TRUNC, 0666)))
    return -1;

  for (i = 0; i < schema->table_count; ++i)
    {
      if (-1 == ca_table_write_table_declaration (new_schema,
                                                  schema->tables[i].name,
                                                  &schema->tables[i].declaration))
        {
          goto done;
        }
    }

  if (-1 == ca_table_sync (new_schema))
    goto done;

  result = 0;

done:

  ca_table_close (new_schema);

  return result;
}

int
ca_schema_create_table (struct ca_schema *schema, const char *name,
                        struct ca_table_declaration *declaration)
{
  struct schema_table *table;

  if (schema->table_count == schema->table_alloc
      && -1 == ARRAY_GROW (&schema->tables, &schema->table_alloc))
    return -1;

  table = &schema->tables[schema->table_count];

  memset (table, 0, sizeof (*table));

  if (!(table->name = safe_strdup (name)))
    goto fail;

  if (!(table->declaration.path = safe_strdup (declaration->path)))
    goto fail;

  table->declaration.field_count = declaration->field_count;

  if (!(table->declaration.fields = safe_memdup (declaration->fields, sizeof (struct ca_field) * declaration->field_count)))
    goto fail;

  ++schema->table_count;

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
          result = ca_table_open ("write-once", schema->tables[i].declaration.path, O_RDONLY, 0);

          if (!result)
            {
              if (errno != ENOENT)
                return NULL;

              if (!(result = ca_table_open ("write-once", schema->tables[i].declaration.path, O_CREAT | O_TRUNC | O_RDWR, 0666)))
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
