#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "ca-table.h"
#include "memory.h"
#include "schema.h"
#include "smalltable-internal.h"

struct schema_table
{
  char *name;
  struct ca_table_declaration declaration;

  struct table *handle;
};

static struct table *schema_table;

/* XXX: Switch to hashmap */

static struct schema_table *tables;
static size_t table_alloc, table_count;

void
schema_value_callback (const struct ca_data *data, void *opaque)
{
  const char *key = opaque;

  if (data->type != CA_TABLE_DECLARATION)
    return;

  if (table_count == table_alloc)
    ARRAY_GROW (&tables, &table_alloc);

  memset (&tables[table_count], 0, sizeof (*tables));

  tables[table_count].name = safe_strdup (key);
  tables[table_count].declaration = data->v.table_declaration;
  ++table_count;
}

static void
schema_row_callback (const char *key, const void *value, size_t value_size,
                     void *opaque)
{
  ca_data_iterate (value, value_size,
                   schema_value_callback, (void *) key);
}

int
ca_schema_load (const char *path)
{
  if (!(schema_table = table_open (path)))
    {
      if (errno != ENOENT)
        return -1;

      if (!(schema_table = table_create (path)))
        return -1;

      return 0;
    }

  table_iterate (schema_table, schema_row_callback, TABLE_ORDER_PHYSICAL,
                 NULL);

  return 0;
}

void
ca_schema_close (void)
{
  size_t i;

  assert (schema_table);

  for (i = 0; i < table_count; ++i)
    free (tables[i].name);

  table_count = 0;

  table_close (schema_table);
}

static void
CA_schema_save (void)
{
  struct table *new_schema;
  size_t i;

  new_schema = table_create (schema_table->path);

  for (i = 0; i < table_count; ++i)
    {
      table_write_table_declaration (new_schema, tables[i].name,
                                     &tables[i].declaration);
    }

  table_close (schema_table);
  schema_table = new_schema;
}

void
ca_schema_add_table (const char *name,
                     struct ca_table_declaration *declaration)
{
  if (table_count == table_alloc)
    ARRAY_GROW (&tables, &table_alloc);

  tables[table_count].name = safe_strdup (name);
  tables[table_count].declaration.path = safe_strdup (declaration->path);
  tables[table_count].declaration.field_count = declaration->field_count;
  tables[table_count].declaration.fields = safe_memdup (declaration->fields, sizeof (struct ca_field) * declaration->field_count);

  ++table_count;

  CA_schema_save ();
}

void
ca_schema_show_tables (void)
{
  size_t i;

  if (!table_count)
    {
      fprintf (stderr, "No tables\n");

      return;
    }

  for (i = 0; i < table_count; ++i)
    printf ("%s\t%s\n", tables[i].name, tables[i].declaration.path);
}

struct table *
ca_schema_table_with_name (const char *name)
{
  size_t i;

  for (i = 0; i < table_count; ++i)
    {
      if (strcmp (tables[i].name, name))
        continue;

      if (!tables[i].handle)
        tables[i].handle = table_open (tables[i].declaration.path);

      return tables[i].handle;
    }

  return NULL;
}
