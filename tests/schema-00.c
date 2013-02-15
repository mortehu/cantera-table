#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

static struct ca_schema *schema;

static int
create_table (void)
{
  struct ca_table_declaration decl;
  struct ca_field fields[2];

  memset (&decl, 0, sizeof (decl));
  memset (fields, 0, sizeof (fields));

  decl.field_count = 2;

  strcpy (fields[0].name, "bar");
  fields[0].type = CA_TEXT;
  fields[0].flags |= CA_FIELD_NOT_NULL | CA_FIELD_PRIMARY_KEY;

  strcpy (fields[0].name, "baz");
  fields[1].type = CA_TEXT;
  fields[1].flags |= CA_FIELD_NOT_NULL | CA_FIELD_PRIMARY_KEY;

  decl.fields = fields;

  return ca_schema_create_table (schema, "foo", &decl);
}

int
main (int argc, char **argv)
{
  int result = EXIT_FAILURE;
  char tmp_dir[64];


  strcpy (tmp_dir, "/tmp/ca-schema-00.tmp.XXXXXX");

  if (!(mkdtemp (tmp_dir)))
    ca_set_error ("mkdtemp failed: %s", strerror (errno));

  if (!(schema = ca_schema_load (tmp_dir)))
    goto fail;

  if (-1 == create_table ())
    goto fail;

  if (-1 == ca_schema_drop_table (schema, "foo"))
    goto fail;

  if (-1 == create_table ())
    goto fail;

  ca_schema_close (schema);

  /* XXX: Remove temporary directory */
  result = EXIT_SUCCESS;

fail:

  if (result != EXIT_SUCCESS)
    fprintf (stderr, "Error: %s\n", ca_last_error ());

  return result;
}
