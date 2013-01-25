#include "ca-table.h"

int
ca_schema_query (struct ca_schema *schema, const char *query,
                 const char *index_table_name,
                 const char *summary_table_name)
{
  struct ca_table *index_table;
  struct ca_table_declaration *index_declaration;

  struct ca_table *summary_table;
  struct ca_table_declaration *summary_declaration;

  int result = -1;

  ca_clear_error ();

  if (!(index_table = ca_schema_table (schema, index_table_name, &index_declaration)))
    goto done;

  if (!(summary_table = ca_schema_table (schema, summary_table_name, &summary_declaration)))
    goto done;

  if (index_declaration->field_count != 2)
    {
      ca_set_error ("Incorrect field count in index table");

      goto done;
    }

  if (summary_declaration->field_count != 2)
    {
      ca_set_error ("Incorrect field count in summary table");

      goto done;
    }

  if (index_declaration->fields[0].type != CA_TEXT)
    {
      ca_set_error ("First field in index table must be text");

      goto done;
    }

  if (index_declaration->fields[1].type != CA_SORTED_UINT)
    {
      ca_set_error ("Second field in index table must be SORTED_UINT, is %s", ca_type_to_string (index_declaration->fields[1].type));

      goto done;
    }

  if (summary_declaration->fields[1].type != CA_TEXT)
    {
      ca_set_error ("Second field in summary table must be text");

      goto done;
    }

  fprintf (stderr, "Hello\n");

  result = 0;

done:

  return result;
}
