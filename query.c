#include <assert.h>

#include "ca-table.h"

static size_t
CA_partition (struct ca_offset_score *data, size_t count, size_t pivot_index)
{
  size_t i, store_index;
  struct ca_offset_score pivot, tmp;

  store_index = 0;

  pivot = data[pivot_index];

  data[pivot_index] = data[count - 1];
  data[count - 1] = pivot;

  for (i = 0; i < count - 1; ++i)
    {
      if (data[i].score > pivot.score)
        {
          tmp = data[store_index];
          data[store_index] = data[i];
          data[i] = tmp;

          ++store_index;
        }
    }

  data[count - 1] = data[store_index];
  data[store_index] = pivot;

  return store_index;
}

void
ca_quicksort (struct ca_offset_score *data, size_t count)
{
  size_t pivot_index;

  while (count >= 2)
    {
      pivot_index = count / 2;

      pivot_index = CA_partition (data, count, pivot_index);

      ca_quicksort (data, pivot_index);

      data += pivot_index + 1;
      count -= pivot_index + 1;
    }
}

int
ca_schema_query (struct ca_schema *schema, const char *query,
                 const char *index_table_name,
                 const char *summary_table_name,
                 ssize_t limit)
{
  struct ca_table *index_table;
  struct ca_table_declaration *index_declaration;

  struct ca_table *summary_table;
  struct ca_table_declaration *summary_declaration;

  struct iovec data_iov;
  const uint8_t *data;

  struct ca_offset_score *offsets = NULL;
  uint32_t i, offset_count;

  ssize_t ret;
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

  if (summary_declaration->field_count != 3)
    {
      ca_set_error ("Incorrect field count in summary table");

      goto done;
    }

  if (index_declaration->fields[0].type != CA_TEXT)
    {
      ca_set_error ("First field in index table must be text");

      goto done;
    }

  if (index_declaration->fields[1].type != CA_OFFSET_SCORE)
    {
      ca_set_error ("Second field in index table must be OFFSET_SCORE, is %s", ca_type_to_string (index_declaration->fields[1].type));

      goto done;
    }

  if (summary_declaration->fields[1].type != CA_TIME)
    {
      ca_set_error ("Second field in summary table must be TIMESTAMP WITH TIME ZONE");

      goto done;
    }

  if (summary_declaration->fields[2].type != CA_TEXT)
    {
      ca_set_error ("Second field in summary table must be TEXT");

      goto done;
    }

  if (1 != (ret = ca_table_seek_to_key (index_table, query)))
    {
      if (!ret) /* "Not found" is not an error */
        result = 0;

      goto done;
    }

  if (1 != (ret = ca_table_read_row (index_table, NULL, &data_iov)))
    {
      if (!ret)
        ca_set_error ("ca_table_read_row unexpectedly returned 0");

      goto done;
    }

  data = data_iov.iov_base;

  if (-1 == ca_data_parse_offset_score (&data, &offsets, &offset_count))
    goto done;

  if (offset_count < limit)
    limit = offset_count;

  ca_quicksort (offsets, offset_count);

  putchar ('[');

  for (i = 0; i < limit; ++i)
    {
      if (i)
        printf (",\n");

      if (-1 == ca_table_seek (summary_table, offsets[i].offset, SEEK_SET))
        goto done;

      if (1 != (ret = ca_table_read_row (summary_table, NULL, &data_iov)))
        {
          if (!ret)
            ca_set_error ("ca_table_read_row unexpectedly returned 0.  Is the index stale?");

          goto done;
        }

      printf ("%s", (const char *) data_iov.iov_base + 8);
    }

  printf ("]\n");

  result = 0;

done:

  free (offsets);

  return result;
}
