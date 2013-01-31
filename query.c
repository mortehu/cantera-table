#include <assert.h>
#include <string.h>

#include "ca-table.h"
#include "memory.h"

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

static size_t
CA_subtract_offsets (struct ca_offset_score *output,
                     const struct ca_offset_score *lhs, size_t lhs_count,
                     const struct ca_offset_score *rhs, size_t rhs_count)
{
  struct ca_offset_score *o;
  const struct ca_offset_score *lhs_end, *rhs_end;

  o = output;
  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end)
    {
      if (lhs->offset == rhs->offset)
        {
          ++lhs;
          ++rhs;

          continue;
        }

      if (lhs->offset < rhs->offset)
        *o++ = *lhs++;
      else
        ++rhs;
    }

  while (lhs != lhs_end)
    *o++ = *lhs++;

  return o - output;
}

static size_t
CA_intersect_offsets (struct ca_offset_score *output,
                      const struct ca_offset_score *lhs, size_t lhs_count,
                      const struct ca_offset_score *rhs, size_t rhs_count)
{
  struct ca_offset_score *o;
  const struct ca_offset_score *lhs_end, *rhs_end;

  o = output;
  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end)
    {
      if (lhs->offset == rhs->offset)
        {
          o->offset = lhs->offset;
          o->score = lhs->score;
          ++o;

          ++lhs;
          ++rhs;

          continue;
        }

      if (lhs->offset < rhs->offset)
        ++lhs;
      else
        ++rhs;
    }

  return o - output;
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

  struct iovec data_iov[2];
  const uint8_t *data;

  struct ca_offset_score *offsets = NULL;
  uint32_t i, offset_count = 0;

  char *query_buf = NULL, *token;

  ssize_t ret;
  int result = -1;

  ca_clear_error ();

  if (!(query_buf = safe_strdup (query)))
    goto done;

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

  if (summary_declaration->fields[1].type != CA_TIMESTAMPTZ)
    {
      ca_set_error ("Second field in summary table must be TIMESTAMP WITH TIME ZONE");

      goto done;
    }

  if (summary_declaration->fields[2].type != CA_TEXT)
    {
      ca_set_error ("Second field in summary table must be TEXT");

      goto done;
    }

  for (token = strtok (query_buf, " \t"); token; token = strtok (NULL, " \t"))
    {
      struct ca_offset_score *token_offsets;
      uint32_t token_offset_count;
      int invert_rank = 0, subtract = 0;

      if (*token == '-')
        ++token, subtract = 1;

      if (*token == '~')
        ++token, invert_rank = 1;

      if (1 != (ret = ca_table_seek_to_key (index_table, token)))
        {
          if (!ret) /* "Not found" is not an error */
            result = 0;

          goto done;
        }

      if (2 != (ret = ca_table_read_row (index_table, data_iov, 2)))
        {
          if (ret >= 0)
            ca_set_error ("ca_table_read_row unexpectedly returned %d", (int) ret);

          goto done;
        }

      data = data_iov[1].iov_base;

      if (-1 == ca_parse_offset_score (&data, &token_offsets, &token_offset_count))
        goto done;

      if (invert_rank)
        {
          for (i = 0; i < token_offset_count; ++i)
            token_offsets[i].score = -token_offsets[i].score;
        }

      if (!offsets)
        {
          offsets = token_offsets;
          offset_count = token_offset_count;
        }
      else
        {
          struct ca_offset_score *merged_offsets;
          size_t merged_offset_count;
          size_t max_merged_size;

          if (subtract)
            max_merged_size = offset_count;
          else
            max_merged_size = (token_offset_count > offset_count) ? offset_count : token_offset_count;

          if (!(merged_offsets = safe_malloc (sizeof (*merged_offsets) * max_merged_size)))
            goto done;

          if (subtract)
            {
              merged_offset_count = CA_subtract_offsets (merged_offsets,
                                                         offsets, offset_count,
                                                         token_offsets, token_offset_count);
            }
          else
            {
              merged_offset_count = CA_intersect_offsets (merged_offsets,
                                                          offsets, offset_count,
                                                          token_offsets, token_offset_count);
            }

          free (offsets);
          free (token_offsets);

          offsets = merged_offsets;
          offset_count = merged_offset_count;
        }
    }

  if (offset_count < limit)
    limit = offset_count;

  ca_quicksort (offsets, offset_count);

  /* XXX: Fetch documents in phsyical order */

  putchar ('[');

  for (i = 0; i < limit; ++i)
    {
      if (i)
        printf (",\n");

      if (-1 == ca_table_seek (summary_table, offsets[i].offset, SEEK_SET))
        goto done;

      if (2 != (ret = ca_table_read_row (summary_table, data_iov, 2)))
        {
          if (ret >= 0)
            ca_set_error ("ca_table_read_row unexpectedly returned %d.  Is the index stale?", (int) ret);

          goto done;
        }

      printf ("%s", (const char *) data_iov[1].iov_base + 8);
    }

  printf ("]\n");

  result = 0;

done:

  free (offsets);
  free (query_buf);

  return result;
}
