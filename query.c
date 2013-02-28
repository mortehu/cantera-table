/*
    Inverted index query processor
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
#include "config.h"
#endif

#include <assert.h>
#include <string.h>
#include <time.h>

#include "ca-table.h"

static size_t
CA_subtract_offsets (struct ca_offset_score *lhs, size_t lhs_count,
                     const struct ca_offset_score *rhs, size_t rhs_count)
{
  struct ca_offset_score *output, *o;
  const struct ca_offset_score *lhs_end, *rhs_end;

  output = o = lhs;

  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end)
    {
      if (lhs->offset == rhs->offset)
        {
          do
            ++lhs;
          while (lhs != lhs_end && lhs->offset == rhs->offset);

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
CA_union_offsets (struct ca_offset_score *result,
                  const struct ca_offset_score *lhs, size_t lhs_count,
                  const struct ca_offset_score *rhs, size_t rhs_count)
{
  struct ca_offset_score *o;
  const struct ca_offset_score *lhs_end, *rhs_end;

  o = result;

  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end)
    {
      if (lhs->offset < rhs->offset)
        *o++ = *lhs++;
      else
        {
          if (lhs->offset == rhs->offset)
            ++lhs;

          *o++ = *rhs++;
        }
    }

  while (lhs != lhs_end)
    *o++ = *lhs++;

  while (rhs != rhs_end)
    *o++ = *rhs++;

  return o - result;
}

static size_t
CA_intersect_offsets (struct ca_offset_score *lhs, size_t lhs_count,
                      const struct ca_offset_score *rhs, size_t rhs_count)
{
  struct ca_offset_score *output, *o;
  const struct ca_offset_score *lhs_end, *rhs_end;

  output = o = lhs;

  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end)
    {
      if (lhs->offset == rhs->offset)
        {
          *o++ = *lhs;

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

static size_t
CA_filter_offsets (struct ca_offset_score *offsets, size_t count,
                   int operator, float operand)
{
  size_t i, result = 0;

  for (i = 0; i < count; ++i)
    {
      switch (operator)
        {
        case L'≤':

          if (offsets[i].score > operand)
            continue;

          break;

        case '<':

          if (offsets[i].score >= operand)
            continue;

          break;

        case '=':

          if (offsets[i].score != operand)
            continue;

          break;

        case '>':

          if (offsets[i].score <= operand)
            continue;

          break;

        case L'≥':

          if (offsets[i].score < operand)
            continue;

          break;
        }

      offsets[result++] = offsets[i];
    }

  return result;
}

static size_t
CA_remove_duplicates (struct ca_offset_score *offsets, size_t count)
{
  struct ca_offset_score *input, *end, *output;
  size_t prev_offset = SIZE_MAX;

  output = offsets;
  input = offsets;
  end = offsets + count;

  ca_sort_offset_score_by_offset (offsets, count);

  while (input != end)
    {
      if (input->offset != prev_offset)
        {
          *output++ = *input;
          prev_offset = input->offset;
        }

      ++input;
    }

  return output - offsets;
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
  uint32_t i, offset_count = 0;

  char *query_buf = NULL, *token, *ch;

  ssize_t ret;
  int first = 1, result = -1;

  ca_clear_error ();

  if (!(query_buf = ca_strdup (query)))
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

  if (index_declaration->fields[1].type != CA_OFFSET_SCORE_ARRAY)
    {
      ca_set_error ("Second field in index table must be OFFSET_SCORE[], is %s", ca_type_to_string (index_declaration->fields[1].type));

      goto done;
    }

  if (summary_declaration->fields[1].type != CA_TEXT)
    {
      ca_set_error ("Second field in summary table must be TEXT");

      goto done;
    }

  for (token = strtok (query_buf, " \t"); token; token = strtok (NULL, " \t"))
    {
      struct ca_offset_score *token_offsets = NULL;
      uint32_t token_offset_count = 0;
      int invert_rank = 0, subtract = 0, add = 0;

      int operator = 0;
      float operand = 0;

      if (*token == '-')
        ++token, subtract = 1;
      else if (*token == '+')
        ++token, add = 1;

      if (*token == '~')
        ++token, invert_rank = 1;

      if (NULL != (ch = strchr (token, '>'))
          || NULL != (ch = strchr (token, '<'))
          || NULL != (ch = strchr (token, '=')))
        {
          char *endptr;
          operator = *ch;
          *ch++ = 0;

          if (*ch == '=')
            {
              if (operator == '>')
                operator = L'≥';
              else if (operator == '<')
                operator = L'≤';

              ++ch;
            }

          operand = strtod (ch, &endptr);

          if (*endptr == '-')
            {
              struct tm tm;

              memset (&tm, 0, sizeof (tm));
              endptr = strptime (ch, "%Y-%m-%d", &tm);

              if (!*endptr)
                operand = timegm (&tm) / 86400;
            }
        }

      if (!strncmp (token, "in-", 3))
        {
          const char *key_match_begin, *key_match_end;
          const char *parameter;

          if (-1 == (ca_table_seek (index_table, 0, SEEK_SET)))
            goto done;

          key_match_begin = token + 3;

          if (NULL != (key_match_end = strchr (key_match_begin, ':')))
            ++key_match_end;
          else
            key_match_begin = key_match_end;

          parameter = key_match_end;

          while (1 == (ret = ca_table_read_row (index_table, &data_iov)))
            {
              const char *key;
              struct ca_offset_score *new_offsets;
              uint32_t new_offset_count;

              key = data_iov.iov_base;

              if (strncmp (key, key_match_begin, key_match_end - key_match_begin))
                continue;

              if (!strstr (key + (key_match_end - key_match_begin), parameter))
                continue;

              data = (const uint8_t *) strchr (data_iov.iov_base, 0) + 1;

              if (-1 == ca_parse_offset_score_array (&data, &new_offsets,
                                                     &new_offset_count))
                goto done;

              if (!token_offsets)
                {
                  token_offsets = new_offsets;
                  token_offset_count = new_offset_count;
                }
              else
                {
                  struct ca_offset_score *merged_offsets;
                  size_t merged_offset_count;
                  size_t max_merged_size;

                  max_merged_size = new_offset_count + token_offset_count;

                  if (!(merged_offsets = ca_malloc (sizeof (*merged_offsets) * max_merged_size)))
                    goto done;

                  merged_offset_count = CA_union_offsets (merged_offsets,
                                                          new_offsets, new_offset_count,
                                                          token_offsets, token_offset_count);

                  free (token_offsets);
                  free (new_offsets);

                  token_offsets = merged_offsets;
                  token_offset_count = merged_offset_count;
                }
            }

          if (ret == -1)
            goto done;
        }
      else
        {
          if (1 != (ret = ca_table_seek_to_key (index_table, token)))
            {
              if (ret == -1)
                goto done;

              token_offsets = NULL;
              token_offset_count = 0;
            }
          else
            {
              if (1 != (ret = ca_table_read_row (index_table, &data_iov)))
                {
                  if (ret >= 0)
                    ca_set_error ("ca_table_read_row unexpectedly returned %d", (int) ret);

                  goto done;
                }

              data = (const uint8_t *) strchr (data_iov.iov_base, 0) + 1;

              if (-1 == ca_parse_offset_score_array (&data, &token_offsets,
                                                     &token_offset_count))
                goto done;
            }
        }

      if (operator)
        {
          token_offset_count =
            CA_filter_offsets (token_offsets, token_offset_count, operator, operand);
        }

      if (invert_rank)
        {
          for (i = 0; i < token_offset_count; ++i)
            token_offsets[i].score = -token_offsets[i].score;
        }

      if (first)
        {
          offsets = token_offsets;
          offset_count = token_offset_count;
        }
      else
        {
          if (subtract)
            {
              offset_count = CA_subtract_offsets (offsets, offset_count,
                                                  token_offsets, token_offset_count);
            }
          else if (add)
            {
              struct ca_offset_score *merged_offsets;
              size_t merged_offset_count;
              size_t max_merged_size;

              max_merged_size = offset_count + token_offset_count;

              if (!(merged_offsets = ca_malloc (sizeof (*merged_offsets) * max_merged_size)))
                goto done;

              merged_offset_count = CA_union_offsets (merged_offsets,
                                                      offsets, offset_count,
                                                      token_offsets, token_offset_count);

              free (offsets);

              offsets = merged_offsets;
              offset_count = merged_offset_count;
            }
          else
            {
              offset_count = CA_intersect_offsets (offsets, offset_count,
                                                   token_offsets, token_offset_count);
            }

          free (token_offsets);
        }

      first = 0;
    }

  offset_count = CA_remove_duplicates (offsets, offset_count);

  ca_sort_offset_score_by_score (offsets, offset_count);

  if (limit >= 0 && limit < offset_count)
    offset_count = limit;

  putchar ('[');

  for (i = 0; i < offset_count; ++i)
    {
      if (i)
        printf (",\n");

      if (-1 == ca_table_seek (summary_table, offsets[i].offset, SEEK_SET))
        goto done;

      if (1 != (ret = ca_table_read_row (summary_table, &data_iov)))
        {
          if (ret >= 0)
            ca_set_error ("ca_table_read_row unexpectedly returned %d.  Is the index stale?", (int) ret);

          goto done;
        }

      printf ("%s", (const char *) strchr (data_iov.iov_base, 0) + 1);
    }

  printf ("]\n");

  result = 0;

done:

  free (offsets);
  free (query_buf);

  return result;
}
