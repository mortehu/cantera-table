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

  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  if (result)
    {
      o = result;

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
  else
    {
      size_t count = 0;

      while (lhs != lhs_end && rhs != rhs_end)
        {
          if (lhs->offset < rhs->offset)
            {
              ++count;
              ++lhs;
            }
          else
            {
              if (lhs->offset == rhs->offset)
                ++lhs;

              ++count;
              ++rhs;
            }
        }

      count += lhs_end - lhs;
      count += rhs_end - rhs;

      return count;
    }
}

/* Note: This function either frees the `rhs' or assigns `lhs' to it */
int
CA_union_offsets_inplace (struct ca_offset_score **lhs, uint32_t *lhs_count,
                          struct ca_offset_score *rhs, uint32_t rhs_count)
{
  struct ca_offset_score *merged_offsets;
  size_t merged_offset_count;
  size_t max_merged_size;

  if (!*lhs)
    {
      *lhs = rhs;
      *lhs_count = rhs_count;

      return 0;
    }

  max_merged_size = *lhs_count + rhs_count;

  if (!(merged_offsets = ca_malloc (sizeof (*merged_offsets) * max_merged_size)))
    return -1;

  merged_offset_count = CA_union_offsets (merged_offsets,
                                          *lhs, *lhs_count, rhs, rhs_count);

  free (*lhs);
  free (rhs);

  *lhs = merged_offsets;
  *lhs_count = merged_offset_count;

  return 0;
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
                   int operator, float operand0, float operand1)
{
  size_t i, result = 0;

  for (i = 0; i < count; ++i)
    {
      switch (operator)
        {
        case '[':

          if (offsets[i].score < operand0 || offsets[i].score > operand1)
            continue;

          break;

        case L'≤':

          if (offsets[i].score > operand0)
            continue;

          break;

        case '<':

          if (offsets[i].score >= operand0)
            continue;

          break;

        case '=':

          if (offsets[i].score != operand0)
            continue;

          break;

        case '>':

          if (offsets[i].score <= operand0)
            continue;

          break;

        case L'≥':

          if (offsets[i].score < operand0)
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

static float
parse_value (char *string, char **endptr)
{
  float result;

  result = strtod (string, endptr);

  if (**endptr == '-')
    {
      struct tm tm;

      memset (&tm, 0, sizeof (tm));
      *endptr = strptime (string, "%Y-%m-%d", &tm);

      result = timegm (&tm) / 86400;
    }

  return result;
}

static ssize_t
CA_schema_subquery (struct ca_offset_score **ret_offsets,
                    const char *query,
                    struct ca_table **index_tables,
                    size_t index_table_count)
{
  char *query_buf = NULL, *token, *ch;

  struct ca_offset_score *offsets = NULL;
  ssize_t offset_count = 0, result = -1, ret;
  int first = 1;

  struct iovec data_iov;
  const uint8_t *data;

  size_t i;

  if (!(query_buf = ca_strdup (query)))
    return -1;

  for (token = strtok (query_buf, " \t"); token; token = strtok (NULL, " \t"))
    {
      struct ca_offset_score *token_offsets = NULL;
      uint32_t token_offset_count = 0;
      int invert_rank = 0, subtract = 0, add = 0;

      int operator = 0;
      float operand0 = 0, operand1 = 0;

      if (*token == '-')
        ++token, subtract = 1;
      else if (*token == '+')
        ++token, add = 1;

      if (*token == '~')
        ++token, invert_rank = 1;

      if (NULL != (ch = strchr (token, '>'))
          || NULL != (ch = strchr (token, '<'))
          || NULL != (ch = strchr (token, '='))
          || NULL != (ch = strchr (token, '[')))
        {
          char *endptr, *delimiter = NULL;
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

          if (operator == '['
              && NULL != (delimiter = strchr (ch, ',')))
            {
              *delimiter++ = 0;

              operand1 = parse_value (delimiter, &endptr);
            }

          operand0 = parse_value (ch, &endptr);
        }

      if (!strncmp (token, "in-", 3))
        {
          const char *key_match_begin, *key_match_end;
          const char *parameter;

          key_match_begin = token + 3;

          if (NULL != (key_match_end = strchr (key_match_begin, ':')))
            ++key_match_end;
          else
            key_match_begin = key_match_end;

          parameter = key_match_end;

          for (i = 0; i < index_table_count; ++i)
            {
              if (-1 == (ca_table_seek (index_tables[i], 0, SEEK_SET)))
                goto done;

              while (1 == (ret = ca_table_read_row (index_tables[i], &data_iov)))
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

                  if (-1 == CA_union_offsets_inplace (&token_offsets, &token_offset_count,
                                                      new_offsets, new_offset_count))
                    goto done;
                }

              if (ret == -1)
                goto done;
            }
        }
      else
        {
          for (i = 0; i < index_table_count; ++i)
            {
              struct ca_offset_score *new_offsets;
              uint32_t new_offset_count;

              if (1 != (ret = ca_table_seek_to_key (index_tables[i], token)))
                {
                  /* No matches in this table */

                  if (ret == -1)
                    goto done;

                  continue;
                }

              if (1 != (ret = ca_table_read_row (index_tables[i], &data_iov)))
                {
                  if (ret >= 0)
                    ca_set_error ("ca_table_read_row unexpectedly returned %d", (int) ret);

                  goto done;
                }

              data = (const uint8_t *) strchr (data_iov.iov_base, 0) + 1;

              if (-1 == ca_parse_offset_score_array (&data, &new_offsets,
                                                     &new_offset_count))
                goto done;

              if (-1 == CA_union_offsets_inplace (&token_offsets, &token_offset_count,
                                                  new_offsets, new_offset_count))
                goto done;
            }
        }

      if (operator)
        {
          token_offset_count =
            CA_filter_offsets (token_offsets, token_offset_count, operator, operand0, operand1);
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

  result = offset_count;
  *ret_offsets = offsets;
  offsets = NULL;

done:

  free (offsets);
  free (query_buf);

  return result;
}

static ssize_t
CA_schema_query (struct ca_offset_score **ret_offsets,
                 struct ca_schema *schema, const char *query)
{
  struct ca_table **index_tables;
  ssize_t index_table_count;

  if (-1 == (index_table_count = ca_schema_index_tables (schema, &index_tables)))
    return -1;

  return CA_schema_subquery (ret_offsets, query, index_tables, index_table_count);
}

int
ca_schema_query (struct ca_schema *schema, const char *query,
                 ssize_t limit)
{
  struct iovec data_iov;

  struct ca_offset_score *offsets = NULL;
  ssize_t i, j, offset_count;

  struct ca_table **summary_tables;
  uint64_t *summary_table_offsets;
  ssize_t summary_table_count;

  struct ca_table **summary_override_tables;
  ssize_t summary_override_table_count;

  ssize_t ret;
  int result = -1;

  ca_clear_error ();

  if (-1 == (summary_table_count = ca_schema_summary_tables (schema, &summary_tables,
                                                             &summary_table_offsets)))
    goto done;

  if (!summary_table_count)
    {
      ca_set_error ("No summary tables found");

      goto done;
    }

  if (-1 == (summary_override_table_count = ca_schema_summary_override_tables (schema, &summary_override_tables)))
    goto done;

  if (-1 == (offset_count = CA_schema_query (&offsets, schema, query)))
    goto done;

  ca_sort_offset_score_by_score (offsets, offset_count);

  if (limit >= 0 && limit < offset_count)
    offset_count = limit;

  putchar ('[');

  for (i = 0; i < offset_count; ++i)
    {
      const char *begin, *end;

      if (i)
        printf (",\n");

      j = summary_table_count;

      while (--j && summary_table_offsets[j] > offsets[i].offset)
        ;

      if (-1 == ca_table_seek (summary_tables[j],
                               offsets[i].offset - summary_table_offsets[j],
                               SEEK_SET))
        goto done;

      if (1 != (ret = ca_table_read_row (summary_tables[j], &data_iov)))
        {
          if (ret >= 0)
            ca_set_error ("ca_table_read_row unexpectedly returned %d",
                          (int) ret);

          goto done;
        }

      for (j = 0; j < summary_override_table_count; ++j)
        {
          if (-1 == (ret = ca_table_seek_to_key (summary_override_tables[j], data_iov.iov_base)))
            goto done;

          if (!ret)
            continue;

          if (1 != (ret = ca_table_read_row (summary_override_tables[j], &data_iov)))
            {
              if (ret >= 0)
                ca_set_error ("ca_table_read_row unexpectedly returned %d",
                              (int) ret);

              goto done;
            }

          break;
        }

      begin = data_iov.iov_base;
      end = begin + data_iov.iov_len;

      begin = strchr (begin, 0) + 1;

      printf ("%.*s", (int) (end - begin), begin);
    }

  printf ("]\n");

  result = 0;

done:

  free (offsets);

  return result;
}

static const struct ca_offset_score *
CA_offset_score_lower_bound (const struct ca_offset_score *begin,
                             const struct ca_offset_score *end,
                             uint64_t offset)
{
  const struct ca_offset_score *middle;
  size_t half, len = end - begin;

  while (len > 0)
    {
      half = len >> 1;
      middle = begin + half;

      if (middle->offset < offset)
        {
          begin = middle;
          ++begin;
          len = len - half - 1;
        }
      else
        len = half;
    }

  return begin;
}

int
ca_schema_query_correlate (struct ca_schema *schema,
                           const char *query_A,
                           const char *query_B)
{
  struct ca_table **index_tables;
  ssize_t index_table_count;

  ssize_t count_A, count_B;
  struct ca_offset_score *offsets_A = NULL, *offsets_B = NULL;
  float prior_A;

  struct iovec data_iov;
  int result = -1, ret;

  if (-1 == (index_table_count = ca_schema_index_tables (schema, &index_tables)))
    return -1;

  if (-1 == (count_A = CA_schema_subquery (&offsets_A, query_A,
                                           index_tables, index_table_count)))
    goto done;

  if (-1 == (count_B = CA_schema_subquery (&offsets_B, query_B,
                                           index_tables, index_table_count)))
    goto done;

  count_B = CA_subtract_offsets (offsets_B, count_B, offsets_A, count_A);

  if (!count_A)
    {
      ca_set_error ("Set A is empty");

      goto done;
    }

  if (!count_B)
    {
      ca_set_error ("Set B is empty");

      goto done;
    }

  if (-1 == (ca_table_seek (index_tables[0], 0, SEEK_SET)))
    goto done;

  prior_A = (float) count_A / (count_A + count_B);

  while (1 == (ret = ca_table_read_row (index_tables[0], &data_iov)))
    {
      const char *key;
      const uint8_t *data;

      struct ca_offset_score *key_offsets;
      uint32_t key_offset_count;

      const struct ca_offset_score *A, *B, *K;
      const struct ca_offset_score *A_end, *B_end, *K_end;

      size_t match_count_A = 0, match_count_B = 0;

      key = data_iov.iov_base;
      data = (const uint8_t *) strchr (data_iov.iov_base, 0) + 1;

      if (-1 == ca_parse_offset_score_array (&data, &key_offsets,
                                             &key_offset_count))
        goto done;

      A = offsets_A;
      B = offsets_B;
      K = key_offsets;

      A_end = A + count_A;
      B_end = B + count_B;
      K_end = K + key_offset_count;

      while (K != K_end)
        {
          size_t offset;

          offset = K->offset;

          A = CA_offset_score_lower_bound (A, A_end, offset);
          B = CA_offset_score_lower_bound (B, B_end, offset);

          if (A != A_end && A->offset == offset)
            ++match_count_A;

          if (B != B_end && B->offset == offset)
            ++match_count_B;

          do
            ++K;
          while (K != K_end && K->offset == offset);
        }

      if (match_count_A || match_count_B)
        {
          float fraction_A, overrepresentation_A;

          fraction_A = (float) match_count_A / (match_count_A + match_count_B);
          overrepresentation_A = fraction_A / prior_A;

          printf ("%.3f\t%zu\t%zu\t%s\n",
                  overrepresentation_A, match_count_A, match_count_B, key);

          fflush (stdout);
        }

      free (key_offsets);
    }

  if (ret == -1)
    goto done;

  result = 0;

done:

  free (offsets_B);
  free (offsets_A);

  return result;
}
