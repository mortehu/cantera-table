/*
    Validate the various offset/score array compression schemes
    Copyright (C) 2013    Morten Hustveit

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

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

static int
validate_values (struct ca_offset_score *values,
                 size_t count,
                 enum ca_offset_score_type expected_type)
{
  size_t compressed_size;
  uint8_t *compressed_data, *p;

  struct ca_offset_score *decompressed_values;
  uint32_t decompressed_value_count;

  ca_sort_offset_score_by_offset (values, count);

  compressed_size = ca_offset_score_size (values, count);

  if (!(compressed_data = ca_malloc (compressed_size)))
    return -1;

  p = compressed_data;

  ca_format_offset_score (&p, values, count);

  assert ((p - compressed_data) <= compressed_size);
  assert (compressed_data[0] == expected_type);

  p = compressed_data;

  if (-1 == ca_parse_offset_score_array ((const uint8_t **) &p, &decompressed_values, &decompressed_value_count))
    return -1;

  assert (decompressed_value_count == count);

  assert (!memcmp (values, decompressed_values,
                   sizeof (*values) * count));

  free (decompressed_values);
  free (compressed_data);

  return 0;
}

int
main (int argc, char **argv)
{
  static const size_t value_count = 1024;
  struct ca_offset_score values[value_count];

  size_t i;

  int result = EXIT_FAILURE;

  for (i = 0; i < value_count; ++i)
    {
      values[i].offset = (random () & 0xffff);
      values[i].score = (double) random () / RAND_MAX;
    }

  if (-1 == validate_values (values, value_count, CA_OFFSET_SCORE_VARBYTE_FLOAT))
    goto done;

  for (i = 0; i < value_count; ++i)
    values[i].score = (i << 8);

  if (-1 == validate_values (values, value_count, CA_OFFSET_SCORE_VARBYTE_U24))
    goto done;

  for (i = 0; i < value_count; ++i)
    values[i].score = i;

  if (-1 == validate_values (values, value_count, CA_OFFSET_SCORE_VARBYTE_U16))
    goto done;

  for (i = 0; i < value_count; ++i)
    values[i].score = i & 0xff;

  if (-1 == validate_values (values, value_count, CA_OFFSET_SCORE_VARBYTE_U8))
    goto done;

  for (i = 0; i < value_count; ++i)
    values[i].score = 0;

  if (-1 == validate_values (values, value_count, CA_OFFSET_SCORE_VARBYTE_ZERO))
    goto done;

  for (i = 0; i < value_count; ++i)
    values[i].offset = i;

  if (-1 == validate_values (values, value_count, CA_OFFSET_SCORE_VARBYTE_ZERO))
    goto done;

  result = EXIT_SUCCESS;

done:

  if (result == EXIT_FAILURE)
    fprintf (stderr, "Error: %s\n", ca_last_error ());

  return result;
}
