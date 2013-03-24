/*
    Low-level data formatter for Cantera Table
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
#include <string.h>

#include "ca-table.h"

#define MAX_HEADER_SIZE 64

int
ca_table_write_offset_score (struct ca_table *table, const char *key,
                             const struct ca_offset_score *values,
                             size_t count)
{
  struct iovec iov[2];
  size_t buffer_alloc;
  uint8_t *buffer, *o;
  int result = -1;

  buffer_alloc = ca_offset_score_size (values, count);

  buffer = ca_malloc (buffer_alloc);
  o = buffer;

  ca_format_offset_score (&o, values, count);

#ifndef NVERIFY
  struct ca_offset_score *tmp;
  uint32_t tmp_count;
  const uint8_t *input;

  input = buffer;

  if (0 == ca_parse_offset_score_array (&input, &tmp, &tmp_count))
    {
      size_t i;

      assert (tmp_count == count);

      for (i = 0; i < count; ++i)
        {
          assert (tmp[i].offset == values[i].offset);
          assert (tmp[i].score == values[i].score);
        }

      free (tmp);
    }
#endif

  assert (o <= buffer + buffer_alloc);

  iov[0].iov_base = (void *) key;
  iov[0].iov_len = strlen (key) + 1;

  iov[1].iov_base = buffer;
  iov[1].iov_len = o - buffer;

  result = ca_table_insert_row (table, iov, sizeof (iov) / sizeof (iov[0]));

  free (buffer);

  return result;
}
