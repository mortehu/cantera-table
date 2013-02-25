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
ca_table_write_time_float4 (struct ca_table *table, const char *key,
                            uint64_t start_time, uint32_t interval,
                            const float *sample_values, size_t sample_count)
{
  struct iovec value[3];
  uint8_t header[MAX_HEADER_SIZE], *o;

  o = header;

  ca_format_integer (&o, start_time);
  ca_format_integer (&o, interval);
  ca_format_integer (&o, sample_count);

  value[0].iov_base = (void *) key;
  value[0].iov_len = strlen (key) + 1;
  value[1].iov_base = header;
  value[1].iov_len = o - header;
  value[2].iov_base = (void *) sample_values;
  value[2].iov_len = sizeof (*sample_values) * sample_count;

  return ca_table_insert_row (table,
                              value, sizeof (value) / sizeof (value[0]));
}

int
ca_table_write_offset_score (struct ca_table *table, const char *key,
                             const struct ca_offset_score *values,
                             size_t count)
{
  struct iovec iov[2];
  size_t buffer_alloc;
  uint8_t *buffer, *o;
  int result = -1;

  /* XXX: Allocate correct amount */
  buffer_alloc = 32 + count * 13;

  buffer = ca_malloc (32 + count * 13);
  o = buffer;

  ca_format_offset_score (&o, values, count);

  assert (o <= buffer + buffer_alloc);

  iov[0].iov_base = (void *) key;
  iov[0].iov_len = strlen (key) + 1;

  iov[1].iov_base = buffer;
  iov[1].iov_len = o - buffer;

  result = ca_table_insert_row (table, iov, sizeof (iov) / sizeof (iov[0]));

  free (buffer);

  return result;
}
