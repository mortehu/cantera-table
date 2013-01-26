#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <string.h>

#include "ca-table.h"
#include "memory.h"

#define MAX_HEADER_SIZE 64

static void
CA_put_integer (uint8_t **output, uint64_t value)
{
  uint8_t buffer[10];
  unsigned int ptr = 9;

  buffer[ptr] = value & 0x7f;
  value >>= 7;

  while (value)
    {
      buffer[--ptr] = 0x80 | value;

      value >>= 7;
    }

  memcpy (*output, &buffer[ptr], 10 - ptr);

  *output += 10 - ptr;
}

int
ca_table_write_time_float4 (struct ca_table *table, const char *key,
                            uint64_t start_time, uint32_t interval,
                            const float *sample_values, size_t sample_count)
{
  struct iovec value[2];
  uint8_t header[MAX_HEADER_SIZE], *o;

  o = header;

  CA_put_integer (&o, start_time);
  CA_put_integer (&o, interval);
  CA_put_integer (&o, sample_count);

  value[0].iov_base = header;
  value[0].iov_len = o - header;
  value[1].iov_base = (void *) sample_values;
  value[1].iov_len = sizeof (*sample_values) * sample_count;

  return ca_table_insert_row (table, key,
                              value, sizeof (value) / sizeof (value[0]));
}

int
ca_table_write_table_declaration (struct ca_table *table,
                                  const char *table_name,
                                  const struct ca_table_declaration *decl)
{
  struct iovec value[3];
  uint8_t header[MAX_HEADER_SIZE], *o;

  o = header;

  CA_put_integer (&o, decl->field_count);

  value[0].iov_base = header;
  value[0].iov_len = o - header;
  value[1].iov_base = (void *) decl->path;
  value[1].iov_len = strlen (decl->path) + 1;
  value[2].iov_base = (void *) decl->fields;
  value[2].iov_len = sizeof (*decl->fields) * decl->field_count;

  return ca_table_insert_row (table, table_name,
                              value, sizeof (value) / sizeof (value[0]));
}

int
ca_table_write_offset_score (struct ca_table *table, const char *key,
                             const struct ca_offset_score *values,
                             size_t count)
{
  struct iovec iov;

  uint8_t *target, *o;
  size_t i, target_alloc, target_size = 0;
  uint64_t prev_offset = 0;

  uint32_t min_score, max_score;
  uint8_t bytes_per_score;

  int result = -1;

  target_alloc = 32;

  if (!(target = safe_malloc (target_alloc)))
    return -1;

  o = target;

  CA_put_integer (&o, CA_OFFSET_SCORE_VARBYTE_FIXED);
  CA_put_integer (&o, count);

  min_score = values[0].score;
  max_score = values[0].score;

  for (i = 1; i < count; ++i)
    {
      if (values[i].score < min_score)
        min_score = values[i].score;
      else if (values[i].score > max_score)
        max_score = values[i].score;
    }

  CA_put_integer (&o, min_score);
  max_score -= min_score;

  if (!max_score)
    bytes_per_score = 0;
  else if (!(max_score & ~0xff))
    bytes_per_score = 1;
  else if (!(max_score & ~0xffff))
    bytes_per_score = 2;
  else if (!(max_score & ~0xffffff))
    bytes_per_score = 3;
  else
    bytes_per_score = 4;

  *o++ = bytes_per_score;

  for (i = 0; i < count; ++i)
    {
      uint32_t score;

      target_size = o - target;

      if (target_size + 16 > target_alloc)
        {
          if (-1 == ARRAY_GROW (&target, &target_alloc))
            goto done;

          o = target + target_size;
        }

      CA_put_integer (&o, values[i].offset - prev_offset);
      prev_offset = values[i].offset;

      score = values[i].score - min_score;

      switch (bytes_per_score)
        {
        case 4: *o++ = score >> 24;
        case 3: *o++ = score >> 16;
        case 2: *o++ = score >> 8;
        case 1: *o++ = score;
        }
    }

  iov.iov_base = target;
  iov.iov_len = o - target;

  result = ca_table_insert_row (table, key, &iov, 1);

done:

  free (target);

  return result;
}
