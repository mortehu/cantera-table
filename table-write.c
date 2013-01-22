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
