#include <string.h>

#include "ca-table.h"
#include "crc32.h"
#include "io.h"
#include "memory.h"
#include "smalltable-internal.h"

void
TABLE_flush (struct table *t)
{
  write_all (t->fd, t->buffer, t->buffer_fill);
  t->write_offset += t->buffer_fill;

  t->crc32 = crc32 (t->crc32, t->buffer, t->buffer_fill);

  t->buffer_fill = 0;
}

void
TABLE_write (struct table *t, const void *data, size_t size)
{
  if (t->buffer_fill + size > t->buffer_size)
    {
      TABLE_flush (t);

      if (size >= t->buffer_size)
        {
          write_all (t->fd, data, size);

          return;
        }
    }

  memcpy (t->buffer + t->buffer_fill, data, size);
  t->buffer_fill += size;
}

void
TABLE_put_integer (struct table *t, uint64_t value)
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

  TABLE_write (t, &buffer[ptr], 10 - ptr);
}

static void
TABLE_write_key (struct table *t, const char *key)
{
  int cmp = 1;

  if (!t->prev_key || (cmp = strcmp (key, t->prev_key)))
    {
      if (t->entry_alloc == t->entry_count)
        ARRAY_GROW (&t->entries, &t->entry_alloc);

      if (cmp < 0)
        t->flags &= ~TABLE_FLAG_ORDERED;

      free (t->prev_key);
      t->prev_key = safe_strdup (key);
      t->prev_time = 0;

      t->entries[t->entry_count++] = t->write_offset + t->buffer_fill;

      TABLE_write (t, key, strlen (key) + 1);
    }
}

/* Writes a key/timestamp/float combination to a table */
void
table_write_sample (struct table *t, const char *key, uint64_t sample_time,
                    float sample_value)
{
  table_write_samples (t, key, sample_time, 1, &sample_value, 1);
}

/* Writes multiple key/timestamp/float combinations to a table */
void
table_write_samples (struct table *t, const char *key,
                     uint64_t start_time, uint32_t interval,
                     const float *sample_values, size_t count)
{
  TABLE_write_key (t, key);

  if (t->no_relative || (!t->prev_time || start_time < t->prev_time))
    {
      TABLE_putc (t, CA_TIME_SERIES);
      TABLE_put_integer (t, start_time);
    }
  else
    {
      TABLE_putc (t, CA_RELATIVE_TIME_SERIES);
      TABLE_put_integer (t, start_time - t->prev_time);
    }

  t->prev_time = start_time;

  TABLE_put_integer (t, interval);
  TABLE_put_integer (t, count);
  TABLE_write (t, sample_values, sizeof (*sample_values) * count);
}

void
table_write_table_declaration (struct table *t, const char *key,
                               const struct ca_table_declaration *declaration)
{
  TABLE_write_key (t, key);

  TABLE_putc (t, CA_TABLE_DECLARATION);
  TABLE_write (t, declaration->path, strlen (declaration->path) + 1);
  TABLE_put_integer (t, declaration->field_count);
  TABLE_write (t, declaration->fields, sizeof (*declaration->fields) * declaration->field_count);
}
