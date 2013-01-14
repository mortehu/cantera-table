#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include "crc32.h"
#include "io.h"
#include "memory.h"
#include "smalltable.h"

#define MAGIC 0x6c6261742e692e70ULL
#define MAJOR_VERSION 0
#define MINOR_VERSION 0

#define TMP_SUFFIX ".tmp.XXXXXX"
#define BUFFER_SIZE (1024 * 1024)

enum TABLE_flags
{
  TABLE_FLAG_ORDERED = 0x0001
};

struct TABLE_header
{
  uint64_t magic;
  uint8_t major_version;
  uint8_t minor_version;
  uint16_t flags;
  uint32_t data_crc32;
  uint64_t index_offset;
};

struct TABLE_entry
{
  const void *data;
  size_t size;
};

struct table
{
  char *path;
  char *tmp_path;

  int fd;

  char *buffer;
  size_t buffer_size, buffer_fill;

  uint64_t write_offset;
  char *prev_key;
  uint64_t prev_time;

  uint32_t crc32;
  uint16_t flags;

  struct TABLE_header *header;

  uint64_t *entries;
  size_t entry_alloc, entry_count;

  struct TABLE_entry *sorted_entries;
};

static void
TABLE_flush (struct table *t);

static void
TABLE_write (struct table *t, const void *data, size_t size);

#define TABLE_putc(t, c) do { if ((t)->buffer_fill == (t)->buffer_size) TABLE_flush ((t)); (t)->buffer[(t)->buffer_fill++] = (c); } while (0)

static void
TABLE_put_integer (struct table *t, uint64_t value);

struct table *
table_create (const char *path)
{
  struct table *result;

  result = safe_malloc (sizeof (*result));
  result->path = safe_strdup (path);

  if (-1 == asprintf (&result->tmp_path, "%s.tmp.%u.XXXXXX", path, getpid ()))
      err (EXIT_FAILURE, "asprintf failed");

  if (-1 == (result->fd = mkstemp (result->tmp_path)))
    err (EXIT_FAILURE, "mkstemp failed on path '%s'", result->tmp_path);

  result->buffer_size = BUFFER_SIZE;
  result->buffer = safe_malloc (result->buffer_size);
  result->buffer_fill = sizeof (struct TABLE_header);

  result->flags = TABLE_FLAG_ORDERED;

  return result;
}

/* Opens an existing table for reading */
struct table *
table_open (const char *path)
{
  struct table *result;
  off_t end;

  result = safe_malloc (sizeof (*result));
  result->path = safe_strdup (path);

  if (-1 == (result->fd = open (path, O_RDONLY)))
    err (EXIT_FAILURE, "Failed to open '%s' for reading", path);

  if (-1 == (end = lseek (result->fd, 0, SEEK_END)))
    err (EXIT_FAILURE, "Failed to seek to end of '%s'", path);

  result->buffer_size = end;
  result->buffer_fill = end;

  if (end)
    {
      if (MAP_FAILED == (result->buffer = mmap (NULL, end, PROT_READ, MAP_SHARED, result->fd, 0)))
        err (EXIT_FAILURE, "Failed to mmap '%s'", path);
    }

  result->header = (struct TABLE_header *) result->buffer;

  if (result->header->magic != MAGIC)
    errx (EX_DATAERR, "Unexpected magic header in '%s'", path);

  result->entries = (uint64_t *) (result->buffer + result->header->index_offset);
  result->entry_alloc = (result->buffer_size - result->header->index_offset) / sizeof (uint64_t);
  result->entry_count = result->entry_alloc;

  return result;
}

/* Closes a previously opened table */
void
table_close (struct table *t)
{
  if (t->tmp_path)
    {
      struct TABLE_header header;

      memset (&header, 0, sizeof (header));
      header.magic = MAGIC; /* Will implicitly store endianness */
      header.major_version = MAJOR_VERSION;
      header.minor_version = MINOR_VERSION;
      header.index_offset = t->write_offset + t->buffer_fill;

      TABLE_write (t, t->entries, sizeof (*t->entries) * t->entry_count);

      TABLE_flush (t);

      header.flags = t->flags;
      header.data_crc32 = t->crc32;

      if (-1 == lseek (t->fd, 0, SEEK_SET))
        err (EXIT_FAILURE, "Failed to seek to start of '%s'", t->tmp_path);

      write_all (t->fd, &header, sizeof (header));

      if (-1 == fsync (t->fd))
        err (EXIT_FAILURE, "Failed to fsync '%s'", t->tmp_path);

      if (-1 == close (t->fd))
        err (EXIT_FAILURE, "Failed to close '%s'", t->tmp_path);

      if (-1 == rename (t->tmp_path, t->path))
        err (EXIT_FAILURE, "Failed to rename '%s' to '%s'", t->tmp_path, t->path);

      free (t->tmp_path);
      free (t->entries);
      free (t->buffer);
    }
  else if (t->buffer_size)
    {
      munmap (t->buffer, t->buffer_size);

      close (t->fd);
    }

  free (t->prev_key);

  free (t->path);
  free (t);
}

/* Retrieves pointers to all values in a table */
const void **
table_values (struct table *t, size_t *count)
{
  *count = 0;

  return NULL;
}

/* Returns the key of a value */
const char *
table_value_key (const void *value)
{
  return (const char *)value;
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
  int cmp = 1;

  if (t->entry_alloc == t->entry_count)
    ARRAY_GROW (&t->entries, &t->entry_alloc);

  if (!t->prev_key || (cmp = strcmp (key, t->prev_key)))
    {
      if (cmp < 0)
        t->flags &= ~TABLE_FLAG_ORDERED;

      free (t->prev_key);
      t->prev_key = safe_strdup (key);
      t->prev_time = 0;

      t->entries[t->entry_count++] = t->write_offset + t->buffer_fill;

      TABLE_write (t, key, strlen (key) + 1);
    }

  if (!t->prev_time || start_time < t->prev_time)
    {
      TABLE_putc (t, TABLE_TIME_SERIES);
      TABLE_put_integer (t, start_time);
    }
  else
    {
      TABLE_putc (t, TABLE_RELATIVE_TIME_SERIES);
      TABLE_put_integer (t, start_time - t->prev_time);
    }

  t->prev_time = start_time;

  TABLE_put_integer (t, interval);
  TABLE_put_integer (t, count);
  TABLE_write (t, sample_values, sizeof (*sample_values) * count);
}

static void
TABLE_flush (struct table *t)
{
  write_all (t->fd, t->buffer, t->buffer_fill);
  t->write_offset += t->buffer_fill;

  t->crc32 = crc32 (t->crc32, t->buffer, t->buffer_fill);

  t->buffer_fill = 0;
}

static void
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

static void
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

static int
TABLE_entrycmp (const void *vlhs, const void *vrhs)
{
  const struct TABLE_entry *lhs = vlhs;
  const struct TABLE_entry *rhs = vrhs;

  return strcmp (lhs->data, rhs->data);
}

static void
TABLE_build_sorted_entries (struct table *t)
{
  size_t i;

  t->sorted_entries = safe_malloc (sizeof (*t->sorted_entries) * t->entry_count);

  for (i = 0; i + 1 < t->entry_count; ++i)
    {
      t->sorted_entries[i].data = t->buffer + t->entries[i];
      t->sorted_entries[i].size = t->entries[i + 1] - t->entries[i];
    }

  t->sorted_entries[i].data = t->buffer + t->entries[i];
  t->sorted_entries[i].size = t->header->index_offset - t->entries[i];

  qsort (t->sorted_entries, t->entry_count, sizeof (*t->sorted_entries),
         TABLE_entrycmp);
}

void
table_iterate (struct table *t, table_iterate_callback callback,
               enum table_order order)
{
  size_t i;

  if (!t->entry_count)
    return;

  if ((t->header->flags & TABLE_FLAG_ORDERED) && order == TABLE_ORDER_KEY)
    order = TABLE_ORDER_PHYSICAL;

  switch (order)
    {
    case TABLE_ORDER_PHYSICAL:

      for (i = 0; i + 1 < t->entry_count; ++i)
        callback (t->buffer + t->entries[i],
                  t->entries[i + 1] - t->entries[i]);

      callback (t->buffer + t->entries[i],
                t->header->index_offset - t->entries[i]);

      break;

    case TABLE_ORDER_KEY:

      if (!t->sorted_entries)
        TABLE_build_sorted_entries (t);

      for (i = 0; i < t->entry_count; ++i)
        callback (t->sorted_entries[i].data, t->sorted_entries[i].size);

      break;
    }
}

static uint64_t
TABLE_parse_integer (const uint8_t **input)
{
  const uint8_t *i;
  uint64_t result = 0;

  i = *input;

  result = *i & 0x7F;

  while (0 != (*i & 0x80))
    {
      result <<= 7;
      result |= *++i & 0x7F;
    }

  *input = ++i;

  return result;
}

void
table_parse_time_series (const uint8_t **input,
                         uint64_t *start_time, uint32_t *interval,
                         const float **sample_values, size_t *count)
{
  const uint8_t *p;

  p = *input;

  *start_time = TABLE_parse_integer (&p);
  *interval = TABLE_parse_integer (&p);
  *count = TABLE_parse_integer (&p);
  *sample_values = (const float *) p;

  p += sizeof (**sample_values) * *count;

  *input = p;
}

/*****************************************************************************/

struct TABLE_iterate_heap
{
  const void *data;
  size_t size, table;
};

static void
TABLE_heap_push (struct TABLE_iterate_heap *heap, size_t heap_size,
                 const struct TABLE_iterate_heap *entry)
{
  size_t parent, i;

  i = heap_size;

  while (i)
    {
      parent = (i - 1) / 2;

      if (strcmp (heap[parent].data, entry->data) <= 0)
        break;

      heap[i] = heap[parent];
      i = parent;
    }

  heap[i] = *entry;
}

void
TABLE_heap_pop (struct TABLE_iterate_heap *heap, size_t heap_size,
                struct TABLE_iterate_heap *dest)
{
  size_t i, child, parent;

  *dest = heap[0];

  for(i = 0, child = 1; child < heap_size; i = child, child = i * 2 + 1)
    {
      if(child + 1 < heap_size
         && strcmp (heap[child].data, heap[child + 1].data) > 0)
        ++child;

      heap[i] = heap[child];
    }

  if(i != heap_size)
    {
      while (i)
        {
          parent = (i - 1) / 2;

          if (strcmp (heap[parent].data, heap[heap_size - 1].data) <= 0)
            break;

          heap[i] = heap[parent];
          i = parent;
        }

      heap[i] = heap[heap_size - 1];
    }
}

void
table_iterate_multiple (struct table **tables, size_t table_count,
                        table_iterate_callback callback)
{
  struct TABLE_iterate_heap *heap;
  size_t heap_size = 0;

  size_t i, *positions;

  heap = safe_malloc (sizeof (*heap) * table_count);
  positions = safe_malloc (sizeof (*positions) * table_count);

  for (i = 0; i < table_count; ++i)
    {
      struct TABLE_iterate_heap e;

      if (!tables[i]->entry_count)
        continue;

      e.table = i;

      if (!(tables[i]->header->flags & TABLE_FLAG_ORDERED) && !tables[i]->sorted_entries)
        {
          TABLE_build_sorted_entries (tables[i]);

          e.data = tables[i]->sorted_entries[0].data;
          e.size = tables[i]->sorted_entries[0].size;
        }
      else
        {
          e.data = tables[i]->buffer + tables[i]->entries[0];

          if (1 == tables[i]->entry_count)
            e.size = tables[i]->header->index_offset - tables[i]->entries[0];
          else
            e.size = tables[i]->entries[1] - tables[i]->entries[0];
        }

      TABLE_heap_push (heap, heap_size++, &e);

      positions[i] = 1;
    }

  while (heap_size)
    {
      struct TABLE_iterate_heap e;

      TABLE_heap_pop (heap, heap_size, &e);
      --heap_size;

      callback (e.data, e.size);

      if (positions[e.table] < tables[e.table]->entry_count)
        {
          size_t j;

          j = positions[e.table]++;

          if (tables[e.table]->sorted_entries)
            {
              e.data = tables[e.table]->sorted_entries[j].data;
              e.size = tables[e.table]->sorted_entries[j].size;
            }
          else
            {
              e.data = tables[e.table]->buffer + tables[e.table]->entries[j];

              if (j + 1 == tables[e.table]->entry_count)
                e.size = tables[e.table]->header->index_offset - tables[e.table]->entries[j];
              else
                e.size = tables[e.table]->entries[j + 1] - tables[e.table]->entries[j];
            }

          TABLE_heap_push (heap, heap_size++, &e);
        }
    }

  free (positions);
  free (heap);
}
