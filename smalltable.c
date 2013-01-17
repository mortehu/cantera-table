#include <assert.h>
#include <errno.h>
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

#include "error.h"
#include "io.h"
#include "memory.h"
#include "smalltable.h"
#include "smalltable-internal.h"

#define MAGIC 0x6c6261742e692e70ULL
#define MAJOR_VERSION 0
#define MINOR_VERSION 0

#define TMP_SUFFIX ".tmp.XXXXXX"
#define BUFFER_SIZE (1024 * 1024)

#ifdef __GNUC__
#  define likely(x)       __builtin_expect((x),1)
#  define unlikely(x)     __builtin_expect((x),0)
#else
#  define likely(x)       (x)
#  define unlikely(x)     (x)
#endif

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
  off_t end = 0;

  result = safe_malloc (sizeof (*result));
  result->fd = -1;

  result->path = safe_strdup (path);

  if (-1 == (result->fd = open (path, O_RDONLY)))
    {
      ca_set_error ("Failed to open '%s' for reading: %s", path, strerror (errno));

      goto fail;
    }

  if (-1 == (end = lseek (result->fd, 0, SEEK_END)))
    {
      ca_set_error ("Failed to seek to end of '%s': %s", path, strerror (errno));

      goto fail;
    }

  if (end < sizeof (struct TABLE_header))
    {
      ca_set_error ("Table file '%s' is too small.  Got %zu bytes, expected %zu",
                    path, (size_t) end, sizeof (struct TABLE_header));

      goto fail;
    }

  result->buffer_size = end;
  result->buffer_fill = end;

  if (MAP_FAILED == (result->buffer = mmap (NULL, end, PROT_READ, MAP_SHARED, result->fd, 0)))
    {
      ca_set_error ("Failed to mmap '%s': %s", path, strerror (errno));

      goto fail;
    }

  result->header = (struct TABLE_header *) result->buffer;

  if (result->header->magic != MAGIC)
    {
      ca_set_error ("Unexpected magic header in '%s'. Got %016llx, expected %016llx", path,
                    (unsigned long long) result->header->magic,
                    (unsigned long long) MAGIC);

      goto fail;
    }

  result->entries = (uint64_t *) (result->buffer + result->header->index_offset);
  result->entry_alloc = (result->buffer_size - result->header->index_offset) / sizeof (uint64_t);
  result->entry_count = result->entry_alloc;

  return result;

fail:

  if (result)
    {
      if (result->buffer)
        munmap (result->buffer, end);

      if (result->fd >= 0)
        close (result->fd);

      free (result->path);
      free (result);
    }

  return NULL;
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

int
table_is_sorted (const struct table *t)
{
  return 0 != (t->header->flags & TABLE_FLAG_ORDERED);
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


/*****************************************************************************/

struct TABLE_entry
{
  const void *data;
  size_t size;
};

static int
TABLE_entrycmp (const void *vlhs, const void *vrhs)
{
  const struct TABLE_entry *lhs = vlhs;
  const struct TABLE_entry *rhs = vrhs;

  return strcmp (lhs->data, rhs->data);
}

void
table_sort (struct table *output, struct table *input)
{
  struct TABLE_entry *sorted_entries;
  const char *prev_key = NULL;
  size_t i;

  sorted_entries = safe_malloc (sizeof (*sorted_entries) * input->entry_count);

  for (i = 0; i + 1 < input->entry_count; ++i)
    {
      sorted_entries[i].data = input->buffer + input->entries[i];
      sorted_entries[i].size = input->entries[i + 1] - input->entries[i];
    }

  sorted_entries[i].data = input->buffer + input->entries[i];
  sorted_entries[i].size = input->header->index_offset - input->entries[i];

  qsort (sorted_entries, input->entry_count, sizeof (*sorted_entries),
         TABLE_entrycmp);

  for (i = 0; i < input->entry_count; ++i)
    {
      const char *key;
      size_t key_length;
      int cmp = 1;

      key = sorted_entries[i].data;
      key_length = strlen (key + 1);

      if (!prev_key || (cmp = strcmp (key, prev_key)))
        {
          assert (cmp > 0);

          if (output->entry_alloc == output->entry_count)
            ARRAY_GROW (&output->entries, &output->entry_alloc);

          output->entries[output->entry_count++] = output->write_offset + output->buffer_fill;

          TABLE_write (output, key, strlen (key) + 1);

          prev_key = key;
        }

      TABLE_write (output, (const char *) sorted_entries[i].data + key_length,
                   sorted_entries[i].size - key_length);
    }

  free (output->prev_key);
  output->prev_key = NULL;
  output->prev_time = 0;

  free (sorted_entries);
}

/*****************************************************************************/

void
table_iterate (struct table *t, table_iterate_callback callback,
               enum table_order order, void *opaque)
{
  size_t i;
  const char *key;
  size_t key_length;

  if (!t->entry_count)
    return;

  switch (order)
    {
    case TABLE_ORDER_PHYSICAL:

      break;

    case TABLE_ORDER_KEY:

      if (!(t->header->flags & TABLE_FLAG_ORDERED))
        errx (EX_DATAERR, "Table '%s' is not sorted", t->path);

      break;

    default:

      errx (EX_SOFTWARE, "Unknown sort order (%d)", order);
    }

  for (i = 0; i + 1 < t->entry_count; ++i)
    {
      key = t->buffer + t->entries[i];
      key_length = strlen (key) + 1;

      callback (key, key + key_length,
                t->entries[i + 1] - t->entries[i] - key_length, opaque);
    }

  key = t->buffer + t->entries[i];
  key_length = strlen (key) + 1;

  callback (key, key + key_length,
            t->header->index_offset - t->entries[i] - key_length, opaque);
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

static void
TABLE_heap_replace_top (struct TABLE_iterate_heap *heap, size_t heap_size,
                        const struct TABLE_iterate_heap *entry)
{
  size_t i, child, parent;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for(i = 1, child = 2; child <= heap_size; i = child, child = i << 1)
    {
      /* Move the smaller child up the tree */

      if(child + 1 <= heap_size
         && strcmp (heap[child].data, heap[child + 1].data) > 0)
        ++child;

      heap[i] = heap[child];
    }

  /* Move ancestor chain down into hole, until we can insert the new entry */

  while (i > 1)
    {
      parent = i >> 1;

      if (strcmp (heap[parent].data, entry->data) <= 0)
        break;

      heap[i] = heap[parent];
      i = parent;
    }

  heap[i] = *entry;
}

static void
TABLE_heap_pop (struct TABLE_iterate_heap *heap, size_t heap_size)
{
  size_t i, child, parent;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for(i = 1, child = 2; child <= heap_size; i = child, child = i << 1)
    {
      /* Move the smaller child up the tree */

      if(child + 1 <= heap_size
         && strcmp (heap[child].data, heap[child + 1].data) > 0)
        ++child;

      heap[i] = heap[child];
    }

  if(i == heap_size)
    return;

  /* Hole did not end up at tail: Move ancestor chain down into hole, until we
   * can insert the tail element as an ancestor */

  while (i > 0)
    {
      parent = i >> 1;

      if (strcmp (heap[parent].data, heap[heap_size].data) <= 0)
        break;

      heap[i] = heap[parent];
      i = parent;
    }

  heap[i] = heap[heap_size];
}

void
table_iterate_multiple (struct table **tables, size_t table_count,
                        table_iterate_callback callback, void *opaque)
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

      if (!(tables[i]->header->flags & TABLE_FLAG_ORDERED))
        errx (EX_DATAERR, "Table '%s' is not sorted.  Use ts-sort", tables[i]->path);

      e.data = tables[i]->buffer + tables[i]->entries[0];

      if (1 == tables[i]->entry_count)
        e.size = tables[i]->header->index_offset - tables[i]->entries[0];
      else
        e.size = tables[i]->entries[1] - tables[i]->entries[0];

      TABLE_heap_push (heap, heap_size++, &e);

      positions[i] = 1;
    }

  while (heap_size)
    {
      struct TABLE_iterate_heap e;
      const char *key;
      size_t key_length;

      e = heap[0];

      key = e.data;
      key_length = strlen (key) + 1;

      callback (key, key + key_length, e.size - key_length, opaque);

      if (positions[e.table] < tables[e.table]->entry_count)
        {
          size_t j;

          j = positions[e.table]++;

          e.data = tables[e.table]->buffer + tables[e.table]->entries[j];

          if (j + 1 == tables[e.table]->entry_count)
            e.size = tables[e.table]->header->index_offset - tables[e.table]->entries[j];
          else
            e.size = tables[e.table]->entries[j + 1] - tables[e.table]->entries[j];

          TABLE_heap_replace_top (heap, heap_size, &e);
        }
      else
        {
          TABLE_heap_pop (heap, heap_size);
          --heap_size;
        }
    }

  free (positions);
  free (heap);
}

const void *
table_lookup (struct table *t, const char *key, size_t *size)
{
  size_t first = 0, half, middle, count;
  size_t key_length;

  if (!(t->header->flags & TABLE_FLAG_ORDERED))
    {
      for (middle = 0; middle < t->entry_count; ++middle)
        {
          if (!strcmp (t->buffer + t->entries[middle], key))
            goto found;
        }

      if (!strcmp (t->buffer + t->entries[middle], key))
        goto found;

      return NULL;
    }

  count = t->entry_count;

  while (count > 0)
    {
      int cmp;

      half = count >> 1;
      middle = first + half;

      cmp = strcmp (t->buffer + t->entries[middle], key);

      if (cmp < 0)
        {
          first = middle + 1;
          count -= half + 1;
        }
      else if (likely (cmp > 0))
        count = half;
      else
        goto found;
    }

  return NULL;

found:

  if (middle + 1 < t->entry_count)
    *size = t->entries[middle + 1] - t->entries[middle];
  else
    *size = t->header->index_offset - t->entries[middle];

  key_length = strlen (t->buffer + t->entries[middle]) + 1;
  *size -= key_length;

  return t->buffer + t->entries[middle] + key_length;
}
