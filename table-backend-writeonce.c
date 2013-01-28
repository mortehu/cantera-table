#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include "ca-table.h"
#include "io.h"
#include "memory.h"

#define MAGIC 0x6c6261742e692e70ULL
#define MAJOR_VERSION 1
#define MINOR_VERSION 1

#define TMP_SUFFIX ".tmp.XXXXXX"
#define BUFFER_SIZE (1024 * 1024)

/*****************************************************************************/

static void *
CA_wo_open (const char *path, int flags, mode_t mode);

static int
CA_wo_stat (void *handle, struct stat *buf);

static int
CA_wo_utime (void *handle, const struct timeval tv[2]);

static int
CA_wo_sync (void *handle);

static void
CA_wo_close (void *handle);

static int
CA_wo_set_flag (void *handle, enum ca_table_flag flag);

static int
CA_wo_is_sorted (void *handle);

static int
CA_wo_insert_row (void *handle, const char *key,
                  const struct iovec *value, size_t value_count);

static int
CA_wo_seek (void *handle, off_t offset, int whence);

static int
CA_wo_seek_to_key (void *handle, const char *key);

static off_t
CA_wo_offset (void *handle);

static ssize_t
CA_wo_read_row (void *handle, const char **key,
                struct iovec *value);

static int
CA_wo_delete_row (void *handle);

/*****************************************************************************/

struct ca_table_backend CA_table_writeonce =
{
  .open = CA_wo_open,
  .stat = CA_wo_stat,
  .utime = CA_wo_utime,
  .sync = CA_wo_sync,
  .close = CA_wo_close,
  .set_flag = CA_wo_set_flag,
  .is_sorted = CA_wo_is_sorted,
  .insert_row = CA_wo_insert_row,
  .seek = CA_wo_seek,
  .seek_to_key = CA_wo_seek_to_key,
  .offset = CA_wo_offset,
  .read_row = CA_wo_read_row,
  .delete_row = CA_wo_delete_row
};

/*****************************************************************************/

enum CA_wo_flags
{
  CA_WO_FLAG_ASCENDING = 0x0001,
  CA_WO_FLAG_DESCENDING = 0x0002
};

struct CA_wo_header
{
  uint64_t magic;
  uint8_t major_version;
  uint8_t minor_version;
  uint16_t flags;
  uint32_t data_crc32c;
  uint64_t index_offset;
};

struct CA_wo
{
  char *path;
  char *tmp_path;

  int fd;
  int open_flags;

  char *buffer;
  size_t buffer_size, buffer_fill;

  uint64_t write_offset;
  char *prev_key;

  uint32_t crc32c;
  uint16_t flags;

  struct CA_wo_header *header;

  uint64_t *entries;
  size_t entry_alloc, entry_count;

  int no_relative;
  int no_fsync;

  /* Used for read, seek, offset and delete */
  off_t offset;
};

/*****************************************************************************/

static int
CA_wo_write (struct CA_wo *t, const void *data, size_t size) CA_USE_RESULT;

static int
CA_wo_flush (struct CA_wo *t) CA_USE_RESULT;

static int
CA_wo_write_all (int fd, const void *data, size_t size) CA_USE_RESULT;

static int
CA_wo_mmap (struct CA_wo *t) CA_USE_RESULT;

static void
CA_wo_free (struct CA_wo *t);

/*****************************************************************************/

static void *
CA_wo_open (const char *path, int flags, mode_t mode)
{
  struct CA_wo *result;

  if (!(result = safe_malloc (sizeof (*result))))
    return NULL;

  result->fd = -1;
  result->open_flags = flags;

  if (!(result->path = safe_strdup (path)))
    goto fail;

  if (flags & O_CREAT)
    {
      mode_t mask;

      mask = umask (0);
      umask (mask);

      if ((flags & O_TRUNC) != O_TRUNC
          || (((flags & O_RDWR) != O_RDWR) && (flags & O_WRONLY) != O_WRONLY))
        {
          ca_set_error ("O_CREAT only allowed with O_TRUNC | O_RDWR or O_TRUNC | O_WRONLY");

          goto fail;
        }

      if (-1 == asprintf (&result->tmp_path, "%s.tmp.%u.XXXXXX", path, getpid ()))
        {
          ca_set_error ("asprintf failed: %s", strerror (errno));

          goto fail;
        }

      if (-1 == (result->fd = mkstemp (result->tmp_path)))
        {
          ca_set_error ("mkstemp failed on path `%s': %s",
                        result->tmp_path, strerror (errno));

          goto fail;
        }


      if (-1 == fchmod (result->fd, mode & ~mask))
        {
          ca_set_error ("fchmod failed on path `%s': %s",
                        result->tmp_path, strerror (errno));

          goto fail;
        }

      /* We use mmap instead of malloc to be more compatible with read code */

      if (MAP_FAILED == (result->buffer = mmap (NULL, /* address */
                                                BUFFER_SIZE,
                                                PROT_READ | PROT_WRITE,
                                                MAP_PRIVATE | MAP_ANONYMOUS,
                                                0,    /* fd */
                                                0)))  /* offset */
        {
          ca_set_error ("failed to allocate %zu bytes: %s",
                        result->buffer_size, strerror (errno));

          goto fail;
        }

      result->buffer_size = BUFFER_SIZE;
      result->buffer_fill = sizeof (struct CA_wo_header);

      result->flags = CA_WO_FLAG_ASCENDING | CA_WO_FLAG_DESCENDING;
    }
  else
    {
      if ((flags & O_WRONLY) == O_WRONLY)
        {
          ca_set_error ("O_WRONLY only allowed with O_CREAT");

          goto fail;
        }

      if ((flags & O_RDWR) == O_RDWR)
        {
          ca_set_error ("O_RDWR only allowed with O_CREAT");

          goto fail;
        }

      if (-1 == (result->fd = open (path, O_RDONLY)))
        {
          ca_set_error ("Failed to open '%s' for reading: %s", path, strerror (errno));

          goto fail;
        }

      if (-1 == CA_wo_mmap (result))
        goto fail;
    }

  return result;

fail:

  CA_wo_free (result);

  return NULL;
}

static int
CA_wo_stat (void *handle, struct stat *buf)
{
  struct CA_wo *t = handle;

  if (-1 == fstat (t->fd, buf))
    {
      ca_set_error ("fstat failed on '%s'", t->path);

      return -1;
    }

  return 0;
}

static int
CA_wo_utime (void *handle, const struct timeval tv[2])
{
  struct CA_wo *t = handle;

  if (-1 == futimes (t->fd, tv))
    {
      ca_set_error ("futimes failed on '%s'", t->path);

      return -1;
    }

  return 0;
}

static int
CA_wo_sync (void *handle)
{
  struct CA_wo_header header;
  struct CA_wo *t = handle;

  if (!t->tmp_path)
    return 0;

  memset (&header, 0, sizeof (header));
  header.magic = MAGIC; /* Will implicitly store endianness */
  header.major_version = MAJOR_VERSION;
  header.minor_version = MINOR_VERSION;
  header.index_offset = t->write_offset + t->buffer_fill;

  /* Add end pointer for convenience when calculating entry sizes later */
  if (t->entry_count == t->entry_alloc
      && -1 == ARRAY_GROW (&t->entries, &t->entry_alloc))
    return -1;

  t->entries[t->entry_count] = header.index_offset;

  if (-1 == CA_wo_write (t, t->entries, sizeof (*t->entries) * (t->entry_count + 1)))
    return -1;

  if (-1 == CA_wo_flush (t))
    return -1;

  header.flags = t->flags;
  header.data_crc32c = t->crc32c;

  if (-1 == lseek (t->fd, 0, SEEK_SET))
    {
      ca_set_error ("Failed to seek to start of '%s': %s",
                    t->tmp_path, strerror (errno));

      return -1;
    }

  if (-1 == CA_wo_write_all (t->fd, &header, sizeof (header)))
    return -1;

  if (!t->no_fsync && -1 == fsync (t->fd))
    {
      ca_set_error ("Failed to fsync '%s': %s",
                    t->tmp_path, strerror (errno));

      return -1;
    }

  if (-1 == rename (t->tmp_path, t->path))
    {
      ca_set_error ("Failed to rename '%s' to '%s': %s",
                    t->tmp_path, t->path, strerror (errno));

      return -1;
    }

  free (t->tmp_path);
  t->tmp_path = NULL;

  free (t->entries);
  t->entries = NULL;

  munmap (t->buffer, t->buffer_size);
  t->buffer_size = 0;

  if ((t->open_flags & O_RDWR) == O_RDWR)
    return CA_wo_mmap (t);

  return 0;
}

static void
CA_wo_close (void *handle)
{
  CA_wo_free (handle);
}

static int
CA_wo_set_flag (void *handle, enum ca_table_flag flag)
{
  struct CA_wo *t = handle;

  switch (flag)
    {
    case CA_TABLE_NO_RELATIVE:

      t->no_relative = 1;

      break;

    case CA_TABLE_NO_FSYNC:

      t->no_fsync = 1;

      break;

    default:

      ca_set_error ("Unknown flag (%d) passed to table_set_flag", flag);

      return -1;
    }

  return 0;
}

static int
CA_wo_is_sorted (void *handle)
{
  struct CA_wo *t = handle;

  return 0 != (t->header->flags & CA_WO_FLAG_ASCENDING);
}

static int
CA_wo_insert_row (void *handle, const char *key,
                  const struct iovec *value, size_t value_count)
{
  struct CA_wo *t = handle;
  size_t i;
  int cmp = 1;

  if (t->entry_alloc == t->entry_count
      && -1 == ARRAY_GROW (&t->entries, &t->entry_alloc))
    return -1;

  if (t->prev_key)
    {
      cmp = strcmp (key, t->prev_key);

      if (cmp < 0)
        t->flags &= ~CA_WO_FLAG_ASCENDING;
      else if (cmp > 0)
        t->flags &= ~CA_WO_FLAG_DESCENDING;
      else
        goto write_value;
    }

  free (t->prev_key);
  t->prev_key = safe_strdup (key);

  t->entries[t->entry_count++] = t->write_offset + t->buffer_fill;

  if (-1 == CA_wo_write (t, key, strlen (key) + 1))
    return -1;

write_value:

  for (i = 0; i < value_count; ++i)
    {
      if (-1 == CA_wo_write (t, value[i].iov_base, value[i].iov_len))
        return -1;
    }

  return 0;
}

static int
CA_wo_seek (void *handle, off_t offset, int whence)
{
  struct CA_wo *t = handle;
  off_t new_offset;

  switch (whence)
    {
    case SEEK_SET:

      new_offset = offset;

      break;

    case SEEK_CUR:

      new_offset = t->offset + offset;

      break;

    case SEEK_END:

      new_offset = t->entry_count + offset;

      break;

    default:

      assert (!"Invalid 'whence' value");
      errno = EINVAL;

      return -1;
    }

  if (new_offset < 0)
    {
      ca_set_error ("Attempt to seek before start of table");

      return -1;
    }

  if (new_offset > t->entry_count)
    {
      ca_set_error ("Attempt to seek past end of table");

      return -1;
    }

  t->offset = new_offset;

  return 0;
}

static int
CA_wo_seek_to_key (void *handle, const char *key)
{
  struct CA_wo *t = handle;
  size_t first = 0, middle, half, count;

  count = t->entry_count;

  if (0 != (t->header->flags & CA_WO_FLAG_ASCENDING))
    {
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
          else if (ca_likely (cmp > 0))
            count = half;
          else
            {
              t->offset = middle;

              return 1;
            }
        }
    }
  else if (0 != (t->header->flags & CA_WO_FLAG_DESCENDING))
    {
      while (count > 0)
        {
          int cmp;

          half = count >> 1;
          middle = first + half;

          cmp = strcmp (t->buffer + t->entries[middle], key);

          if (cmp > 0)
            {
              first = middle + 1;
              count -= half + 1;
            }
          else if (ca_likely (cmp < 0))
            count = half;
          else
            {
              t->offset = middle;

              return 1;
            }
        }
    }
  else
    {
      off_t offset;

      for (offset = 0; offset < t->entry_count; ++offset)
        {
          if (!strcmp (t->buffer + t->entries[offset], key))
            {
              t->offset = offset;

              return 1;
            }
        }
    }

  return 0;
}

static off_t
CA_wo_offset (void *handle)
{
  struct CA_wo *t = handle;

  return t->offset;
}

static ssize_t
CA_wo_read_row (void *handle, const char **out_key,
                struct iovec *value)
{
  struct CA_wo *t = handle;
  const char *key;
  size_t key_length;

  if (t->offset < 0)
    {
      ca_set_error ("Current offset is negative");

      return -1;
    }

  if (t->offset >= t->entry_count)
    return 0;

  key = t->buffer + t->entries[t->offset];
  key_length = strlen (key) + 1;

  if (out_key)
    *out_key = key;

  if (value)
    {
      value->iov_base = (void *) (key + key_length);
      value->iov_len = t->entries[t->offset + 1] - t->entries[t->offset] - key_length;
    }

  ++t->offset;

  return 1;
}

static int
CA_wo_delete_row (void *handle)
{
  ca_set_error ("Deleting from write-once tables is not supported");

  return -1;
}

/*****************************************************************************/

static int
CA_wo_write (struct CA_wo *t, const void *data, size_t size)
{
  if (t->buffer_fill + size > t->buffer_size)
    {
      if (-1 == CA_wo_flush (t))
        return -1;

      if (size >= t->buffer_size)
        return CA_wo_write_all (t->fd, data, size);
    }

  memcpy (t->buffer + t->buffer_fill, data, size);
  t->buffer_fill += size;

  return 0;
}

static int
CA_wo_flush (struct CA_wo *t)
{
  if (-1 == CA_wo_write_all (t->fd, t->buffer, t->buffer_fill))
    return -1;

  t->write_offset += t->buffer_fill;

  t->crc32c = ca_crc32c (t->crc32c, t->buffer, t->buffer_fill);

  t->buffer_fill = 0;

  return 0;
}

static int
CA_wo_write_all (int fd, const void *data, size_t size)
{
  const char *cdata = data;
  ssize_t ret = 1;

  /* XXX: Register write failure and prevent committing table */

  while (size)
    {
      if (0 > (ret = write (fd, cdata, size)))
        {
          if (ret == 0)
            ca_set_error ("write returned 0");
          else
            ca_set_error ("write failed: %s", strerror (errno));

          return -1;
        }

      size -= ret;
      cdata += ret;
    }

  return 0;
}

static int
CA_wo_mmap (struct CA_wo *t)
{
  off_t end = 0;

  if (-1 == (end = lseek (t->fd, 0, SEEK_END)))
    {
      ca_set_error ("Failed to seek to end of '%s': %s", t->path, strerror (errno));

      return -1;
    }

  if (end < sizeof (struct CA_wo_header))
    {
      ca_set_error ("Table file '%s' is too small.  Got %zu bytes, expected %zu",
                    t->path, (size_t) end, sizeof (struct CA_wo_header));

      return -1;
    }

  t->buffer_size = end;
  t->buffer_fill = end;

  if (MAP_FAILED == (t->buffer = mmap (NULL, end, PROT_READ, MAP_SHARED, t->fd, 0)))
    {
      ca_set_error ("Failed to mmap '%s': %s", t->path, strerror (errno));

      return -1;
    }

  t->header = (struct CA_wo_header *) t->buffer;

  if (t->header->major_version != MAJOR_VERSION)
    {
      ca_set_error ("Unsupported major version %u in '%s'",
                    t->header->major_version, t->path);

      return -1;
    }

  if (t->header->magic != MAGIC)
    {
      ca_set_error ("Unexpected magic header in '%s'. Got %016llx, expected %016llx", t->path,
                    (unsigned long long) t->header->magic,
                    (unsigned long long) MAGIC);

      return -1;
    }

  t->entries = (uint64_t *) (t->buffer + t->header->index_offset);
  t->entry_alloc = (t->buffer_size - t->header->index_offset) / sizeof (uint64_t);
  t->entry_count = t->entry_alloc - 1; /* Don't count end pointer */

  return 0;
}

static void
CA_wo_free (struct CA_wo *t)
{
  if (t->fd != -1)
    close (t->fd);

  if (t->buffer_size)
    munmap (t->buffer, t->buffer_size);

  if (t->tmp_path)
    {
      unlink (t->tmp_path);
      free (t->tmp_path);

      free (t->entries); /* Part of t->buffer otherwise */
    }

  free (t->path);
  free (t);
}
