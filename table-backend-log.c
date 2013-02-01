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
#include "memory.h"

/*****************************************************************************/

static void *
CA_log_open (const char *path, int flags, mode_t mode);

static int
CA_log_stat (void *handle, struct stat *buf);

static int
CA_log_utime (void *handle, const struct timeval tv[2]);

static int
CA_log_sync (void *handle);

static void
CA_log_close (void *handle);

static int
CA_log_set_flag (void *handle, enum ca_table_flag flag);

static int
CA_log_is_sorted (void *handle);

static int
CA_log_insert_row (void *handle, const struct iovec *value, size_t value_count);

static int
CA_log_seek (void *handle, off_t offset, int whence);

static int
CA_log_seek_to_key (void *handle, const char *key);

static off_t
CA_log_offset (void *handle);

static ssize_t
CA_log_read_row (void *handle, struct iovec *value, size_t value_count);

static int
CA_log_delete_row (void *handle);

/*****************************************************************************/

struct ca_table_backend CA_table_log =
{
  .open = CA_log_open,
  .stat = CA_log_stat,
  .utime = CA_log_utime,
  .sync = CA_log_sync,
  .close = CA_log_close,
  .set_flag = CA_log_set_flag,
  .is_sorted = CA_log_is_sorted,
  .insert_row = CA_log_insert_row,
  .seek = CA_log_seek,
  .seek_to_key = CA_log_seek_to_key,
  .offset = CA_log_offset,
  .read_row = CA_log_read_row,
  .delete_row = CA_log_delete_row
};

/*****************************************************************************/

struct CA_log
{
  char *path;

  int fd;
  int open_flags;

  struct ca_file_buffer *write_buffer;

  uint8_t *read_map;
  off_t read_map_size;

  off_t offset;
};

/*****************************************************************************/

static void
CA_log_free (struct CA_log *t);

/*****************************************************************************/

static void *
CA_log_open (const char *path, int flags, mode_t mode)
{
  struct CA_log *result;

  if (((flags & O_RDWR) == O_RDWR
       || (flags & O_WRONLY) == O_WRONLY)
      && ((flags & O_APPEND) != O_APPEND))
    {
      ca_set_error ("O_RDWR and O_WRONLY only allowed with O_APPEND");

      return NULL;
    }

  if (!(result = safe_malloc (sizeof (*result))))
    return NULL;

  result->fd = -1;
  result->open_flags = flags;

  if (!(result->path = safe_strdup (path)))
    goto fail;

  if (-1 == (result->fd = open (path, flags, mode)))
    {
      ca_set_error ("open failed on path `%s': %s", path, strerror (errno));

      goto fail;
    }

  if (!(result->write_buffer = ca_file_buffer_alloc (result->fd)))
    goto fail;

  return result;

fail:

  CA_log_free (result);

  return NULL;
}

static int
CA_log_stat (void *handle, struct stat *buf)
{
  struct CA_log *t = handle;

  if (-1 == fstat (t->fd, buf))
    {
      ca_set_error ("fstat failed on '%s'", t->path);

      return -1;
    }

  return 0;
}

static int
CA_log_utime (void *handle, const struct timeval tv[2])
{
  struct CA_log *t = handle;

  if (-1 == futimes (t->fd, tv))
    {
      ca_set_error ("futimes failed on '%s'", t->path);

      return -1;
    }

  return 0;
}

static int
CA_log_sync (void *handle)
{
  struct CA_log *t = handle;

  if (-1 == ca_file_buffer_flush (t->write_buffer))
    return -1;

  if (-1 == fdatasync (t->fd))
    {
      ca_set_error ("fdatasync failed: %s", strerror (errno));

      return -1;
    }

  return 0;
}

static void
CA_log_close (void *handle)
{
  CA_log_free (handle);
}

static int
CA_log_set_flag (void *handle, enum ca_table_flag flag)
{
  ca_set_error ("Flag %d not support by log tables", flag);

  return -1;
}

static int
CA_log_is_sorted (void *handle)
{
  return 0;
}

static int
CA_log_insert_row (void *handle, const struct iovec *value, size_t value_count)
{
  struct CA_log *t = handle;
  size_t i;

  struct iovec *iov;
  uint64_t size = 0;

  int result = -1;

  if (!(iov = safe_malloc ((value_count + 1) * sizeof (*iov))))
    return -1;

  for (i = 0; i < value_count; ++i)
    size += value[i].iov_len;

  size += sizeof (size);

  iov[0].iov_base = &size;
  iov[0].iov_len = sizeof (size);
  memcpy (iov + 1, value, value_count * sizeof (*iov));

  if (-1 == ca_file_buffer_writev (t->write_buffer, iov, value_count + 1))
    goto done;

  t->read_map = MAP_FAILED;

  result = 0;

done:

  free (iov);

  return result;
}

static int
CA_log_seek (void *handle, off_t offset, int whence)
{
  struct CA_log *t = handle;
  off_t new_offset;

  ca_clear_error ();

  switch (whence)
    {
    case SEEK_SET:

      new_offset = offset;

      break;

    case SEEK_CUR:

      new_offset = t->offset + offset;

      break;

    case SEEK_END:

      if (-1 == (new_offset = lseek (t->fd, 0, SEEK_END)))
        {
          ca_set_error ("lseek failed: %s", strerror (errno));

          return -1;
        }

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

  t->offset = new_offset;

  return 0;
}

static int
CA_log_seek_to_key (void *handle, const char *key)
{
  ca_set_error ("Cannot seek to key in log tables");

  return -1;
}

static off_t
CA_log_offset (void *handle)
{
  struct CA_log *t = handle;

  return t->offset;
}

static ssize_t
CA_log_read_row (void *handle, struct iovec *value, size_t value_size)
{
  struct CA_log *t = handle;
  uint64_t size;

  if (-1 == ca_file_buffer_flush (t->write_buffer))
    return -1;

  if (t->offset >= t->read_map_size)
    {
      void *new_map;
      off_t new_map_size;

      if (-1 == (new_map_size = lseek (t->fd, 0, SEEK_END)))
        {
          ca_set_error ("lseek failed: %s", strerror (errno));

          return -1;
        }

      if (t->offset >= new_map_size)
        return 0;

      if (!t->read_map_size)
        {
          if (MAP_FAILED == (new_map = mmap (NULL, new_map_size, PROT_READ, MAP_SHARED, t->fd, 0)))
            {
              ca_set_error ("mmap failed: %s", strerror (errno));

              return -1;
            }
        }
      else
        {
          if (MAP_FAILED == (new_map = mremap (t->read_map, t->read_map_size,
                                               new_map_size, MREMAP_MAYMOVE)))
            {
              ca_set_error ("mremap failed: %s", strerror (errno));

              return -1;
            }
        }

      t->read_map = new_map;
      t->read_map_size = new_map_size;
    }

  if (t->offset + sizeof (size) > t->read_map_size)
    {
      ca_set_error ("Record truncated in header");

      return -1;
    }

  memcpy (&size, t->read_map + t->offset, sizeof (size));

  if (!size || value_size < 1)
    {
      t->offset += size;

      memset (value, 0, value_size * sizeof (*value));

      return 1;
    }

  if (t->offset + size > t->read_map_size)
    {
      ca_set_error ("Record truncated");

      return -1;
    }

  value[0].iov_base = t->read_map + t->offset + sizeof (size);
  value[0].iov_len = size - sizeof (size);

  t->offset += size;

  return 1;
}

static int
CA_log_delete_row (void *handle)
{
  ca_set_error ("Deleting from log tables is not supported");

  return -1;
}

/*****************************************************************************/

static void
CA_log_free (struct CA_log *t)
{
  ca_file_buffer_flush (t->write_buffer);

  if (t->fd != -1)
    close (t->fd);

  if (t->read_map != MAP_FAILED)
    munmap (t->read_map, t->read_map_size);

  free (t->path);
  free (t);
}
