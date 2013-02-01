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

  off_t offset;

  int failed;
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

  if (-1 == fdatasync (t->fd))
    {
      ca_set_error ("fdatasync failed: %s", strerror (errno));

      t->failed = 1;

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
  ssize_t ret;

  off_t result = -1;

  if (t->failed)
    {
      ca_set_error ("Cannot write to log table because a previous write failed");

      return -1;
    }

  if (!(iov = safe_malloc ((value_count + 1) * sizeof (*iov))))
    return -1;

  for (i = 0; i < value_count; ++i)
    size += value[i].iov_len;

  size += sizeof (size);

  iov[0].iov_base = &size;
  iov[0].iov_len = sizeof (size);
  memcpy (iov + 1, value, value_count * sizeof (*iov));


  if (size != (ret = writev (t->fd, iov, value_count + 1)))
    {
      if (ret == -1)
        ca_set_error ("writev failed: %s", strerror (errno));
      else
        ca_set_error ("Short writev; atomicity cannot be guaranteed");

      t->failed = 1;

      goto done;
    }

  /* Get offset after writev, since we're using O_APPEND and writev may seek
   * before writing */

  if (-1 == (result = lseek (t->fd, 0, SEEK_CUR)))
    goto done;

  assert (result >= size);

  result -= size;

done:

  free (iov);

  return result;
}

static int
CA_log_seek (void *handle, off_t offset, int whence)
{
  struct CA_log *t = handle;

  ca_clear_error ();

  return lseek (t->fd, offset, whence);
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

  ssize_t ret;

  if (sizeof (size) != (ret = pread (t->fd, &size, sizeof (size), t->offset)))
    {
      if (ret == 0)
        return 0;

      if (ret == -1)
        ca_set_error ("pread failed: %s", strerror (errno));
      else
        ca_set_error ("Short pread while reading row header");

      return -1;
    }

  if (!size || value_size < 1)
    {
      t->offset += size;

      memset (value, 0, value_size * sizeof (*value));

      return 1;
    }

  size -= sizeof (size);

  /* XXX: Leak!  Use statement arena instead */

  if (!(value[0].iov_base = safe_malloc (size)))
    return -1;

  value[0].iov_len = size;

  if (size != (ret = pread (t->fd, value[0].iov_base, size, t->offset + sizeof (size))))
    {
      if (ret == -1)
        ca_set_error ("pread failed: %s", strerror (errno));
      else
        ca_set_error ("Short pread reading row data");

      return -1;
    }

  t->offset += size + sizeof (size);

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
  if (t->fd != -1)
    close (t->fd);

  free (t->path);
  free (t);
}
