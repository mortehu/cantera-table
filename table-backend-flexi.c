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
CA_flexi_open (const char *path, int flags, mode_t mode);

static int
CA_flexi_stat (void *handle, struct stat *buf);

static int
CA_flexi_utime (void *handle, const struct timeval tv[2]);

static int
CA_flexi_sync (void *handle);

static void
CA_flexi_close (void *handle);

static int
CA_flexi_set_flag (void *handle, enum ca_table_flag flag);

static int
CA_flexi_is_sorted (void *handle);

static int
CA_flexi_insert_row (void *handle, const struct iovec *value, size_t value_count);

static int
CA_flexi_seek (void *handle, off_t offset, int whence);

static int
CA_flexi_seek_to_key (void *handle, const char *key);

static off_t
CA_flexi_offset (void *handle);

static ssize_t
CA_flexi_read_row (void *handle, struct iovec *value, size_t value_count);

static int
CA_flexi_delete_row (void *handle);

/*****************************************************************************/

struct ca_table_backend CA_table_flexi =
{
  .open = CA_flexi_open,
  .stat = CA_flexi_stat,
  .utime = CA_flexi_utime,
  .sync = CA_flexi_sync,
  .close = CA_flexi_close,
  .set_flag = CA_flexi_set_flag,
  .is_sorted = CA_flexi_is_sorted,
  .insert_row = CA_flexi_insert_row,
  .seek = CA_flexi_seek,
  .seek_to_key = CA_flexi_seek_to_key,
  .offset = CA_flexi_offset,
  .read_row = CA_flexi_read_row,
  .delete_row = CA_flexi_delete_row
};

/*****************************************************************************/

struct CA_flexi_row_header
{
  uint64_t insert_xid;
  uint64_t delete_xid;
  uint32_t size;
  uint32_t flags;
};

struct CA_flexi
{
  char *path;

  int fd;
  int open_flags;

  uint8_t *map;
  off_t map_size;

  off_t offset;
};

/*****************************************************************************/

static int
CA_flexi_remap (struct CA_flexi *t, off_t new_map_size);

static void
CA_flexi_free (struct CA_flexi *t);

/*****************************************************************************/

static void *
CA_flexi_open (const char *path, int flags, mode_t mode)
{
  struct CA_flexi *result;

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

  CA_flexi_free (result);

  return NULL;
}

static int
CA_flexi_stat (void *handle, struct stat *buf)
{
  struct CA_flexi *t = handle;

  if (-1 == fstat (t->fd, buf))
    {
      ca_set_error ("fstat failed on '%s'", t->path);

      return -1;
    }

  return 0;
}

static int
CA_flexi_utime (void *handle, const struct timeval tv[2])
{
  struct CA_flexi *t = handle;

  if (-1 == futimes (t->fd, tv))
    {
      ca_set_error ("futimes failed on '%s'", t->path);

      return -1;
    }

  return 0;
}

static int
CA_flexi_sync (void *handle)
{
  struct CA_flexi *t = handle;

  if (t->map_size
      && -1 == msync (t->map, t->map_size, MS_SYNC))
    {
      ca_set_error ("msync failed: %s", strerror (errno));

      return -1;
    }

  return 0;
}

static void
CA_flexi_close (void *handle)
{
  CA_flexi_free (handle);
}

static int
CA_flexi_set_flag (void *handle, enum ca_table_flag flag)
{
  ca_set_error ("Flag %d not support by flexi tables", flag);

  return -1;
}

static int
CA_flexi_is_sorted (void *handle)
{
  return 0;
}

static int
CA_flexi_insert_row (void *handle, const struct iovec *value, size_t value_count)
{
  struct CA_flexi *t = handle;
  size_t i;
  struct CA_flexi_row_header header;

  off_t write_offset, new_map_size;

  header.insert_xid = ca_xid;
  header.delete_xid = CA_INVALID_XID;
  header.size = 0;
  header.flags = 0;

  for (i = 0; i < value_count; ++i)
    header.size += value[i].iov_len;

  if (-1 == (write_offset = lseek (t->fd, 0, SEEK_END)))
    {
      ca_set_error ("lseek failed: %s", strerror (errno));

      return -1;
    }

  /* Align to 8 byte boundary */
  write_offset = (write_offset + 7) & ~7;

  new_map_size = write_offset + sizeof (header) + header.size;

  if (-1 == ftruncate (t->fd, new_map_size))
    {
      ca_set_error ("ftruncate failed: %s", strerror (errno));

      return -1;
    }

  if (-1 == CA_flexi_remap (t, new_map_size))
    return -1;

  memcpy (t->map + write_offset, &header, sizeof (header));
  write_offset += sizeof (header);

  for (i = 0; i < value_count; ++i)
    {
      memcpy (t->map + write_offset, value[i].iov_base, value[i].iov_len);
      write_offset += value[i].iov_len;
    }

  return 0;
}

static int
CA_flexi_seek (void *handle, off_t offset, int whence)
{
  struct CA_flexi *t = handle;
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
CA_flexi_seek_to_key (void *handle, const char *key)
{
  ca_set_error ("Cannot seek to key in flexi tables");

  return -1;
}

static off_t
CA_flexi_offset (void *handle)
{
  struct CA_flexi *t = handle;

  return t->offset;
}

static ssize_t
CA_flexi_read_row (void *handle, struct iovec *value, size_t value_size)
{
  struct CA_flexi *t = handle;
  const struct CA_flexi_row_header *header = NULL;

  /* Loop until we find a record visible in the current transaction */
  for (;;)
    {
      /* Align to 8 byte boundary */
      t->offset = (t->offset + 7) & ~7;

      if (t->offset >= t->map_size)
        {
          off_t new_map_size;

          if (-1 == (new_map_size = lseek (t->fd, 0, SEEK_END)))
            {
              ca_set_error ("lseek failed: %s", strerror (errno));

              return -1;
            }

          if (t->offset >= new_map_size)
            return 0;

          if (-1 == CA_flexi_remap (t, new_map_size))
            return -1;
        }

      if (t->offset + sizeof (*header) > t->map_size)
        {
          ca_set_error ("Record truncated in header");

          return -1;
        }

      header = (const struct CA_flexi_row_header *) (t->map + t->offset);

      if (header->insert_xid > ca_xid
          || header->delete_xid <= ca_xid)
        {
          t->offset += sizeof (*header) + header->size;

          continue;
        }

      if (value_size < 1)
        {
          t->offset += sizeof (*header) + header->size;

          return 1;
        }

      if (t->offset + sizeof (*header) + header->size > t->map_size)
        {
          ca_set_error ("Table truncated (expected %lu + %lu bytes, got %lu)",
                        (unsigned long) t->offset,
                        (unsigned long) header->size,
                        (unsigned long) t->map_size);

          return -1;
        }

      value[0].iov_base = t->map + t->offset + sizeof (*header);
      value[0].iov_len = header->size;

      t->offset += sizeof (*header) + header->size;

      return 1;
    }
}

static int
CA_flexi_delete_row (void *handle)
{
  struct CA_flexi *t = handle;
  struct CA_flexi_row_header *header = NULL;

  /* Align to 8 byte boundary */
  t->offset = (t->offset + 7) & ~7;

  if (t->offset >= t->map_size)
    {
      off_t new_map_size;

      if (-1 == (new_map_size = lseek (t->fd, 0, SEEK_END)))
        {
          ca_set_error ("lseek failed: %s", strerror (errno));

          return -1;
        }

      if (t->offset >= new_map_size)
        {
          ca_set_error ("Attempt to delete past end of table");

          return -1;
        }

      if (-1 == CA_flexi_remap (t, new_map_size))
        return -1;
    }

  if (t->offset + sizeof (*header) > t->map_size)
    {
      ca_set_error ("Record truncated");

      return -1;
    }

  header = (struct CA_flexi_row_header *) (t->map + t->offset);

  /* Locking should prevent multiple transactions from deleting the same row */
  assert (header->delete_xid == CA_INVALID_XID
          || header->delete_xid == ca_xid);

  header->delete_xid = ca_xid;

  t->offset += sizeof (*header) + header->size;

  return 0;
}

/*****************************************************************************/

static int
CA_flexi_remap (struct CA_flexi *t, off_t new_map_size)
{
  void *new_map;

  if (!t->map_size)
    {
      if (MAP_FAILED == (new_map = mmap (NULL, new_map_size,
                                         PROT_READ | PROT_WRITE,
                                         MAP_SHARED,
                                         t->fd,
                                         0)))
        {
          ca_set_error ("mmap failed: %s", strerror (errno));

          return -1;
        }
    }
  else
    {
      if (MAP_FAILED == (new_map = mremap (t->map, t->map_size,
                                           new_map_size, MREMAP_MAYMOVE)))
        {
          ca_set_error ("mremap failed: %s", strerror (errno));

          return -1;
        }
    }

  t->map = new_map;
  t->map_size = new_map_size;

  return 0;
}

static void
CA_flexi_free (struct CA_flexi *t)
{
  if (t->fd != -1)
    close (t->fd);

  if (t->map_size)
    munmap (t->map, t->map_size);

  free (t->path);
  free (t);
}
