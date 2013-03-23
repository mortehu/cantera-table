/*
    Write Once Read Many database table backend
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
#include <errno.h>
#include <limits.h>
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

#define MAGIC 0x6c6261742e692e70ULL
#define MAJOR_VERSION 2
#define MINOR_VERSION 0

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
CA_wo_insert_row (void *handle, const struct iovec *value, size_t value_count);

static int
CA_wo_seek (void *handle, off_t offset, int whence);

static int
CA_wo_seek_to_key (void *handle, const char *key);

static off_t
CA_wo_offset (void *handle);

static ssize_t
CA_wo_read_row (void *handle, struct iovec *value);

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

  struct ca_file_buffer *write_buffer;
  uint64_t write_offset;

  uint8_t *buffer;
  size_t buffer_size, buffer_fill;

  uint32_t crc32c;

  struct CA_wo_header *header;

  uint64_t entry_count;

  union
    {
      uint64_t *u64;
      uint32_t *u32;
      uint16_t *u16;
    } index;
  uint64_t index_size;
  unsigned int index_bits;

  int no_relative;
  int no_fsync;

  /* Used for read, seek, offset and delete */
  off_t offset;
};

/*****************************************************************************/

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

  if (!(result = ca_malloc (sizeof (*result))))
    return NULL;

  result->fd = -1;
  result->buffer = MAP_FAILED;
  result->open_flags = flags;

  if (!(result->path = ca_strdup (path)))
    goto fail;

  if ((flags & O_WRONLY) == O_WRONLY)
    {
      ca_set_error ("O_WRONLY is not allowed");

      goto fail;
    }

  if (flags & O_CREAT)
    {
      struct CA_wo_header dummy_header;
      mode_t mask;

      mask = umask (0);
      umask (mask);

      if ((flags & O_TRUNC) != O_TRUNC
          || ((flags & O_RDWR) != O_RDWR))
        {
          ca_set_error ("O_CREAT only allowed with O_TRUNC | O_RDWR");

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

      if (!(result->write_buffer = ca_file_buffer_alloc (result->fd)))
        goto fail;

      memset (&dummy_header, 0, sizeof (dummy_header));

      if (-1 == ca_file_buffer_write (result->write_buffer,
                                      &dummy_header,
                                      sizeof (dummy_header)))
        goto fail;

      result->write_offset = sizeof (dummy_header);
    }
  else
    {
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

static uint64_t
CA_wo_hash (const char *str)
{
  uint64_t result = 0x2257d6803a6f1b2ULL;

  while (*str)
    result = result * 31 + (unsigned char) *str++;

  return result;
}

static int
CA_wo_build_index (struct CA_wo *t)
{
  struct iovec row;
  const char *prev_key = NULL;
  unsigned int flags = CA_WO_FLAG_ASCENDING | CA_WO_FLAG_DESCENDING;
  int ret;

  if (-1 == CA_wo_seek (t, 0, SEEK_SET))
    return -1;

  for (;;)
    {
      uint64_t offset, hash;

      uint64_t fib[2] = { 2, 1 };
      unsigned int collisions = 0;
      int cmp;

      offset = CA_wo_offset (t);

      if (1 != (ret = CA_wo_read_row (t, &row)))
        break;

      if (flags && prev_key)
        {
          cmp = strcmp (row.iov_base, prev_key);

          if (cmp < 0)
            flags &= ~CA_WO_FLAG_ASCENDING;
          else if (cmp > 0)
            flags &= ~CA_WO_FLAG_DESCENDING;
        }

      hash = CA_wo_hash (row.iov_base) % t->index_size;

      prev_key = row.iov_base;

#define HASH_INSERT(bits)                                        \
  do                                                             \
    {                                                            \
      while (t->index.u##bits[hash])                             \
        {                                                        \
          ++collisions;                                          \
          hash = (hash + fib[collisions & 1]) % t->index_size;   \
          fib[collisions & 1] += fib[~collisions & 1];           \
        }                                                        \
                                                                 \
      t->index.u##bits[hash] = offset;                           \
    }                                                            \
  while (0)

      switch (t->index_bits)
        {
        case 16: HASH_INSERT(16); break;
        case 32: HASH_INSERT(32); break;
        default: HASH_INSERT(64); break;
        }

#undef HASH_INSERT
    }

  if (ret == -1)
    return -1;

  t->header->flags = flags;

  assert (ret == 0);

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
  header.index_offset = t->write_offset;

  if (-1 == ca_file_buffer_flush (t->write_buffer))
    return -1;

  header.data_crc32c = t->crc32c;

  if (-1 == lseek (t->fd, 0, SEEK_SET))
    {
      ca_set_error ("Failed to seek to start of '%s': %s",
                    t->tmp_path, strerror (errno));

      return -1;
    }

  if (-1 == CA_wo_write_all (t->fd, &header, sizeof (header)))
    return -1;

  if (!(header.index_offset & ~0xffffULL))
    t->index_bits = 16;
  else if (!(header.index_offset & ~0xffffffffULL))
    t->index_bits = 32;
  else
    t->index_bits = 64;

  t->index_size = t->entry_count * 2 + 1;

  if (-1 == ftruncate (t->fd, header.index_offset + t->index_size * (t->index_bits / CHAR_BIT)))
    {
      ca_set_error ("ftruncate failed: %s", strerror (errno));

      return -1;
    }

  if (-1 == CA_wo_mmap (t))
    return -1;

  if (-1 == CA_wo_build_index (t))
    return -1;

  /* XXX: I think we need to sync all ancestor directories in order to be
   * completely safe */

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

  ca_file_buffer_free (t->write_buffer);
  t->write_buffer = NULL;

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
CA_wo_insert_row (void *handle, const struct iovec *value, size_t value_count)
{
  struct CA_wo *t = handle;
  uint8_t size_buf[10], *p = size_buf;
  int result = 0;

  struct iovec *iov;
  uint64_t i, size = 0;

  if (!(iov = ca_malloc ((value_count + 1) * sizeof (*iov))))
    return -1;

  for (i = 0; i < value_count; ++i)
    size += value[i].iov_len;

  ca_format_integer (&p, size);

  ++t->entry_count;

  iov[0].iov_base = size_buf;
  iov[0].iov_len = p - size_buf;
  memcpy (iov + 1, value, value_count * sizeof (*iov));

  if (-1 == ca_file_buffer_writev (t->write_buffer, iov, value_count + 1))
    result = -1;

  t->write_offset += size + iov[0].iov_len;

  free (iov);

  return result;
}

static int
CA_wo_seek (void *handle, off_t offset, int whence)
{
  struct CA_wo *t = handle;
  off_t new_offset;

  switch (whence)
    {
    case SEEK_SET:

      new_offset = sizeof (struct CA_wo_header);

      break;

    case SEEK_CUR:

      new_offset = t->offset + offset;

      break;

    case SEEK_END:

      new_offset = t->header->index_offset;

      break;

    default:

      assert (!"Invalid 'whence' value");
      errno = EINVAL;

      return -1;
    }

  if (new_offset < sizeof (struct CA_wo_header))
    {
      ca_set_error ("Attempt to seek before start of table");

      return -1;
    }

  if (new_offset > t->header->index_offset)
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
  uint64_t hash, offset;

  uint64_t fib[2] = { 2, 1 };
  unsigned int collisions = 0;

  hash = CA_wo_hash (key) % t->index_size;

  for (;;)
    {
      const uint8_t *data;
      int cmp;

      switch (t->index_bits)
        {
        case 16: offset = t->index.u16[hash]; break;
        case 32: offset = t->index.u32[hash]; break;
        default: offset = t->index.u64[hash]; break;
        }

      if (!offset)
        break;

      data = t->buffer + offset;

      while ((*data) & 0x80)
        ++data;

      ++data;

      if (!(cmp = strcmp ((const char *) data, key)))
        {
          t->offset = offset;

          return 1;
        }

      ++collisions;
      hash = (hash + fib[collisions & 1]) % t->index_size;
      fib[collisions & 1] += fib[~collisions & 1];
    }

  return -1;
}

static off_t
CA_wo_offset (void *handle)
{
  struct CA_wo *t = handle;

  return t->offset;
}

static ssize_t
CA_wo_read_row (void *handle, struct iovec *value)
{
  struct CA_wo *t = handle;
  uint64_t size;
  uint8_t *p;

  if (t->offset < 0)
    {
      ca_set_error ("Current offset is negative");

      return -1;
    }

  if (t->offset >= t->header->index_offset)
    {
      assert (t->offset == t->header->index_offset);

      return 0;
    }

  p = t->buffer + t->offset;

  size = ca_parse_integer ((const uint8_t **) &p);

  value->iov_base = p;
  value->iov_len = size;

  p += size;
  t->offset = p - t->buffer;

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
  int prot = PROT_READ;

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

  if ((t->open_flags & O_RDWR) == O_RDWR)
    prot |= PROT_WRITE;

  if (MAP_FAILED == (t->buffer = mmap (NULL, end, prot, MAP_SHARED, t->fd, 0)))
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

  if (!(t->header->index_offset & ~0xffffULL))
    t->index_bits = 16;
  else if (!(t->header->index_offset & ~0xffffffffULL))
    t->index_bits = 32;
  else
    t->index_bits = 64;

  t->index.u64 = (uint64_t *) (t->buffer + t->header->index_offset);
  t->index_size = (t->buffer_size - t->header->index_offset) / (t->index_bits / CHAR_BIT);

  t->offset = sizeof (struct CA_wo_header);

  return 0;
}

static void
CA_wo_free (struct CA_wo *t)
{
  if (t->fd != -1)
    close (t->fd);

  ca_file_buffer_free (t->write_buffer);
  t->write_buffer = NULL;

  if (t->buffer != MAP_FAILED)
    munmap (t->buffer, t->buffer_size);

  if (t->tmp_path)
    {
      unlink (t->tmp_path);
      free (t->tmp_path);
    }

  free (t->path);
  free (t);
}
