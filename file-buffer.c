#include <errno.h>
#include <stddef.h>
#include <string.h>

#include "ca-table.h"
#include "memory.h"

#define BUFFER_SIZE (1024 * 1024)

struct ca_file_buffer
{
  size_t fill;
  int fd;
  char data[1];
};

struct ca_file_buffer *
ca_file_buffer_alloc (int fd)
{
  struct ca_file_buffer *result;
  size_t alloc_size;

  alloc_size = offsetof (struct ca_file_buffer, data) + BUFFER_SIZE;

  if (!(result = safe_malloc (alloc_size)))
    return NULL;

  result->fill = 0;
  result->fd = fd;

  return result;
}

void
ca_file_buffer_free (struct ca_file_buffer *buffer)
{
  free (buffer);
}

int
ca_file_buffer_write (struct ca_file_buffer *buffer,
                      const void *buf, size_t count)
{
  struct iovec iov;

  iov.iov_base = (void *) buf;
  iov.iov_len = count;

  return ca_file_buffer_writev (buffer, &iov, 1);
}

int
ca_file_buffer_writev (struct ca_file_buffer *buffer,
                       const struct iovec *iov, int count)
{
  struct iovec *write_iov = NULL;
  int write_count = 0;

  int i, result = -1;
  ssize_t length = 0;

  for (i = 0; i < count; ++i)
    length += iov[i].iov_len;

  if (buffer->fill + length > BUFFER_SIZE)
    {
      ssize_t ret, expected = 0;

      if (!(write_iov = safe_malloc (sizeof (*iov) * (count + 1))))
        return -1;

      if (buffer->fill)
        {
          write_iov[0].iov_base = buffer->data;
          write_iov[0].iov_len = buffer->fill;
          write_count = 1;

          expected = buffer->fill;

          buffer->fill = 0;
        }

      memcpy (&write_iov[write_count], iov, sizeof (*iov) * count);
      write_count += count;

      expected += length;

      if (expected != (ret = writev (buffer->fd, write_iov, write_count)))
        {
          if (ret == -1)
            ca_set_error ("writev failed: %s", strerror (errno));
          else
            ca_set_error ("short writev (%zd of %zd bytes)",
                          ret, expected);

          goto done;
        }
    }
  else
    {
      for (i = 0; i < count; ++i)
        {
          memcpy (&buffer->data[buffer->fill], iov[i].iov_base, iov[i].iov_len);
          buffer->fill += iov[i].iov_len;
        }
    }

  result = 0;

done:

  free (write_iov);

  return result;
}

int
ca_file_buffer_flush (struct ca_file_buffer *buffer)
{
  ssize_t ret;

  if (!buffer->fill)
    return 0;

  if (buffer->fill != (ret = write (buffer->fd, buffer->data, buffer->fill)))
    {
      if (ret == -1)
        ca_set_error ("write failed: %s", strerror (errno));
      else
        ca_set_error ("short write (%zd of %zu bytes)",
                      ret, buffer->fill);

      return -1;
    }

  buffer->fill = 0;

  return 0;
}
