#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include "array.h"
#include "journal.h"

#define BUFFER_SIZE (1024 * 1024)

static char buffer[BUFFER_SIZE];
static size_t buffer_fill;

static int journal_fd;
static char *journal_path;

struct journal_file
{
  char *path;
  int fd;
  int dirty;
  uint64_t size;

  char *buffer;
  size_t buffer_fill;
};

static struct journal_file *journal_files;
static size_t journal_file_alloc;
static size_t journal_file_count;

static void *
safe_malloc (size_t size)
{
  void *result;

  if (!(result = malloc (size)))
    errx (EX_OSERR, "malloc failed (%zu bytes)", size);

  memset (result, 0, size);

  return result;
}

static void
write_all (int fd, const void *data, size_t size)
{
  const char *cdata = data;
  ssize_t ret = 1;

  while (size)
    {
      if (0 > (ret = write (fd, cdata, size)))
        {
          if (ret == 0)
            errx (EX_IOERR, "write returned 0");

          err (EX_IOERR, "write failed");
        }

      size -= ret;
      cdata += ret;
    }
}

static void
journal_replay (off_t journal_length)
{
  void *journal_map;

  const unsigned char *begin, *end;

  size_t path_length;

  if (MAP_FAILED == (journal_map = mmap (NULL, journal_length, PROT_READ, MAP_PRIVATE, journal_fd, 0)))
    err (EX_OSERR, "Failed to mmap '%s'", journal_path);

  begin = journal_map;
  end = begin + journal_length;

  journal_file_count = 0;

  begin = journal_map;

  while (begin != end)
    {
      switch (*begin++)
        {
        case JOURNAL_TRUNCATE:

            {
              uint32_t file_index;
              uint64_t offset;

              memcpy (&file_index, begin, sizeof (uint32_t));
              begin += sizeof (uint32_t);

              memcpy (&offset, begin, sizeof (uint64_t));
              begin += sizeof (uint64_t);

              if (file_index >= journal_file_count)
                {
                  err (EX_DATAERR, "journal: attempt to truncate file %u of %zu",
                       file_index + 1, journal_file_count);
                }

              if (-1 == ftruncate (journal_files[file_index].fd,
                                   offset))
                {
                  err (EX_IOERR, "failed to truncate '%s' to %llu bytes",
                       journal_files[file_index].path,
                       (unsigned long long) offset);
                }

              journal_files[file_index].size = offset;
            }

          break;

        case JOURNAL_CREATE_FILE:

            {
              struct journal_file *f;

              if (journal_file_count == journal_file_alloc)
                GROW_ARRAY (&journal_files, &journal_file_alloc);

              f = &journal_files[journal_file_count++];

              memset (f, 0, sizeof (*f));

              path_length = strlen ((const char *) begin) + 1; /* Including terminating NUL */

              f->path = safe_malloc (path_length);

              memcpy (f->path, begin, path_length);

              if (-1 == (f->fd = open (f->path, O_RDWR | O_APPEND)))
                err (EX_OSERR, "Failed to open '%s' for reading and writing", f->path);

              if (-1 == (f->size = lseek (f->fd, 0, SEEK_END)))
                err (EX_IOERR, "Failed to seek to end of `%s'", f->path);

              begin += path_length;
            }

          break;

        default:

          errx (EX_DATAERR, "Unknown operation in journal");
        }
    }

  munmap (journal_map, journal_length);
}

void
journal_commit (void)
{
  uint32_t i;

  char *new_journal_path;

  journal_flush ();

  close (journal_fd);

  new_journal_path = safe_malloc (strlen (journal_path) + 12);
  strcpy (new_journal_path, journal_path);
  strcat (new_journal_path, ".tmp.XXXXXX");

  /* XXX: Maybe avoid dangling temporary files */

  if (-1 == (journal_fd = mkstemp (new_journal_path)))
    err (EX_OSERR, "Failed to create temporary file");

  if (-1 == flock (journal_fd, LOCK_EX))
    err (EX_UNAVAILABLE, "Unable to lock '%s' using flock", new_journal_path);

  for (i = 0; i < journal_file_count; ++i)
    {
      size_t path_length;
      struct journal_file *f;

      f = &journal_files[i];

      if (f->buffer)
        {
          if (f->buffer_fill)
            write_all (f->fd, f->buffer, f->buffer_fill);

          if (-1 == fsync (f->fd))
            err (EX_OSERR, "Failed to fsync %s", f->path);

          free (f->buffer);
          f->buffer_fill = 0;
        }

      path_length = strlen (f->path) + 1; /* Including terminating NUL */

      if (1 + path_length + buffer_fill > BUFFER_SIZE)
        journal_flush ();

      buffer[buffer_fill++] = JOURNAL_CREATE_FILE;
      memcpy (&buffer[buffer_fill], f->path, path_length);
      buffer_fill += path_length;

      if (1 + sizeof (uint32_t) + sizeof (uint64_t) > BUFFER_SIZE)
        journal_flush ();

      buffer[buffer_fill++] = JOURNAL_TRUNCATE;
      memcpy (&buffer[buffer_fill], &i, sizeof (uint32_t));
      buffer_fill += sizeof (uint32_t);
      memcpy (&buffer[buffer_fill], &f->size, sizeof (uint64_t));
      buffer_fill += sizeof (uint64_t);
    }

  journal_flush ();

  if (-1 == fsync (journal_fd))
    err (EX_OSERR, "Failed to sync new journal");

  if (-1 == rename (new_journal_path, journal_path))
    err (EX_OSERR, "Failed to rename '%s' to '%s'", new_journal_path, journal_path);
}

void
journal_init (const char *path)
{
  off_t journal_length;

  assert (!journal_path);

  if (!(journal_path = strdup (path)))
    err (EX_OSERR, "strdup faileD");

  if (-1 == (journal_fd = open (path, O_RDWR | O_APPEND | O_CREAT, 0666)))
    err (EX_UNAVAILABLE, "Failed to open '%s' for appending", path);

  if (-1 == flock (journal_fd, LOCK_EX))
    err (EX_UNAVAILABLE, "Unable to lock '%s' using flock", path);

  if (-1 == (journal_length = lseek (journal_fd, 0, SEEK_END)))
    err (EX_IOERR, "Failed to seek to end of '%s'", path);

  if (journal_length > 0)
    journal_replay (journal_length);
}

int
journal_file_open (const char *path)
{
  int result;
  size_t path_length;

  path_length = strlen (path);

  assert (2 + path_length < BUFFER_SIZE);

  for (result = 0; result < journal_file_count; ++result)
    {
      if (!strcmp (journal_files[result].path, path))
        return result;
    }

  if (journal_file_count == journal_file_alloc)
    GROW_ARRAY (&journal_files, &journal_file_alloc);

  if (!(journal_files[result].path = strdup (path)))
    err (EX_OSERR, "strdup failed");

  if (-1 == (journal_files[result].fd = open (path, O_RDWR | O_APPEND | O_CREAT | O_TRUNC, 0666)))
    err (EX_IOERR, "Failed to create '%s'", path);

  if (2 + path_length + buffer_fill > BUFFER_SIZE)
    journal_flush ();

  buffer[buffer_fill++] = JOURNAL_CREATE_FILE;
  memcpy (&buffer[buffer_fill], path, path_length + 1);
  buffer_fill += path_length + 1;

  ++journal_file_count;

  return result;
}


off_t
journal_file_size (int handle)
{
  return journal_files[handle].size;
}

void
journal_file_append (int handle, const void *data, size_t size)
{
  struct journal_file *f;

  f = &journal_files[handle];

  if (!f->buffer)
    f->buffer = safe_malloc (BUFFER_SIZE);

  if (size + f->buffer_fill > BUFFER_SIZE)
    {
      write_all (f->fd, f->buffer, f->buffer_fill);
      f->buffer_fill = 0;
    }

  if (size >= BUFFER_SIZE)
    write_all (f->fd, data, size);
  else
    {
      memcpy (&f->buffer[f->buffer_fill], data, size);
      f->buffer_fill += size;
    }

  f->size += size;
}

void
journal_flush ()
{
  write_all (journal_fd, buffer, buffer_fill);
  buffer_fill = 0;
}
