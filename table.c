#include <string.h>

#include "ca-table.h"

uint64_t ca_xid;

/*****************************************************************************/

extern struct ca_table_backend CA_table_log;
extern struct ca_table_backend CA_table_writeonce;

struct ca_table
{
  struct ca_table_backend *backend;
  void *handle;
};

/*****************************************************************************/

struct ca_table_backend *
ca_table_backend (const char *name)
{
  if (!strcmp (name, "log"))
    return &CA_table_log;

  if (!strcmp (name, "write-once"))
    return &CA_table_writeonce;

  ca_set_error ("Backend '%s' not found", name);

  return NULL;
}

/*****************************************************************************/

struct ca_table *
ca_table_open (const char *backend_name,
               const char *path, int flags, mode_t mode)
{
  struct ca_table *result;

  if (!(result = ca_malloc (sizeof (*result))))
    return NULL;

  if (!(result->backend = ca_table_backend (backend_name)))
    {
      free (result);

      return NULL;
    }

  if (!(result->handle = result->backend->open (path, flags, mode)))
    {
      free (result);

      return NULL;
    }

  return result;
}

int
ca_table_stat (struct ca_table *table, struct stat *buf)
{
  return table->backend->stat (table->handle, buf);
}

int
ca_table_utime (struct ca_table *table, const struct timeval tv[2])
{
  return table->backend->utime (table->handle, tv);
}

int
ca_table_sync (struct ca_table *table)
{
  return table->backend->sync (table->handle);
}

void
ca_table_close (struct ca_table *table)
{
  if (!table)
    return;

  table->backend->close (table->handle);

  free (table);
}

int
ca_table_set_flag (struct ca_table *table, enum ca_table_flag flag)
{
  return table->backend->set_flag (table->handle, flag);
}

int
ca_table_is_sorted (struct ca_table *table)
{
  return table->backend->is_sorted (table->handle);
}

int
ca_table_insert_row (struct ca_table *table,
                     const struct iovec *value, size_t value_count)
{
  return table->backend->insert_row (table->handle, value, value_count);
}

int
ca_table_seek (struct ca_table *table, off_t offset, int whence)
{
  return table->backend->seek (table->handle, offset, whence);
}

int
ca_table_seek_to_key (struct ca_table *table, const char *key)
{
  return table->backend->seek_to_key (table->handle, key);
}

off_t
ca_table_offset (struct ca_table *table)
{
  return table->backend->offset (table->handle);
}

ssize_t
ca_table_read_row (struct ca_table *table, struct iovec *value)
{
  return table->backend->read_row (table->handle, value);
}

int
ca_table_delete_row (struct ca_table *table)
{
  return table->backend->delete_row (table->handle);
}

/*****************************************************************************/
