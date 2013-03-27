#include <string.h>

#include "ca-table.h"
#include "ca-internal.h"

int
ca_schema_sample (struct ca_schema *schema, const char *key)
{
  struct ca_time_series_table *time_series_tables;
  ssize_t i, time_series_table_count;
  size_t key_length;

  struct iovec data_iov;
  int ret;

  if (-1 == (time_series_table_count = ca_schema_time_series_tables (schema, &time_series_tables)))
    return -1;

  key_length = strlen (key);

  for (i = 0; i < time_series_table_count; ++i)
    {
      const void *data;
      struct ca_time_series_table *tst;

      tst = &time_series_tables[i];

      if (tst->prefix_length > key_length)
        continue;

      if (memcmp (tst->prefix, key, tst->prefix_length))
        continue;

      if (-1 == (ret = ca_table_seek_to_key (tst->table, key + tst->prefix_length)))
        return -1;

      if (!ret)
        continue;

      if (1 != (ret = ca_table_read_row (tst->table, &data_iov)))
        {
          if (ret >= 0)
            ca_set_error ("ca_table_read_row unexpectedly returned %d", (int) ret);

          return -1;
        }

      data = strchr (data_iov.iov_base, 0) + 1;

      if (-1 == CA_output_time_series (data))
        return -1;

      putchar ('\n');

      break;
    }

  return 0;
}
