#include <string.h>

#include "ca-table.h"
#include "ca-internal.h"

int
ca_schema_sample (struct ca_schema *schema, const char *key)
{
  struct ca_table **time_series_tables;
  ssize_t i, time_series_table_count;

  struct iovec data_iov;
  int ret;

  if (-1 == (time_series_table_count = ca_schema_time_series_tables (schema, &time_series_tables)))
    return -1;

  for (i = 0; i < time_series_table_count; ++i)
    {
      const void *data;

      if (-1 == (ret = ca_table_seek_to_key (time_series_tables[i], key)))
        return -1;

      if (!ret)
        continue;

      if (1 != (ret = ca_table_read_row (time_series_tables[i], &data_iov)))
        {
          if (ret >= 0)
            ca_set_error ("ca_table_read_row unexpectedly returned %d", (int) ret);

          return -1;
        }

      data = strchr (data_iov.iov_base, 0) + 1;

      if (-1 == CA_output_time_series (data))
        return -1;

      break;
    }

  return 0;
}
