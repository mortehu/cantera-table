#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <string.h>

#include <err.h>
#include <sysexits.h>

#include "ca-table.h"

uint64_t
ca_data_parse_integer (const uint8_t **input)
{
  const uint8_t *i;
  uint64_t result = 0;

  i = *input;

  result = *i & 0x7F;

  while (0 != (*i & 0x80))
    {
      result <<= 7;
      result |= *++i & 0x7F;
    }

  *input = ++i;

  return result;
}

const char *
ca_data_parse_string (const uint8_t **input)
{
  const char *result;

  result = (const char *) *input;

  *input = (const uint8_t *) strchr (result, 0) + 1;

  return result;
}

void
ca_data_parse_time_float4 (const uint8_t **input,
                           uint64_t *start_time, uint32_t *interval,
                           const float **sample_values, uint32_t *count)
{
  const uint8_t *p;

  p = *input;

  *start_time = ca_data_parse_integer (&p);
  *interval = ca_data_parse_integer (&p);
  *count = ca_data_parse_integer (&p);
  *sample_values = (const float *) p;

  p += sizeof (**sample_values) * *count;

  *input = p;
}
#if 0



void
table_parse_time_series (const uint8_t **input,
                         uint64_t *start_time, uint32_t *interval,
                         const float **sample_values, size_t *count)
{
  const uint8_t *p;

  p = *input;

  *start_time = CA_data_parse_integer (&p);
  *interval = CA_data_parse_integer (&p);
  *count = CA_data_parse_integer (&p);
  *sample_values = (const float *) p;

  p += sizeof (**sample_values) * *count;

  *input = p;
}

void
ca_data_iterate (const void *data, size_t size,
                 ca_data_iterate_callback callback, void *opaque)
{
  struct ca_data buffer;
  const uint8_t *begin, *end;
  uint64_t previous_time = 0;

  begin = data;
  end = begin + size;

  while (begin != end)
    {
      buffer.type = *begin++;

      switch (buffer.type)
        {
        case CA_TIME_SERIES:

          table_parse_time_series (&begin,
                                   &buffer.v.time_series.start_time,
                                   &buffer.v.time_series.interval,
                                   &buffer.v.time_series.values,
                                   &buffer.v.time_series.count);
          previous_time = buffer.v.time_series.start_time;

          break;

        case CA_RELATIVE_TIME_SERIES:

          table_parse_time_series (&begin,
                                   &buffer.v.time_series.start_time,
                                   &buffer.v.time_series.interval,
                                   &buffer.v.time_series.values,
                                   &buffer.v.time_series.count);
          buffer.v.time_series.start_time += previous_time;
          previous_time = buffer.v.time_series.start_time;

          buffer.type = CA_TIME_SERIES;

          break;

        case CA_TABLE_DECLARATION:

          buffer.v.table_declaration.path = CA_data_parse_string (&begin);

          buffer.v.table_declaration.field_count = CA_data_parse_integer (&begin);
          buffer.v.table_declaration.fields = (struct ca_field *) begin;

          begin += sizeof (struct ca_field) * buffer.v.table_declaration.field_count;

          break;

        default:

          errx (EX_DATAERR, "Unknown data type %d", begin[-1]);
        }

      callback (&buffer, opaque);
    }
}
#endif
