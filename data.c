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

void
ca_data_parse_table_declaration (const uint8_t **input,
                                 struct ca_table_declaration *declaration)
{
  const uint8_t *p;

  p = *input;

  declaration->field_count = ca_data_parse_integer (&p);
  declaration->path = (char *) p;

  p = (const uint8_t *) strchr (declaration->path, 0) + 1;

  declaration->fields = (struct ca_field *) p;

  p += sizeof (struct ca_field) * declaration->field_count;

  *input = p;
}
