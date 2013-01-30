#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <string.h>

#include <err.h>
#include <sysexits.h>

#include "ca-table.h"
#include "memory.h"

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

float
ca_data_parse_float (const uint8_t **input)
{
  float result;

  memcpy (&result, *input, sizeof (result));
  *input += sizeof (result);

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

int
ca_data_parse_offset_score (const uint8_t **input,
                            struct ca_offset_score **sample_values,
                            uint32_t *count)
{
  enum ca_offset_score_type type;
  uint_fast32_t i;
  const uint8_t *p;
  uint64_t offset = 0;

  p = *input;

  type = *p++;
  *count = ca_data_parse_integer (&p);

  if (!(*sample_values = safe_malloc (sizeof (**sample_values) * *count)))
    return -1;

  switch (type)
    {
    case CA_OFFSET_SCORE_VARBYTE_FLOAT:

      for (i = 0; i < *count; ++i)
        {
          offset += ca_data_parse_integer (&p);
          (*sample_values)[i].offset = offset;
          (*sample_values)[i].score = ca_data_parse_float (&p);
        }

      break;

    default:

      ca_set_error ("Unknown (offset, score) array encoding %d", (int) type);

      free (*sample_values);

      return -1;
    }

  *input = p;

  return 0;
}
