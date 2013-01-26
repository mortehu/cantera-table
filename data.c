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
    case CA_OFFSET_SCORE_VARBYTE_FIXED:

        {
          uint32_t min_score;
          uint8_t bytes_per_score;

          min_score = ca_data_parse_integer (&p);
          bytes_per_score = *p++;

          if (bytes_per_score > 4)
            {
              ca_set_error ("Invalid number of bytes per score (%u)", bytes_per_score);

              free (*sample_values);

              return -1;
            }

          for (i = 0; i < *count; ++i)
            {
              uint32_t score;

              offset += ca_data_parse_integer (&p);
              (*sample_values)[i].offset = offset;

              score = min_score;

              switch (bytes_per_score)
                {
                case 4: score += p[3] << 24;
                case 3: score += p[2] << 16;
                case 2: score += p[1] << 8;
                case 1: score += p[0];
                }

              p += bytes_per_score;
              (*sample_values)[i].score = score;
            }
        }

      break;

    default:

      ca_set_error ("Unknown sorted uint array encoding %d", (int) type);

      free (*sample_values);

      return -1;
    }

  *input = p;

  return 0;
}
