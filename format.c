#include <math.h>
#include <string.h>

#include "ca-table.h"

size_t
ca_integer_size (uint64_t value)
{
  if (value < 0x00000000000000080ULL) return 1;
  if (value < 0x00000000000004000ULL) return 2;
  if (value < 0x00000000000200000ULL) return 3;
  if (value < 0x00000000010000000ULL) return 4;
  if (value < 0x00000000800000000ULL) return 5;
  if (value < 0x00000040000000000ULL) return 6;
  if (value < 0x00002000000000000ULL) return 7;
  if (value < 0x00100000000000000ULL) return 8;
  if (value < 0x08000000000000000ULL) return 9;
  return 10;
}

void
ca_format_integer (uint8_t **output, uint64_t value)
{
  uint8_t buffer[10];
  unsigned int ptr = 9;

  buffer[ptr] = value & 0x7f;
  value >>= 7;

  while (value)
    {
      buffer[--ptr] = 0x80 | value;

      value >>= 7;
    }

  memcpy (*output, &buffer[ptr], 10 - ptr);

  *output += 10 - ptr;
}

void
ca_format_float (uint8_t **output, float value)
{
  memcpy (*output, &value, sizeof (value));

  *output += sizeof (value);
}

void
ca_format_time_float4 (uint8_t **output,
                       uint64_t start_time, uint32_t interval,
                       const float *sample_values, size_t sample_count)
{
  uint8_t *o;

  o = *output;

  ca_format_integer (&o, start_time);
  ca_format_integer (&o, interval);
  ca_format_integer (&o, sample_count);

  memcpy (o, sample_values, sizeof (*sample_values) * sample_count);
  o += sizeof (*sample_values) * sample_count;

  *output = o;
}

size_t
ca_offset_score_size (const struct ca_offset_score *values, size_t count)
{
  size_t i, result = 0;
  uint64_t prev_offset = 0;

  result += 1;
  result += ca_integer_size (count);

  for (i = 0; i < count; ++i)
    {
      result += ca_integer_size (values[i].offset - prev_offset);
      prev_offset = values[i].offset;

      result += sizeof (float);
    }

  return result;
}

void
ca_format_offset_score (uint8_t **output,
                        const struct ca_offset_score *values, size_t count)
{
  uint8_t *o;
  size_t i;
  uint64_t prev_offset = 0;

  enum ca_offset_score_type format;

  float min_score, max_score;
  int all_integer = 1;

  o = *output;

  if (!count)
    {
      ca_format_integer (&o, CA_OFFSET_SCORE_VARBYTE_FLOAT);
      ca_format_integer (&o, 0);
    }

  /* Analyze */

  min_score = max_score = values[0].score;

  for (i = 1; i < count; ++i)
    {
      float int_part, frac_part;

      if (values[i].score < min_score)
        min_score = values[i].score;
      else if (values[i].score > max_score)
        max_score = values[i].score;

      frac_part = modff (values[i].score, &int_part);

      if (frac_part)
        all_integer = 0;
    }

  if (min_score == max_score && min_score == 0)
    format = CA_OFFSET_SCORE_VARBYTE_ZERO;
  else if (all_integer && (max_score - min_score) <= 0xff)
    format = CA_OFFSET_SCORE_VARBYTE_U8;
  else if (all_integer && (max_score - min_score) <= 0xffff)
    format = CA_OFFSET_SCORE_VARBYTE_U16;
  else if (all_integer && (max_score - min_score) <= 0xffffff)
    format = CA_OFFSET_SCORE_VARBYTE_U24;
  else
    format = CA_OFFSET_SCORE_VARBYTE_FLOAT;

  /* Output */

  ca_format_integer (&o, format);
  ca_format_integer (&o, count);

  if (format == CA_OFFSET_SCORE_VARBYTE_U8
      || format == CA_OFFSET_SCORE_VARBYTE_U16
      || format == CA_OFFSET_SCORE_VARBYTE_U24)
    ca_format_float (&o, min_score);

  for (i = 0; i < count; ++i)
    {
      ca_format_integer (&o, values[i].offset - prev_offset);
      prev_offset = values[i].offset;

      if (format == CA_OFFSET_SCORE_VARBYTE_FLOAT)
        ca_format_float (&o, values[i].score);
      else if (format == CA_OFFSET_SCORE_VARBYTE_U8)
        *o++ = (uint8_t) (values[i].score - min_score);
      else if (format == CA_OFFSET_SCORE_VARBYTE_U16)
        {
          uint_fast16_t delta;

          delta = (uint_fast16_t) (values[i].score - min_score);

          *o++ = delta >> 8;
          *o++ = delta;
        }
      else if (format == CA_OFFSET_SCORE_VARBYTE_U24)
        {
          uint_fast32_t delta;

          delta = (uint_fast32_t) (values[i].score - min_score);

          *o++ = delta >> 16;
          *o++ = delta >> 8;
          *o++ = delta;
        }
    }

  *output = o;
}
