#include <assert.h>
#include <math.h>
#include <string.h>

#include "ca-table.h"
#include "ca-internal.h"

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

size_t
ca_offset_score_size (const struct ca_offset_score *values, size_t count)
{
  return 32 + count * 12;
}

static uint64_t
CA_gcd (uint64_t a, uint64_t b)
{
  uint64_t tmp;

  while (b)
    {
      tmp = b;
      b = a % b;
      a = tmp;
    }

  return a;
}

void
ca_format_offset_score (uint8_t **output,
                        const struct ca_offset_score *values, size_t count)
{
  uint8_t *o;
  size_t i;

  uint64_t min_step = 0, max_step = 0, step_gcd = 0;

  float min_score, max_score;
  int all_integer = 1;
  uint8_t score_flags = 0;

  o = *output;

  if (!count)
    {
      ca_format_integer (&o, CA_OFFSET_SCORE_FLEXI);
      ca_format_integer (&o, 0);

      return;
    }

  /* Analyze */

  min_score = max_score = values[0].score;

  for (i = 1; i < count; ++i)
    {
      uint64_t step;
      float int_part, frac_part;

      step = values[i].offset - values[i - 1].offset;

      if (i == 1)
        min_step = max_step = step_gcd = step;
      else
        {
          if (step < min_step)
            min_step = step;
          else if (step > max_step)
            max_step = step;

          step_gcd = CA_gcd (step, step_gcd);
        }

      if (values[i].score < min_score)
        min_score = values[i].score;
      else if (values[i].score > max_score)
        max_score = values[i].score;

      frac_part = modff (values[i].score, &int_part);

      if (frac_part)
        all_integer = 0;
    }


  ca_format_integer (&o, CA_OFFSET_SCORE_FLEXI);
  ca_format_integer (&o, count);

  /* Output offsets */

  ca_format_integer (&o, values[0].offset);
  ca_format_integer (&o, step_gcd);

  if (step_gcd)
    {
      struct CA_rle_context rle;

      min_step /= step_gcd;
      max_step /= step_gcd;

      ca_format_integer (&o, min_step);
      ca_format_integer (&o, max_step - min_step);

      if (min_step == max_step)
        ; /* Nothing needs to be stored */
      else if (max_step - min_step <= 0x0f)
        {
          CA_rle_init_write (&rle, o);

          for (i = 1; i < count; i += 2)
            {
              uint8_t tmp;

              tmp = (values[i].offset - values[i - 1].offset) / step_gcd - min_step;

              if (i + 1 < count)
                tmp |= ((values[i + 1].offset - values[i].offset) / step_gcd - min_step) << 4;

              CA_rle_put (&rle, tmp);
            }

          o = CA_rle_flush (&rle);
        }
      else if (max_step - min_step <= 0xff)
        {
          CA_rle_init_write (&rle, o);

          for (i = 1; i < count; ++i)
            CA_rle_put (&rle, (values[i].offset - values[i - 1].offset) / step_gcd - min_step);

          o = CA_rle_flush (&rle);
        }
      else
        {
          for (i = 1; i < count; ++i)
            ca_format_integer (&o, (values[i].offset - values[i - 1].offset) / step_gcd - min_step);
        }
    }
  else
    {
      assert (min_step == max_step);
      assert (max_step == 0);

      /* All steps are zero; no need to store them */
    }

  /* Output scores */

  if (max_score == min_score)
    {
      score_flags = 0x80;

      count = 1;
    }

  if (all_integer)
    {
      if (max_score - min_score <= 0xff)
        score_flags |= 0x01;
      else if (max_score - min_score <= 0xffff)
        score_flags |= 0x02;
      else
        score_flags |= 0x03;
    }

  *o++ = score_flags;

  if (all_integer)
    ca_format_integer (&o, min_score);

  for (i = 0; i < count; ++i)
    {
      switch (score_flags & 0x03)
        {
        case 0x00:

          ca_format_float (&o, values[i].score);

          break;

        case 0x01:

          *o++ = (uint8_t) (values[i].score - min_score);

          break;

        case 0x02:

            {
              uint_fast16_t delta;

              delta = (uint_fast16_t) (values[i].score - min_score);

              *o++ = delta >> 8;
              *o++ = delta;
            }

          break;

        case 0x03:

            {
              uint_fast32_t delta;

              delta = (uint_fast32_t) (values[i].score - min_score);

              *o++ = delta >> 16;
              *o++ = delta >> 8;
              *o++ = delta;
            }

          break;
        }
    }

  *output = o;
}
