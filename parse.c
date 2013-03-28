/*
    Low-level data parser for Cantera Table
    Copyright (C) 2013    Morten Hustveit
    Copyright (C) 2013    eVenture Capital Partners II

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <string.h>

#include <err.h>
#include <sysexits.h>

#include "ca-table.h"
#include "ca-internal.h"

uint64_t
ca_parse_integer (const uint8_t **input)
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
ca_parse_float (const uint8_t **input)
{
  float result;

  memcpy (&result, *input, sizeof (result));
  *input += sizeof (result);

  return result;
}

const char *
ca_parse_string (const uint8_t **input)
{
  const char *result;

  result = (const char *) *input;

  *input = (const uint8_t *) strchr (result, 0) + 1;

  return result;
}

int
ca_offset_score_parse (const uint8_t **input,
                       struct ca_offset_score **return_values,
                       uint32_t *count)
{
  struct ca_offset_score *values;
  enum ca_offset_score_type type;
  uint_fast32_t i;
  const uint8_t *p;

  p = *input;

  type = *p++;

  if (type != CA_OFFSET_SCORE_FLEXI)
    {
      ca_set_error ("Unknown (offset, score) array encoding %d", (int) type);

      return -1;
    }

  *count = ca_parse_integer (&p);

  if (!*count)
    return 0;

  if (!(*return_values = ca_malloc (sizeof (**return_values) * *count)))
    return -1;

  values = *return_values;

    {
      struct CA_rle_context rle;
      uint64_t step_gcd, min_step, max_step;
      uint8_t score_flags;
      uint32_t min_score = 0;
      size_t parse_score_count;

      values[0].offset = ca_parse_integer (&p);

      step_gcd = ca_parse_integer (&p);

      if (!step_gcd)
        {
          for (i = 1; i < *count; ++i)
            values[i].offset = values[0].offset;
        }
      else
        {
          min_step = ca_parse_integer (&p);
          max_step = ca_parse_integer (&p) + min_step;

          if (min_step == max_step)
            {
              for (i = 1; i < *count; ++i)
                values[i].offset = values[i - 1].offset + step_gcd * min_step;
            }
          else if (max_step - min_step <= 0x0f)
            {
              CA_rle_init_read (&rle, p);

              for (i = 1; i < *count; i += 2)
                {
                  uint8_t tmp;

                  tmp = CA_rle_get (&rle);

                  values[i].offset = values[i - 1].offset + step_gcd * (min_step + (tmp & 0x0f));

                  if (i + 1 < *count)
                    values[i + 1].offset = values[i].offset + step_gcd * (min_step + (tmp >> 4));
                }

              assert (!rle.run);
              p = CA_rle_flush (&rle);
            }
          else if (max_step - min_step <= 0xff)
            {
              CA_rle_init_read (&rle, p);

              for (i = 1; i < *count; ++i)
                values[i].offset = values[i - 1].offset + step_gcd * (min_step + CA_rle_get (&rle));

              assert (!rle.run);
              p = CA_rle_flush (&rle);
            }
          else
            {
              for (i = 1; i < *count; ++i)
                values[i].offset = values[i - 1].offset + step_gcd * (min_step + ca_parse_integer (&p));
            }
        }

      score_flags = *p++;

      if (score_flags & 3)
        min_score = ca_parse_integer (&p);

      parse_score_count = (0 != (score_flags & 0x80)) ? 1 : *count;

      for (i = 0; i < parse_score_count; ++i)
        {
          switch (score_flags & 0x03)
            {
            case 0x00:

              values[i].score = ca_parse_float (&p);

              break;

            case 0x01:

              values[i].score = min_score + p[0];
              ++p;

              break;

            case 0x02:

              values[i].score = min_score + (p[0] << 8) + p[1];
              p += 2;

              break;

            case 0x03:

              values[i].score = min_score + (p[0] << 16) + (p[1] << 8) + p[2];
              p += 3;

              break;
            }
        }

      for (; i < *count; ++i)
        values[i].score = values[0].score;
    }

  return 0;
}

int
ca_offset_score_max_offset (const uint8_t *input, uint64_t *result)
{
  enum ca_offset_score_type type;
  uint_fast32_t i;
  const uint8_t *p;
  uint64_t offset = 0, count;

  struct CA_rle_context rle;
  uint64_t step_gcd, min_step, max_step;

  p = input;

  type = *p++;

  if (type != CA_OFFSET_SCORE_FLEXI)
    {
      ca_set_error ("Unknown (offset, score) array encoding %d", (int) type);

      return -1;
    }

  count = ca_parse_integer (&p);

  if (!count)
    {
      *result = 0;

      return 0;
    }

  offset = ca_parse_integer (&p);

  step_gcd = ca_parse_integer (&p);

  if (step_gcd && count > 1)
    {
      min_step = ca_parse_integer (&p);
      max_step = ca_parse_integer (&p) + min_step;

      if (min_step == max_step)
        {
          offset += step_gcd * min_step * (count - 1);
        }
      else if (max_step - min_step <= 0x0f)
        {
          CA_rle_init_read (&rle, p);

          for (i = 1; i < count; i += 2)
            {
              uint8_t tmp;

              tmp = CA_rle_get (&rle);

              offset += step_gcd * (min_step + (tmp & 0x0f));

              if (i + 1 < count)
                offset += step_gcd * (min_step + (tmp >> 4));
            }

          assert (!rle.run);
        }
      else if (max_step - min_step <= 0xff)
        {
          CA_rle_init_read (&rle, p);

          for (i = 1; i < count; ++i)
            offset += step_gcd * (min_step + CA_rle_get (&rle));

          assert (!rle.run);
        }
      else
        {
          for (i = 1; i < count; ++i)
            offset += step_gcd * (min_step + ca_parse_integer (&p));
        }
    }

  *result = offset;

  return 0;
}
