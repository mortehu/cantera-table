/*
    Output formatting (JSON, text and otherwise)
    Copyright (C) 2013    Morten Hustveit

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

#include <stdio.h>
#include <string.h>
#include <time.h>

#include "query.h"

char CA_time_format[64];
enum ca_param_value CA_output_format = CA_PARAM_VALUE_CSV;

void
CA_output_char (int ch)
{
  putchar (ch);
}

void
CA_output_string (const char *string)
{
  fwrite (string, 1, strlen (string), stdout);
}

void
CA_output_json_string (const char *string)
{
  putchar ('"');

  while (*string)
    {
      int ch;

      ch = *string++;

      switch (ch)
        {
        case '\\': break;
        case '"': ch = '"'; break;
        case '\a': ch = 'a'; break;
        case '\b': ch = 'b'; break;
        case '\t': ch = 't'; break;
        case '\n': ch = 'n'; break;
        case '\v': ch = 'v'; break;
        case '\f': ch = 'f'; break;
        case '\r': ch = 'r'; break;

        default:

          if (ch < ' ')
            printf ("\\u%04x", ch);
          else
            putchar (ch);

          continue;
        }

      putchar ('\\');
      putchar (ch);
    }

  putchar ('"');
}

void
CA_output_float4 (float number)
{
  printf ("%.9g", number);
}

void
CA_output_float8 (double number)
{
  printf ("%.17g", number);
}

void
CA_output_uint64 (uint64_t number)
{
  char buffer[20];
  char *begin, *o;

  begin = o = buffer;

  while (number > 0)
    {
      *o++ = '0' + (number % 10);
      number /= 10;
    }

  if (o == begin)
    putchar ('0');
  else
    {
      size_t length = o-- - begin;

      while (begin < o)
        {
          char tmp;

          tmp = *o;
          *o = *begin;
          *begin = tmp;

          --o;
          ++begin;
        }

      fwrite (buffer, 1, length, stdout);
    }
}

int
CA_output_offset_score_array (const uint8_t *data)
{
  size_t i;
  int first = 1;
  const char *format = "%llu:%.9g";

  uint32_t sample_count;
  struct ca_offset_score *sample_values;

  if (CA_output_format == CA_PARAM_VALUE_JSON)
    {
      putchar ('[');

      format = "{\"offset\":\"%llu\",\"score\":%.9g}";
    }

  if (-1 == ca_parse_offset_score_array (&data,
                                         &sample_values, &sample_count))
    return -1;

  for (i = 0; i < sample_count; ++i)
    {
      if (!first)
        putchar (',');

      printf (format,
              (unsigned long long) sample_values[i].offset,
              sample_values[i].score);

      first = 0;
    }

  free (sample_values);

  if (CA_output_format == CA_PARAM_VALUE_JSON)
    putchar (']');

  return 0;
}

int
CA_output_time_series (const uint8_t *data)
{
  char time_buffer[64];

  size_t i;
  int first = 1;
  const char *format = "%s:%.9g";

  uint32_t sample_count;
  struct ca_offset_score *sample_values;

  if (CA_output_format == CA_PARAM_VALUE_JSON)
    {
      putchar ('[');

      format = "{\"time\":\"%s\",\"value\":%.9g}";
    }

  if (-1 == ca_parse_offset_score_array (&data,
                                         &sample_values, &sample_count))
    return -1;

  for (i = 0; i < sample_count; ++i)
    {
      time_t time;
      struct tm tm;

      if (!first)
        putchar (',');

      time = sample_values[i].offset;
      memset (&tm, 0, sizeof (tm));

      gmtime_r (&time, &tm);

      strftime (time_buffer, sizeof (time_buffer), CA_time_format, &tm);

      printf (format, time_buffer, sample_values[i].score);

      first = 0;
    }

  free (sample_values);

  if (CA_output_format == CA_PARAM_VALUE_JSON)
    putchar (']');

  return 0;
}
