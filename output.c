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

void
CA_output_time_float4 (struct iovec *iov)
{
  char time_buffer[64];

  const uint8_t *begin, *end;
  size_t i;
  int first = 1, delimiter = '\n';
  const char *format = "%s\t%.9g";

  begin = iov->iov_base;
  end = begin + iov->iov_len;

  if (CA_output_format == CA_PARAM_VALUE_JSON)
    {
      putchar ('[');

      delimiter = ',';
      format = "{\"time\":\"%s\",\"value\":%.9g}";
    }

  while (begin != end)
    {
      uint64_t start_time;
      uint32_t interval, sample_count;
      const float *sample_values;

      ca_parse_time_float4 (&begin,
                            &start_time, &interval,
                            &sample_values, &sample_count);

      for (i = 0; i < sample_count; ++i)
        {
          time_t time;
          struct tm tm;

          if (!first)
            putchar (delimiter);

          time = start_time + i * interval;

          gmtime_r (&time, &tm);

          strftime (time_buffer, sizeof (time_buffer), CA_time_format, &tm);

          printf (format, time_buffer, sample_values[i]);

          first = 0;
        }
    }

  if (CA_output_format == CA_PARAM_VALUE_JSON)
    putchar (']');
}
