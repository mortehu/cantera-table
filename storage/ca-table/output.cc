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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <vector>

#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/query.h"

char CA_time_format[64];
enum ca_param_value CA_output_format = CA_PARAM_VALUE_CSV;

void CA_output_char(int ch) { putchar(ch); }

void CA_output_string(const char* string) {
  fwrite(string, 1, strlen(string), stdout);
}

void CA_output_json_string(const char* string, size_t length) {
  putchar_unlocked('"');

  while (length--) {
    auto ch = static_cast<uint8_t>(*string++);

    switch (ch) {
      case '\\':
        break;
      case '"':
        ch = '"';
        break;
      case '\a':
        ch = 'a';
        break;
      case '\b':
        ch = 'b';
        break;
      case '\t':
        ch = 't';
        break;
      case '\n':
        ch = 'n';
        break;
      case '\v':
        ch = 'v';
        break;
      case '\f':
        ch = 'f';
        break;
      case '\r':
        ch = 'r';
        break;

      default:

        if (ch < ' ')
          printf("\\u%04x", ch);
        else
          putchar_unlocked(ch);

        continue;
    }

    putchar_unlocked('\\');
    putchar_unlocked(ch);
  }

  putchar_unlocked('"');
}

void CA_output_float4(float number) { printf("%.9g", number); }

void CA_output_float8(double number) { printf("%.17g", number); }

void CA_output_uint64(uint64_t number) {
  char buffer[20];
  char* begin, *o;

  begin = o = buffer;

  while (number > 0) {
    *o++ = '0' + (number % 10);
    number /= 10;
  }

  if (o == begin)
    putchar('0');
  else {
    size_t length = o-- - begin;

    while (begin < o) {
      char tmp;

      tmp = *o;
      *o = *begin;
      *begin = tmp;

      --o;
      ++begin;
    }

    fwrite(buffer, 1, length, stdout);
  }
}
