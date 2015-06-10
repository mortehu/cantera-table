/*
    Error message formatting and retrieval routines
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage/ca-table/error.h"

static __thread char* CA_last_error;
static __thread int CA_last_error_was_malloced;

const char* ca_last_error(void) {
  return CA_last_error ? CA_last_error : strerror(errno);
}

void ca_clear_error(void) {
  if (CA_last_error_was_malloced) free(CA_last_error);

  CA_last_error = 0;
  CA_last_error_was_malloced = 0;
}

void ca_set_error(const char* format, ...) {
  va_list args;
  char* prev_error;

  prev_error = CA_last_error;

  va_start(args, format);

  if (-1 == vasprintf(&CA_last_error, format, args)) {
    CA_last_error = strerror(errno);

    if (CA_last_error_was_malloced) free(prev_error);

    CA_last_error_was_malloced = 0;
  } else {
    if (CA_last_error_was_malloced) free(prev_error);

    CA_last_error_was_malloced = 1;
  }
}
