#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

static __thread char* CA_last_error;
static __thread int CA_last_error_was_malloced;

const char *
ca_last_error (void)
{
  return CA_last_error ? CA_last_error : strerror (errno);
}

void
ca_clear_error (void)
{
  if (CA_last_error_was_malloced)
    free (CA_last_error);

  CA_last_error = 0;
  CA_last_error_was_malloced = 0;
}

void
ca_set_error (const char *format, ...)
{
  va_list args;
  char* prev_error;

  prev_error = CA_last_error;

  va_start (args, format);

  if (-1 == vasprintf (&CA_last_error, format, args))
    {
      CA_last_error = strerror (errno);

      if (CA_last_error_was_malloced)
        free (prev_error);

      CA_last_error_was_malloced = 0;
    }
  else
    {
      if (CA_last_error_was_malloced)
        free (prev_error);

      CA_last_error_was_malloced = 1;
    }
}
