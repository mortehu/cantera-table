/*
    malloc wrappers for Cantera Table
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

#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <sysexits.h>

#include "ca-table.h"

void *
ca_malloc (size_t size)
{
  void *result;

  if (!(result = malloc (size)))
    {
      ca_set_error ("malloc failed (%zu bytes)", size);

      return NULL;
    }

  memset (result, 0, size);

  return result;
}

void *
ca_memdup (const void *data, size_t size)
{
  void *result;

  if (!(result = malloc (size)))
    {
      ca_set_error ("malloc failed (%zu bytes)", size);

      return NULL;
    }

  memcpy (result, data, size);

  return result;
}

char *
ca_strdup (const char *string)
{
  return ca_memdup (string, strlen (string) + 1);
}

int
ca_array_grow (void **array, size_t *alloc, size_t element_size)
{
  size_t new_alloc;
  void *new_array;

  new_alloc = *alloc * 3 / 2 + 16;

  if (!(new_array = realloc (*array, new_alloc * element_size)))
    {
      ca_set_error ("realloc failed (allocation size %zu)", new_alloc * element_size);

      return -1;
    }

  *array = new_array;
  *alloc = new_alloc;

  return 0;
}
