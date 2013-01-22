#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <sysexits.h>

#include "memory.h"

void *
safe_malloc (size_t size)
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
safe_memdup (const void *data, size_t size)
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
safe_strdup (const char *string)
{
  return safe_memdup (string, strlen (string) + 1);
}

int
array_grow (void **array, size_t *alloc, size_t element_size)
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
