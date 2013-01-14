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
    err (EX_OSERR, "malloc failed (%zu bytes)", size);

  memset (result, 0, size);

  return result;
}

char *
safe_strdup (const char *string)
{
  size_t length;
  char *result;

  length = strlen (string) + 1;

  result = safe_malloc (length);

  memcpy (result, string, length);

  return result;
}

void
array_grow (void **array, size_t *alloc, size_t element_size)
{
  size_t new_alloc;
  void *new_array;

  new_alloc = *alloc * 3 / 2 + 16;

  if (!(new_array = realloc (*array, new_alloc * element_size)))
    err (EX_OSERR, "realloc failed (allocation size %zu)", new_alloc * element_size);

  *array = new_array;
  *alloc = new_alloc;
}
