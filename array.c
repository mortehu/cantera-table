#include <stdlib.h>

#include <err.h>
#include <sysexits.h>

void
grow_array (void **array, size_t *alloc, size_t element_size)
{
  size_t new_alloc;
  void *new_array;

  new_alloc = *alloc * 3 / 2 + 16;

  if (!(new_array = realloc (*array, new_alloc * element_size)))
    err (EX_OSERR, "realloc failed (allocation size %zu)", new_alloc * element_size);

  *array = new_array;
  *alloc = new_alloc;
}
