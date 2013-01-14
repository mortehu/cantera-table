#ifndef MEMORY_H_
#define MEMORY_H_ 1

#include <stdlib.h>

void *
safe_malloc (size_t size);

char *
safe_strdup (const char *string);

void
array_grow (void **array, size_t *alloc, size_t element_size);

#define ARRAY_GROW(array, alloc) array_grow ((void **) (array), alloc, sizeof(**(array)))

#endif /* MEMORY_H_ */
