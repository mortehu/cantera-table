#ifndef MEMORY_H_
#define MEMORY_H_ 1

#include <stdlib.h>

#include "ca-table.h"

void *
safe_malloc (size_t size) CA_USE_RESULT;

void *
safe_memdup (const void *data, size_t size) CA_USE_RESULT;

char *
safe_strdup (const char *string) CA_USE_RESULT;

int
array_grow (void **array, size_t *alloc, size_t element_size) CA_USE_RESULT;

#define ARRAY_GROW(array, alloc) array_grow ((void **) (array), alloc, sizeof(**(array)))

#endif /* MEMORY_H_ */
