#ifndef ARRAY_H_
#define ARRAY_H_ 1

void
array_grow (void **array, size_t *alloc, size_t element_size);

#define ARRAY_GROW(array, alloc) array_grow ((void **) (array), alloc, sizeof(**(array)))

#endif /* !ARRAY_H_ */
