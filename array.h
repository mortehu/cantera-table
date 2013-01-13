#ifndef ARRAY_H_
#define ARRAY_H_ 1

void
grow_array (void **array, size_t *alloc, size_t element_size);

#define GROW_ARRAY(array, alloc) grow_array((void **) (array), alloc, sizeof(**(array)))

#endif /* !ARRAY_H_ */
