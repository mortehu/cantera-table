#ifndef CA_ARENA_H_
#define CA_ARENA_H_ 1

#include <stdlib.h>

#define CA_MALLOC  __attribute__((malloc))

#ifdef __cplusplus
extern "C" {
#endif

struct ca_arena_info
{
  void *data;
  size_t size;
  size_t used;

  struct ca_arena_info *next;
  struct ca_arena_info *last;
};

void
ca_arena_init(struct ca_arena_info *arena);

void
ca_arena_reset(struct ca_arena_info *arena);

void
ca_arena_free(struct ca_arena_info *arena);

void*
ca_arena_alloc(struct ca_arena_info *arena, size_t size) CA_MALLOC;

void*
ca_arena_calloc(struct ca_arena_info *arena, size_t size) CA_MALLOC;

char*
ca_arena_strdup(struct ca_arena_info *arena, const char *string) CA_MALLOC;

char*
ca_arena_strndup(struct ca_arena_info *arena, const char *string, size_t length) CA_MALLOC;

#ifdef __cplusplus
}
#endif

#endif /* !CA_ARENA_H_ */
