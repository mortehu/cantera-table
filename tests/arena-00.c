#include <assert.h>

#include "arena.h"

int
main (int argc, char **argv)
{
  struct ca_arena_info arena;
  size_t i;

  ca_arena_init (&arena);

  for (i = 0; i < 1500; ++i)
    ca_arena_calloc (&arena, 1500);

  ca_arena_free (&arena);

  return EXIT_SUCCESS;
}
