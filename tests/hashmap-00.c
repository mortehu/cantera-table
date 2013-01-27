#include <assert.h>
#include <stdio.h>

#include "ca-table.h"

#include "words.c"

int
main(int argc, char **argv)
{
  struct ca_hashmap *hashmap;
  size_t i;

  hashmap = ca_hashmap_create (64);

  for (i = 0; i < WORD_COUNT; ++i)
    ca_hashmap_insert_int (hashmap, words[i], i);

  for (i = WORD_COUNT; i-- > 0; )
    {
      int64_t *ret = ca_hashmap_get_int (hashmap, words[i]);

      assert (ret);
      assert (*ret == i);
    }

  ca_hashmap_free (hashmap);

  return 0;
}
