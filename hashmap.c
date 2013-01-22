#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "hashmap.h"

#define HASHMAP_REBUILD_NOM 4
#define HASHMAP_REBUILD_DEN 3
#define HASHMAP_GROWTH_FUNCTION(n)  (((n) + 1) * 2 - 1)

struct ca_hashmap_data
{
  char *key;
  void *value;
};

struct ca_hashmap
{
  struct ca_hashmap_data *nodes;
  size_t size;
  size_t capacity;
};

static size_t
hash (const char *key)
{
  size_t v = *key++;

  while (*key)
    v = v * 31 + *key++;

  return v;
}

struct ca_hashmap *
ca_hashmap_create (size_t capacity)
{
  struct ca_hashmap *result;

  result = malloc (sizeof (struct ca_hashmap));

  if (!result)
    return NULL;

  result->size = 0;
  result->capacity = capacity;

  if (capacity)
    {
      result->nodes = calloc (capacity, sizeof (*result->nodes));

      if (!result->nodes)
        {
          free (result);

          return NULL;
        }
    }
  else
    result->nodes = 0;

  return result;
}

void
ca_hashmap_free (struct ca_hashmap *map)
{
  size_t i;

  if (!map)
    return;

  for (i = 0; i < map->capacity; ++i)
    free (map->nodes[i].key);

  free (map->nodes);
  free (map);
}

int
ca_hashmap_insert (struct ca_hashmap *map, const char *key, void *value)
{
  size_t i, n;

  /* If map is starting to get full, grow it and rehash all elements */
  if (map->size * HASHMAP_REBUILD_NOM >= map->capacity * HASHMAP_REBUILD_DEN)
    {
      struct ca_hashmap_data *new_nodes;
      size_t new_capacity;

      if (!map->capacity)
        new_capacity = 15;
      else
        new_capacity = HASHMAP_GROWTH_FUNCTION (map->capacity);

      new_nodes = calloc (new_capacity, sizeof (*new_nodes));

      if (!new_nodes)
        return -1;

      for (i = 0; i < map->capacity; ++i)
        {
          if (!map->nodes[i].key)
            continue;

          n = hash (map->nodes[i].key) % new_capacity;

          while (new_nodes[n].key)
            n = (n + 1) % new_capacity;

          new_nodes[n] = map->nodes[i];
        }

      free (map->nodes);
      map->nodes = new_nodes;
      map->capacity = new_capacity;
    }

  n = hash (key) % map->capacity;

  while (map->nodes[n].key)
    {
      if (!strcmp (map->nodes[n].key, key))
        {
          errno = EEXIST;

          return -1;
        }

      n = (n + 1) % map->capacity;
    }

  map->nodes[n].key = strdup (key);
  map->nodes[n].value = value;
  ++map->size;

  return 0;
}

void *
ca_hashmap_get (const struct ca_hashmap *h, const char *key)
{
  size_t n;

  n = hash (key) % h->capacity;

  while (h->nodes[n].key)
    {
      if (!strcmp (h->nodes[n].key, key))
        return h->nodes[n].value;

      n = (n + 1) % h->capacity;
    }

  return 0;
}
