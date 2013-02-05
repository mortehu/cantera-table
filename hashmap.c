/*
    Hash map implementation
    Copyright (C) 2011,2013    Morten Hustveit
    Copyright (C) 2013         eVenture Capital Partners II

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

#define HASHMAP_REBUILD_NOM 4
#define HASHMAP_REBUILD_DEN 3
#define HASHMAP_GROWTH_FUNCTION(n)  (((n) + 1) * 2 - 1)

static size_t
hash (const char *key)
{
  size_t v = *key;

  while (*key++)
    v = v * 31 + *key;

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

static struct ca_hashmap_data *
CA_hashmap_insert_helper (struct ca_hashmap *map, const char *key)
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
        return NULL;

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
        return &map->nodes[n];

      n = (n + 1) % map->capacity;
    }

  return &map->nodes[n];
}

int
ca_hashmap_insert (struct ca_hashmap *map, const char *key, void *value)
{
  struct ca_hashmap_data *node;

  if (!(node = CA_hashmap_insert_helper (map, key)))
    return -1;

  if (node->key)
    {
      ca_set_error ("Attempt to insert duplicate key '%s' into hashmap", key);

      return -1;
    }

  if (!(node->key = ca_strdup (key)))
    return -1;

  node->value.pointer = value;
  ++map->size;

  return 0;
}

int64_t
ca_hashmap_insert_int (struct ca_hashmap *map, const char *key, int64_t value)
{
  struct ca_hashmap_data *node;

  if (!(node = CA_hashmap_insert_helper (map, key)))
    return -1;

  if (!node->key)
    {
      if (!(node->key = ca_strdup (key)))
        return -1;

      ++map->size;
    }

  node->value.integer = value;

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
        return h->nodes[n].value.pointer;

      n = (n + 1) % h->capacity;
    }

  return 0;
}

int64_t *
ca_hashmap_get_int (const struct ca_hashmap *h, const char *key)
{
  size_t n;

  n = hash (key) % h->capacity;

  while (h->nodes[n].key)
    {
      if (!strcmp (h->nodes[n].key, key))
        return &h->nodes[n].value.integer;

      n = (n + 1) % h->capacity;
    }

  return NULL;
}
