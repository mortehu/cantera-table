/*
    Arena allocator
    Copyright (C) 2013    Morten Hustveit

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

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <err.h>
#include <stdlib.h>
#include <string.h>

#include "arena.h"
#include "ca-table.h"

#define ARENA_BLOCK_SIZE (256 * 1024)

void
ca_arena_init(struct ca_arena_info* arena)
{
  memset(arena, 0, sizeof(*arena));
}

void
ca_arena_free(struct ca_arena_info* arena)
{
  ca_arena_reset (arena);

  free(arena->data);

  memset(arena, 0, sizeof(*arena));
}

void
ca_arena_reset(struct ca_arena_info* arena)
{
  struct ca_arena_info* node;

  node = arena->next;

  while(node)
  {
    struct ca_arena_info* tmp;

    tmp = node;
    node = node->next;

    free(tmp->data);
    free(tmp);
  }

  arena->used = 0;
  arena->next = NULL;
  arena->last = NULL;
}

void*
ca_arena_alloc(struct ca_arena_info* arena, size_t size)
{
  void* result;
  struct ca_arena_info* node;

  if(!size)
    return 0;

  size = (size + 3) & ~3;

  if(size > ARENA_BLOCK_SIZE)
    {
      struct ca_arena_info* new_arena;

      if (!(new_arena = malloc(sizeof(*new_arena))))
        {
          ca_set_error ("Failed to allucate %zu bytes", sizeof (*new_arena));

          return NULL;
        }

      if (!(new_arena->data = malloc(size)))
        {
          ca_set_error ("Failed to allocate %zu bytes", size);

          return NULL;
        }

      new_arena->size = size;
      new_arena->used = size;
      new_arena->next = 0;

      if(!arena->last)
        {
          arena->next = new_arena;
          arena->last = new_arena;
        }
      else
        {
          arena->last->next = new_arena;
          arena->last = new_arena;
        }

      return new_arena->data;
    }

  if(arena->last)
    node = arena->last;
  else
    {
      if(!arena->data)
        {
          if (!(arena->data = malloc(ARENA_BLOCK_SIZE)))
            {
              ca_set_error ("Failed to allocate %u bytes", ARENA_BLOCK_SIZE);

              return NULL;
            }

          arena->size = ARENA_BLOCK_SIZE;
        }

      node = arena;
    }

  if(size > node->size - node->used)
    {
      struct ca_arena_info* new_arena;

      if (!(new_arena = malloc(sizeof(*new_arena))))
        {
          ca_set_error ("Failed to allocate %zu bytes", sizeof (*new_arena));

          return NULL;
        }

      if (!(new_arena->data = malloc(ARENA_BLOCK_SIZE)))
        {
          ca_set_error ("Failed to allocate %u bytes", ARENA_BLOCK_SIZE);

          return NULL;
        }

      new_arena->size = ARENA_BLOCK_SIZE;
      new_arena->used = 0;
      new_arena->next = 0;

      if(!arena->last)
        {
          arena->next = new_arena;
          arena->last = new_arena;
        }
      else
        {
          arena->last->next = new_arena;
          arena->last = new_arena;
        }

      node = new_arena;
    }

  assert(node->size == ARENA_BLOCK_SIZE);
  assert(node->used < node->size);
  assert(size <= node->size - node->used);

  result = (char*) node->data + node->used;
  node->used += size;

  return result;
}

void*
ca_arena_calloc(struct ca_arena_info* arena, size_t size)
{
  void* result;

  if (!(result = ca_arena_alloc(arena, size)))
    return NULL;

  memset(result, 0, size);

  return result;
}

char*
ca_arena_strdup(struct ca_arena_info* arena, const char* string)
{
  char* result;

  if (!(result = ca_arena_alloc(arena, strlen(string) + 1)))
    return NULL;

  strcpy(result, string);

  return result;
}

char*
ca_arena_strndup(struct ca_arena_info* arena, const char* string, size_t length)
{
  char* result;

  if (!(result = ca_arena_alloc(arena, length + 1)))
    return NULL;

  memcpy(result, string, length);
  result[length] = 0;

  return result;
}
