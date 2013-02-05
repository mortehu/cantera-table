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
#include <stdarg.h>
#include <stdio.h>
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
      if (!(result = ca_malloc (size)))
        return NULL;

      if (-1 == ca_arena_add_pointer (arena, result))
        {
          free (result);

          return NULL;
        }

      return result;
    }

  if(arena->last)
    node = arena->last;
  else
    {
      if(!arena->data)
        {
          if (!(arena->data = ca_malloc (ARENA_BLOCK_SIZE)))
            return NULL;

          arena->size = ARENA_BLOCK_SIZE;
        }

      node = arena;
    }

  if(size > node->size - node->used)
    {
      struct ca_arena_info* new_arena;

      if (!(new_arena = ca_malloc(sizeof(*new_arena))))
        return NULL;

      if (!(new_arena->data = ca_malloc(ARENA_BLOCK_SIZE)))
        {
          free (new_arena);

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

char *
ca_arena_sprintf (struct ca_arena_info *arena, const char *format, ...)
{
  size_t size = 32;
  char *result;
  va_list ap;

  if (!(result = ca_arena_alloc (arena, size)))
    return NULL;

  for (;;)
    {
      int n;

      va_start (ap, format);
      n = vsnprintf (result, size, format, ap);
      va_end (ap);

      if (n > -1 && n < size)
        return result;

      size = (n > -1) ? (n + 1) : (size * 2);

      /* XXX: Pop last allocation of stack? */

      if (!(result = ca_arena_alloc (arena, size)))
        return NULL;
    }
}


int
ca_arena_add_pointer(struct ca_arena_info *arena, void *pointer)
{
  struct ca_arena_info *new_arena;

  if (!(new_arena = ca_malloc (sizeof (*new_arena))))
    return -1;

  new_arena->data = pointer;
  new_arena->size = 1;
  new_arena->used = 1;

  if (arena->last)
    arena->last->next = new_arena;
  else
    arena->next = new_arena;

  arena->last = new_arena;

  return 0;
}
