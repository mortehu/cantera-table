/*
    Heap based merge of sorted tables
    Copyright (C) 2013    Morten Hustveit
    Copyright (C) 2013    eVenture Capital Partners II

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

#include <string.h>

#include "ca-table.h"

struct CA_merge_heap
{
  struct iovec value[2];
  size_t table;
};

#define CA_HEAP_KEY(h) ((const char *) (h)->value[0].iov_base)

static void
CA_merge_heap_push (struct CA_merge_heap *heap, size_t heap_size,
                    const struct CA_merge_heap *entry)
{
  size_t parent, i;

  i = heap_size;

  while (i)
    {
      parent = (i - 1) / 2;

      if (strcmp (CA_HEAP_KEY (&heap[parent]), CA_HEAP_KEY (entry)) <= 0)
        break;

      heap[i] = heap[parent];
      i = parent;
    }

  heap[i] = *entry;
}

static void
CA_merge_heap_replace_top (struct CA_merge_heap *heap, size_t heap_size,
                           const struct CA_merge_heap *entry)
{
  size_t i, child, parent;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for(i = 1, child = 2; child <= heap_size; i = child, child = i << 1)
    {
      /* Move the smaller child up the tree */

      if(child + 1 <= heap_size
         && strcmp (CA_HEAP_KEY (&heap[child]), CA_HEAP_KEY (&heap[child + 1])) > 0)
        ++child;

      heap[i] = heap[child];
    }

  /* Move ancestor chain down into hole, until we can insert the new entry */

  while (i > 1)
    {
      parent = i >> 1;

      if (strcmp (CA_HEAP_KEY (&heap[parent]), CA_HEAP_KEY (entry)) <= 0)
        break;

      heap[i] = heap[parent];
      i = parent;
    }

  heap[i] = *entry;
}

static void
CA_merge_heap_pop (struct CA_merge_heap *heap, size_t heap_size)
{
  size_t i, child, parent;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for(i = 1, child = 2; child <= heap_size; i = child, child = i << 1)
    {
      /* Move the smaller child up the tree */

      if(child + 1 <= heap_size
         && strcmp (CA_HEAP_KEY (&heap[child]), CA_HEAP_KEY (&heap[child + 1])) > 0)
        ++child;

      heap[i] = heap[child];
    }

  if(i == heap_size)
    return;

  /* Hole did not end up at tail: Move ancestor chain down into hole, until we
   * can insert the tail element as an ancestor */

  while (i > 0)
    {
      parent = i >> 1;

      if (strcmp (CA_HEAP_KEY (&heap[parent]), CA_HEAP_KEY (&heap[heap_size])) <= 0)
        break;

      heap[i] = heap[parent];
      i = parent;
    }

  heap[i] = heap[heap_size];
}

int
ca_table_merge (struct ca_table **tables, size_t table_count,
                ca_merge_callback callback, void *opaque)
{
  struct CA_merge_heap *heap = NULL;
  size_t heap_size = 0;

  size_t i;
  ssize_t ret;

  int result = -1;

  if (!(heap = ca_malloc (sizeof (*heap) * table_count)))
    goto done;

  for (i = 0; i < table_count; ++i)
    {
      struct CA_merge_heap e;

      e.table = i;

      if (!ca_table_is_sorted (tables[i]))
        {
          ca_set_error ("ca_table merge called on unsorted table");

          goto done;
        }

      if (0 >= (ret = ca_table_read_row (tables[i], e.value, 2)))
        {
          if (ret == 0)
            continue;

          goto done;
        }

      CA_merge_heap_push (heap, heap_size++, &e);
    }

  while (heap_size)
    {
      struct CA_merge_heap e;

      e = heap[0];

      if (-1 == callback (CA_HEAP_KEY (&e), &e.value[1], opaque))
        goto done;

      if (0 >= (ret = ca_table_read_row (tables[e.table], e.value, 2)))
        {
          if (ret == 0)
            {
              CA_merge_heap_pop (heap, heap_size);
              --heap_size;

              continue;
            }

          goto done;
        }

      CA_merge_heap_replace_top (heap, heap_size, &e);
    }

  result = 0;

done:

  free (heap);

  return result;
}
