/*
    Thread safe ring buffer implementation
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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <pthread.h>

#include "ca-table.h"

/* Limitations of this implementation:
 *
 *   - Single writer only
 *   - Single reader only
 *   - Cannot write blocks larger than FIFO size
 *   - Cannot read blocks larger than FIFO size
 */

struct ca_fifo
{
  pthread_cond_t fill_available;  /* sizeof == 48 bytes */
  pthread_cond_t space_available; /* sizeof == 48 bytes */
  pthread_mutex_t state_lock;     /* sizeof == 40 bytes */
  uint32_t fill, space;

  uint32_t write_offset, read_offset;

  /* Must be the last member.  See use of offsetof () */
  uint8_t data[1];
};

struct ca_fifo *
ca_fifo_create (size_t size)
{
  struct ca_fifo *result;

  if (!(result = ca_malloc (offsetof (struct ca_fifo, data) + size)))
    return NULL;

  pthread_cond_init (&result->fill_available, NULL);
  pthread_cond_init (&result->space_available, NULL);
  pthread_mutex_init (&result->state_lock, NULL);

  result->space = size;

  return result;
}

void
ca_fifo_free (struct ca_fifo *fifo)
{
  free (fifo);
}

void
ca_fifo_put (struct ca_fifo *fifo, const void *data, size_t size)
{
  uint_fast32_t tail_space, remaining;

  pthread_mutex_lock (&fifo->state_lock);

  assert (size <= fifo->fill + fifo->space);

  while (fifo->space < size)
    pthread_cond_wait (&fifo->space_available, &fifo->state_lock);

  tail_space = fifo->fill + fifo->space - fifo->write_offset;

  pthread_mutex_unlock (&fifo->state_lock);

  remaining = size;

  if (tail_space <= remaining)
    {
      memcpy (&fifo->data[fifo->write_offset], data, tail_space);

      fifo->write_offset = 0;

      data = (const char *) data + tail_space;
      remaining -= tail_space;
    }

  memcpy (&fifo->data[fifo->write_offset], data, remaining);

  fifo->write_offset += remaining;

  pthread_mutex_lock (&fifo->state_lock);

  fifo->fill += size;
  fifo->space -= size;
  pthread_cond_signal (&fifo->fill_available);

  pthread_mutex_unlock (&fifo->state_lock);
}

void
ca_fifo_get (struct ca_fifo *fifo, void *data, size_t size)
{
  uint_fast32_t tail_fill, remaining;

  pthread_mutex_lock (&fifo->state_lock);

  assert (size <= fifo->fill + fifo->space);

  while (fifo->fill < size)
    pthread_cond_wait (&fifo->fill_available, &fifo->state_lock);

  tail_fill = fifo->fill + fifo->space - fifo->read_offset;

  pthread_mutex_unlock (&fifo->state_lock);

  remaining = size;

  if (tail_fill <= remaining)
    {
      memcpy (data, &fifo->data[fifo->read_offset], tail_fill);

      fifo->read_offset = 0;

      data = (char *) data + tail_fill;
      remaining -= tail_fill;
    }

  memcpy (data, &fifo->data[fifo->read_offset], remaining);

  fifo->read_offset += remaining;

  pthread_mutex_lock (&fifo->state_lock);

  fifo->space += size;
  fifo->fill -= size;
  pthread_cond_signal (&fifo->space_available);

  pthread_mutex_unlock (&fifo->state_lock);
}
