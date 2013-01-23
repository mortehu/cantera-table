#include <assert.h>
#include <pthread.h>
#include <string.h>

#include "ca-table.h"
#include "memory.h"

struct ca_fifo
{
  size_t size;

  size_t fill, space;
  size_t write_offset, read_offset;

  pthread_mutex_t state_lock;
  pthread_cond_t fill_available;
  pthread_cond_t space_available;

  uint8_t data[1];
};

struct ca_fifo *
ca_fifo_create (size_t size)
{
  struct ca_fifo *result;

  if (!(result = safe_malloc (sizeof (*result) + size)))
    return NULL;

  result->size = size;
  result->space = size;

  pthread_mutex_init (&result->state_lock, NULL);
  pthread_cond_init (&result->fill_available, NULL);
  pthread_cond_init (&result->space_available, NULL);

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
  size_t tail_space, remaining;

  pthread_mutex_lock (&fifo->state_lock);

  while (fifo->space < size)
    pthread_cond_wait (&fifo->space_available, &fifo->state_lock);

  pthread_mutex_unlock (&fifo->state_lock);

  tail_space = fifo->size - fifo->write_offset;
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

  assert (fifo->write_offset < fifo->size);

  pthread_mutex_lock (&fifo->state_lock);

  fifo->fill += size;
  fifo->space -= size;
  pthread_cond_broadcast (&fifo->fill_available);

  pthread_mutex_unlock (&fifo->state_lock);
}

void
ca_fifo_get (struct ca_fifo *fifo, void *data, size_t size)
{
  size_t tail_fill, remaining;

  pthread_mutex_lock (&fifo->state_lock);

  while (fifo->fill < size)
    pthread_cond_wait (&fifo->fill_available, &fifo->state_lock);

  pthread_mutex_unlock (&fifo->state_lock);

  tail_fill = fifo->size - fifo->read_offset;
  remaining = size;

  if (tail_fill <= remaining)
    {
      memcpy (data, &fifo->data[fifo->read_offset], tail_fill);

      fifo->read_offset = 0;

      data = (char *) data + tail_fill;
      remaining -= tail_fill;
    }

  memcpy (&fifo->data[fifo->read_offset],
          data, remaining);

  fifo->read_offset += remaining;

  assert (fifo->read_offset < fifo->size);

  pthread_mutex_lock (&fifo->state_lock);

  fifo->space += size;
  fifo->fill -= size;
  pthread_cond_broadcast (&fifo->space_available);

  pthread_mutex_unlock (&fifo->state_lock);
}
