#include <assert.h>
#include <pthread.h>

#include "ca-table.h"

static void *
writer_thread (void *opaque)
{
  struct ca_fifo *fifo = opaque;

  uint8_t buffer[16];
  size_t i, j;

  for (j = 0; j < 10000; ++j)
    {
      for (i = 0; i < 16; ++i)
        buffer[i] = i;

      ca_fifo_put (fifo, buffer, sizeof (buffer));
    }

  return NULL;
}

static void *
reader_thread (void *opaque)
{
  struct ca_fifo *fifo = opaque;

  uint8_t buffer[16];
  size_t i, j;

  for (j = 0; j < 10000; ++j)
    {
      ca_fifo_get (fifo, buffer, sizeof (buffer));

      for (i = 0; i < 16; ++i)
        assert (buffer[i] == i);
    }

  return NULL;
}

int
main (int argc, char **argv)
{
  struct ca_fifo *fifo;
  pthread_t writer, reader;

  fifo = ca_fifo_create (255);

  pthread_create (&writer, 0, writer_thread, fifo);
  pthread_create (&reader, 0, reader_thread, fifo);
  pthread_join (writer, NULL);
  pthread_join (reader, NULL);

  ca_fifo_free (fifo);

  return EXIT_SUCCESS;
}
