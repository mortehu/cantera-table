#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

#include "words.c"

int
main (int argc, char **argv)
{
  struct ca_table *table_A = NULL, *table_B = NULL;

  char tmp_path[256];
  struct iovec value[2];

  int fd;
  size_t i;
  ssize_t ret;

  const char *prev_key = NULL;

  int result = EXIT_FAILURE;

  sprintf (tmp_path, "/tmp/wo-sort-00.tmp.XXXXXX");

  if (-1 == (fd = mkstemp (tmp_path)))
    {
      fprintf (stderr, "Failed to create temporary file (%s)", tmp_path);

      return EXIT_FAILURE;
    }

  close (fd); /* We don't need it */

  if (!(table_A = ca_table_open ("write-once", tmp_path, O_CREAT | O_TRUNC | O_RDWR, 0666)))
    goto fail;

  for (i = 0; i < WORD_COUNT; ++i)
    {
      value[0].iov_base = (void *) words[i];
      value[0].iov_len = strlen (words[i]) + 1;

      value[1].iov_base = &i;
      value[1].iov_len = sizeof (i);

      if (-1 == ca_table_insert_row (table_A, value, sizeof (value) / sizeof (value[0])))
        goto fail;
    }

  if (-1 == ca_table_sync (table_A))
    goto fail;

  if (-1 == ca_table_seek (table_A, 0, SEEK_SET))
    goto fail;

  /* Check that we can read back what we just wrote */
  for (i = 0; i < WORD_COUNT; ++i)
    {
      const char *key;

      if (-1 == (ret = ca_table_read_row (table_A, value, sizeof (value) / sizeof (value[0]))))
        goto fail;

      assert (ret <= sizeof (value) / sizeof (value[0]));
      assert (ret == 2); /* ret == 1 is also valid -- learn to deal with it */

      key = value[0].iov_base;

      if (strcmp (key, words[i]))
        {
          ca_set_error ("Key at offset %zu is %s, expected %s", i, key, words[i]);

          goto fail;
        }
    }

  if (!(table_B = ca_table_open ("write-once", tmp_path, O_CREAT | O_TRUNC | O_RDWR, 0666)))
    goto fail;

  if (-1 == ca_table_sort (table_B, table_A))
    goto fail;

  ca_table_close (table_A);
  table_A = NULL;

  if (-1 == ca_table_sync (table_B))
    goto fail;

  if (-1 == ca_table_seek (table_B, 0, SEEK_SET))
    goto fail;

  /* Check that keys come out in the right order */
  while (0 < (ret = ca_table_read_row (table_B, value, sizeof (value) / sizeof (value[0]))))
    {
      const char *key;

      assert (ret <= sizeof (value) / sizeof (value[0]));
      assert (ret == 2); /* ret == 1 is also valid -- learn to deal with it */

      key = value[0].iov_base;

      assert (!prev_key || strcmp (prev_key, key) < 0);

      if (value[1].iov_len != sizeof (i))
        {
          ca_set_error ("Got value size %zu while %zu was expected (key was %s)",
                        value[1].iov_len, sizeof (i), key);

          goto fail;
        }

      memcpy (&i, value[1].iov_base, sizeof (i));

      if (i >= sizeof (words) / sizeof (words[0]))
        {
          ca_set_error ("Got offset %zu, which is of out range (key was %s)",
                        i, key);

          goto fail;
        }

      if (strcmp (words[i], key))
        {
          ca_set_error ("Got key '%s' for offset %zu, while expecting '%s'", key, i, words[i]);

          goto fail;
        }

      prev_key = key;
    }

  if (ret == -1)
    goto fail;

  for (i = 0; i < WORD_COUNT; ++i)
    {
      const char *key;

      if (-1 == (ret = ca_table_seek_to_key (table_B, words[i])))
        goto fail;

      if (ret != 1)
        {
          ca_set_error ("Seek to key '%s' unexpectedly returned %d", words[i], ret);

          goto fail;
        }

      if (0 >= (ret = ca_table_read_row (table_B, value, sizeof (value) / sizeof (value[0]))))
        {
          if (!ret)
            ca_set_error ("Did not get value for key %s after seek to key", words[i]);

          goto fail;
        }

      assert (ret == 2); /* ret == 1 is also valid -- learn to deal with it */

      key = value[0].iov_base;

      assert (!strcmp (words[i], key));
    }

  result = EXIT_SUCCESS;

fail:

  if (result == EXIT_FAILURE)
    fprintf (stderr, "Error: %s\n", ca_last_error ());

  unlink (tmp_path);

  ca_table_close (table_A);
  ca_table_close (table_B);

  return result;
}
