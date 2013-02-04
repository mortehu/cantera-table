#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

#include "words.c"

int
main (int argc, char **argv)
{
  struct ca_table *table_A = NULL, *table_B = NULL;

  char tmp_path_A[64], tmp_path_B[64];
  struct iovec value[2];

  int fd;
  size_t i;
  ssize_t ret;

  const char *prev_key = NULL;

  int result = EXIT_FAILURE;

  sprintf (tmp_path_A, "/tmp/flexi-sort-00.tmp.XXXXXX");
  sprintf (tmp_path_B, "/tmp/flexi-sort-00.tmp.XXXXXX");

  if (-1 == (fd = mkstemp (tmp_path_A)))
    {
      fprintf (stderr, "Failed to create temporary file (%s)", tmp_path_A);

      return EXIT_FAILURE;
    }

  if (-1 == (fd = mkstemp (tmp_path_B)))
    {
      fprintf (stderr, "Failed to create temporary file (%s)", tmp_path_B);

      unlink (tmp_path_A);

      return EXIT_FAILURE;
    }

  close (fd); /* We don't need it */

  if (!(table_A = ca_table_open ("flexi", tmp_path_A, O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666)))
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

      /* Delete all odd-numbered rows */
      if (i & 1)
        {
          if (-1 == ca_table_delete_row (table_A))
            goto fail;

          continue;
        }

      if (-1 == (ret = ca_table_read_row (table_A, value, sizeof (value) / sizeof (value[0]))))
        goto fail;

      assert (ret <= sizeof (value) / sizeof (value[0]));
      assert (ret == 1); /* ret == 1 is also valid -- learn to deal with it */

      key = value[0].iov_base;

      if (strcmp (key, words[i]))
        {
          ca_set_error ("Key at offset %zu is %s, expected %s", i, key, words[i]);

          goto fail;
        }

      assert (value[0].iov_len + strlen (key) + 1 + sizeof (size_t));
    }

  if (!(table_B = ca_table_open ("flexi", tmp_path_B, O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666)))
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
      const char *begin, *end;
      const char *key;

      assert (ret <= sizeof (value) / sizeof (value[0]));

      assert (ret == 1);

      begin = value[0].iov_base;
      end = begin + value[0].iov_len;

      key = begin;
      begin = strchr (begin, 0) + 1;

      assert (!prev_key || strcmp (prev_key, key) < 0);

      assert (end - begin == sizeof (i));
      memcpy (&i, begin, sizeof (i));

      assert (!(i & 1)); /* Odd numbered rows were deleted earlier */

      assert (!strcmp (words[i], key));

      prev_key = key;
    }

  if (ret == -1)
    goto fail;

  if (-1 != (ret = ca_table_seek_to_key (table_B, words[0])))
    {
      ca_set_error ("Expected ca_table_seek_to_key to return -1, got %d", (int) ret);

      goto fail;
    }

  result = EXIT_SUCCESS;

fail:

  if (result == EXIT_FAILURE)
    fprintf (stderr, "Error: %s\n", ca_last_error ());

  unlink (tmp_path_B);
  unlink (tmp_path_A);

  ca_table_close (table_A);
  ca_table_close (table_B);

  return result;
}
