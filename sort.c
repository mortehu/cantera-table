#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"
#include "memory.h"

struct CA_sort_entry
{
  const char *key;
  struct iovec value;
};

static int
CA_sort_entrycmp (const void *vlhs, const void *vrhs)
{
  const struct CA_sort_entry *lhs = vlhs;
  const struct CA_sort_entry *rhs = vrhs;

  return strcmp (lhs->key, rhs->key);
}

int
ca_table_sort (struct ca_table *output, struct ca_table *input)
{
  struct CA_sort_entry *sorted_entries = NULL;
  size_t i, sorted_entry_alloc = 0, sorted_entry_count = 0;

  ssize_t ret;

  int result = -1;

  for (;;)
    {
      if (sorted_entry_count == sorted_entry_alloc
          && -1 == ARRAY_GROW (&sorted_entries, &sorted_entry_alloc))
        return -1;

      ret = ca_table_read_row (input,
                               &sorted_entries[sorted_entry_count].key,
                               &sorted_entries[sorted_entry_count].value);

      if (ret <= 0)
        {
          if (!ret)
            break;

          goto done;
        }

      ++sorted_entry_count;
    }

  qsort (sorted_entries, sorted_entry_count, sizeof (*sorted_entries),
         CA_sort_entrycmp);

  for (i = 0; i < sorted_entry_count; ++i)
    {
      if (-1 == ca_table_insert_row (output,
                                     sorted_entries[i].key,
                                     &sorted_entries[i].value, 1))
        goto done;
    }

  result = 0;

done:

  free (sorted_entries);

  return result;
}
