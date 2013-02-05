/*
    Cantera Table sorting functions
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

struct CA_sort_entry
{
  struct iovec value[2];
  size_t value_count;
};

static int
CA_sort_entrycmp (const void *vlhs, const void *vrhs)
{
  const struct CA_sort_entry *lhs = vlhs;
  const struct CA_sort_entry *rhs = vrhs;

  return strcmp (lhs->value[0].iov_base, rhs->value[0].iov_base);
}

int
ca_table_sort (struct ca_table *output, struct ca_table *input)
{
  struct CA_sort_entry *sorted_entries = NULL;
  size_t i, sorted_entry_alloc = 0, sorted_entry_count = 0;

  ssize_t ret;

  int result = -1;

  if (-1 == ca_table_seek (input, 0, SEEK_SET))
    return -1;

  for (;;)
    {
      if (sorted_entry_count == sorted_entry_alloc
          && -1 == CA_ARRAY_GROW (&sorted_entries, &sorted_entry_alloc))
        return -1;

      ret = ca_table_read_row (input,
                               sorted_entries[sorted_entry_count].value, 2);

      if (ret <= 0)
        {
          if (!ret)
            break;

          goto done;
        }

      sorted_entries[sorted_entry_count].value_count = ret;

      ++sorted_entry_count;
    }

  qsort (sorted_entries, sorted_entry_count, sizeof (*sorted_entries),
         CA_sort_entrycmp);

  for (i = 0; i < sorted_entry_count; ++i)
    {
      if (-1 == ca_table_insert_row (output,
                                     sorted_entries[i].value,
                                     sorted_entries[i].value_count))
        goto done;
    }

  result = 0;

done:

  free (sorted_entries);

  return result;
}
