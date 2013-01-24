#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ca-table.h"

static struct
{
  const char *name;
  enum ca_type type;
} CA_types[] =
{
    { "integer",           CA_INT64 },
    { "numeric",           CA_NUMERIC },
    { "table_declaration", CA_TABLE_DECLARATION },
    { "text",              CA_TEXT },
    { "time",              CA_TIME },
    { "time_float4",       CA_TIME_SERIES }
};

enum ca_type
ca_type_from_string (const char *string)
{
  size_t first = 0, middle, half, count;

  count = sizeof (CA_types) / sizeof (CA_types[0]);

  while (count > 0)
    {
      int cmp;

      half = count >> 1;
      middle = first + half;

      cmp = strcasecmp (CA_types[middle].name, string);

      if (cmp < 0)
        {
          first = middle + 1;
          count -= half + 1;
        }
      else if (ca_likely (cmp > 0))
        count = half;
      else
        return CA_types[middle].type;
    }

  return CA_INVALID;
}
