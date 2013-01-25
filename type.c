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
    { "INTEGER",           CA_INT64 },
    { "NUMERIC",           CA_NUMERIC },
    { "SORTED_UINT",       CA_SORTED_UINT },
    { "TABLE_DECLARATION", CA_TABLE_DECLARATION },
    { "TEXT",              CA_TEXT },
    { "TIME",              CA_TIME },
    { "TIME_FLOAT4",       CA_TIME_SERIES }
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

const char *
ca_type_to_string (enum ca_type type)
{
  size_t i;

  for (i = 0; i < sizeof (CA_types) / sizeof (CA_types[0]); ++i)
    {
      if (CA_types[i].type == type)
        return CA_types[i].name;
    }

  return NULL;
}
