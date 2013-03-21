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
    { "BOOLEAN",        CA_BOOLEAN },
    { "FLOAT",          CA_FLOAT4 },
    { "FLOAT4",         CA_FLOAT4 },
    { "FLOAT8",         CA_FLOAT8 },
    { "INT16",          CA_INT16 },
    { "INT32",          CA_INT32 },
    { "INT64",          CA_INT64 },
    { "INT8",           CA_INT8 },
    { "INTEGER",        CA_INT64 },
    { "NUMERIC",        CA_NUMERIC },
    { "OFFSET_SCORE[]", CA_OFFSET_SCORE_ARRAY },
    { "REAL",           CA_FLOAT8 },
    { "SMALLINT",       CA_INT16 },
    { "TEXT",           CA_TEXT },
    { "TIMESTAMPTZ",    CA_TIMESTAMPTZ },
    { "UINT16",         CA_UINT16 },
    { "UINT32",         CA_UINT32 },
    { "UINT64",         CA_UINT64 },
    { "UINT8",          CA_UINT8 },
    { "VOID",           CA_VOID }
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
