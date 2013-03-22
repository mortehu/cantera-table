#include <assert.h>
#include <time.h>

#include "query.h"

float
ca_cast_text_to_float4 (const char *text)
{
  char *endptr;
  float result;

  result = strtof (text, &endptr);

  if (*endptr)
    ca_set_error ("Invalid floating point value '%s'", text);

  return result;
}

double
ca_cast_text_to_float8 (const char *text)
{
  char *endptr;
  double result;

  result = strtod (text, &endptr);

  if (*endptr)
    ca_set_error ("Invalid floating point value '%s'", text);

  return result;
}

const char *
CA_cast_to_text (struct ca_query_parse_context *context,
                 const struct expression_value *value)
{
  char *result = NULL;

  switch (value->type)
    {
    case CA_BOOLEAN:

      return value->d.integer ? "TRUE" : "FALSE";

    case CA_FLOAT4:

      return ca_arena_sprintf (&context->arena, "%.9g", value->d.float4);

    case CA_FLOAT8:

      return ca_arena_sprintf (&context->arena, "%.9g", value->d.float8);

    case CA_INT8:
    case CA_INT16:
    case CA_INT32:
    case CA_INT64:

      return ca_arena_sprintf (&context->arena, "%lld", (long long) value->d.integer);

    case CA_UINT8:
    case CA_UINT16:
    case CA_UINT32:
    case CA_UINT64:

      return ca_arena_sprintf (&context->arena, "%llu", (long long) value->d.integer);

    case CA_NUMERIC:

      return value->d.numeric;

    case CA_TEXT:

      return value->d.string_literal;

    case CA_TIMESTAMPTZ:

        {
          time_t t;

          result = ca_arena_alloc (&context->arena, 24);

          t = value->d.integer;

          strftime (result, 24, CA_time_format, gmtime (&t));
        }

      break;

    default:

      ca_set_error ("Unsupported type %d", value->type);

      break;
    }

  return result;
}
