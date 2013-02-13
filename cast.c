#include <assert.h>
#include <time.h>

#include "query.h"

const char *
CA_cast_to_text (struct ca_query_parse_context *context,
                 const struct expression_value *value)
{
  char *result = NULL;
  size_t result_alloc = 0, result_size = 0;

  switch (value->type)
    {
    case CA_BOOLEAN:

      return value->d.integer ? "TRUE" : "FALSE";

    case CA_FLOAT4:

      return ca_arena_sprintf (&context->arena, "%.7g", value->d.float4);

    case CA_FLOAT8:

      return ca_arena_sprintf (&context->arena, "%.7g", value->d.float8);

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

    case CA_TIME_FLOAT4:

        {
          const uint8_t *begin, *end;
          size_t i;
          int first = 1;

          begin = value->d.iov.iov_base;
          end = begin + value->d.iov.iov_len;

          assert (value->d.iov.iov_len > 0);

          while (begin != end)
            {
              uint64_t start_time;
              uint32_t interval, sample_count;
              const float *sample_values;

              ca_parse_time_float4 (&begin,
                                    &start_time, &interval,
                                    &sample_values, &sample_count);

              for (i = 0; i < sample_count; ++i)
                {
                  time_t time;
                  struct tm tm;
                  char *o;

                  /*   13 bytes %.7g formatted -FLT_MAX
                   * + 19 bytes for ISO 8601 date/time
                   * + 1 byte for LF
                   * + 1 byte for TAB
                   */
                  if (result_size + 34 >= result_alloc
                      && -1 == CA_ARRAY_GROW_N (&result, &result_alloc, 34))
                    return NULL;

                  if (!first)
                    result[result_size++] = '\n';

                  time = start_time + i * interval;

                  gmtime_r (&time, &tm);

                  o = result + result_size;

                  /* XXX Support varying length date/time */

                  o += strftime (o, 20, CA_time_format, &tm);

                  *o++ = '\t';

                  o += sprintf (o, "%.7g", sample_values[i]);

                  result_size = o - result;

                  first = 0;
                }
            }

          if (result_size == result_alloc
              && -1 == CA_ARRAY_GROW_N (&result, &result_alloc, 1))
            return NULL;

          result[result_size] = 0;

          if (-1 == ca_arena_add_pointer (&context->arena, result))
            {
              free (result);
              result = NULL;
            }
        }

      break;

    default:

      ca_set_error ("Unsupported type %d", value->type);

      break;
    }

  return result;
}
