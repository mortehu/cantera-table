#include <assert.h>
#include <string.h>
#include <time.h>

#include "query.h"

const char *
CA_cast_to_json (struct ca_query_parse_context *context,
                 const struct expression_value *value)
{
  char *result = NULL;
  size_t result_alloc = 0, result_size = 0;

  switch (value->type)
    {
    case CA_BOOLEAN:

      return value->d.integer ? "true" : "false";

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

      /* XXX: Escape special characters */
      return ca_arena_sprintf (&context->arena, "\"%s\"", value->d.string_literal);

    case CA_TIMESTAMPTZ:

        {
          time_t t;

          result = ca_arena_alloc (&context->arena, 22);

          t = value->d.integer;

          strftime (result, 22, "\"%Y-%m-%dT%H:%M:%S\"", gmtime (&t));
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

          if (-1 == CA_ARRAY_GROW_N (&result, &result_alloc, 2))
            {
              free (result);

              return NULL;
            }

          result[result_size++] = '[';

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

                  if (result_size + 128 >= result_alloc
                      && -1 == CA_ARRAY_GROW_N (&result, &result_alloc, 128))
                    return NULL;

                  if (!first)
                    result[result_size++] = ',';

                  time = start_time + i * interval;

                  gmtime_r (&time, &tm);

                  o = result + result_size;

                  /* XXX: Check for overflow */

                  o += sprintf (o, "{\"time\":\"");
                  o += strftime (o, result + result_alloc - o, context->time_format, &tm);
                  o += sprintf (o, "\",\"value\":%.7g}", sample_values[i]);

                  result_size = o - result;

                  first = 0;
                }
            }

          if (result_size == result_alloc
              && -1 == CA_ARRAY_GROW (&result, &result_alloc))
            return NULL;

          result[result_size++] = ']';
          result[result_size++] = 0;

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


int
CA_output_json (const char *field_name, const char *value,
                uint32_t field_index, uint32_t field_count)
{
  if (!field_index)
    putchar ('{');
  else
    putchar (',');

  printf ("\"%s\":", field_name);

  fwrite (value, 1, strlen (value), stdout);

  if (field_index + 1 == field_count)
    putchar ('}');

  return 0;
}
