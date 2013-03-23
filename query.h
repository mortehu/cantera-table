#ifndef CA_QUERY_H_
#define CA_QUERY_H_ 1

#include "arena.h"
#include "ca-table.h"

#ifdef __cplusplus
extern "C" {
#endif

enum ca_param_value
{
  /* OUTPUT FORMAT */
  CA_PARAM_VALUE_CSV,
  CA_PARAM_VALUE_JSON
};

struct ca_query_parse_context
{
  void *scanner;
  struct ca_arena_info arena;
  int error;

  struct ca_schema *schema;
};

enum ca_sql_statement_type
{
  CA_SQL_SAMPLE,
  CA_SQL_SET,
  CA_SQL_QUERY,
  CA_SQL_QUERY_CORRELATE
};

struct sample_statement
{
  char *key;
};

struct query_statement
{
  const char *query;
  int64_t limit;
};

struct query_correlate_statement
{
  const char *query_A;
  const char *query_B;
};

enum ca_param
{
  CA_PARAM_OUTPUT_FORMAT,
  CA_PARAM_TIME_FORMAT
};

struct set_statement
{
  enum ca_param parameter;
  union
    {
      enum ca_param_value enum_value;
      char *string_value;
    } v;
};

struct statement
{
  enum ca_sql_statement_type type;

  union
    {
      struct sample_statement sample;
      struct set_statement set;
      struct query_statement query;
      struct query_correlate_statement query_correlate;
    } u;

  struct statement *next;
};

/*****************************************************************************/

extern char CA_time_format[64];
extern enum ca_param_value CA_output_format;
extern enum ca_param_value CA_isolation_level;

/*****************************************************************************/

int
CA_parse_script (struct ca_query_parse_context *context, FILE *input);

void
CA_process_statement (struct ca_query_parse_context *context,
                      struct statement *stmt);

/*****************************************************************************/

void
CA_output_char (int ch);

void
CA_output_string (const char *string);

void
CA_output_json_string (const char *string);

void
CA_output_float4 (float number);

void
CA_output_float8 (double number);

void
CA_output_uint64 (uint64_t number);

void
CA_output_time_float4 (struct iovec *iov);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !CA_QUERY_H_ */
