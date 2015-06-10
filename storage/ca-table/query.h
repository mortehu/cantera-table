#ifndef CA_STORAGE_CA_TABLE_QUERY_H_
#define CA_STORAGE_CA_TABLE_QUERY_H_ 1

#include <stdint.h>

#include <sys/uio.h>

#include "storage/ca-table/arena.h"

#ifdef __cplusplus
extern "C" {
#endif

enum ca_param_value {
  /* OUTPUT FORMAT */
  CA_PARAM_VALUE_CSV,
  CA_PARAM_VALUE_JSON
};

struct ca_query_parse_context {
  void* scanner;
  struct ca_arena_info arena;
  int error;

  struct ca_schema* schema;
};

enum StatementType {
  kStatementCorrelate,
  kStatementQuery,
  kStatementSelect,
  kStatementSet
};

struct double_list {
  double value;
  struct double_list* next;
};

struct string_list {
  const char* value;
  struct string_list* next;
};

// Defines a keyword and a set of thresholds values for partitioning query
// results into groups.
struct threshold_clause {
  char* key;
  struct double_list* values;
};

struct query_statement {
  // Set to non-zero to retrieve document keys instead of JSON summaries.
  int keys_only;

  // The query string.
  const char* query;

  struct threshold_clause* thresholds;
  int64_t limit;
  size_t offset;
};

struct query_correlate_statement {
  const char* query_A;
  const char* query_B;
};

enum ca_param { CA_PARAM_OUTPUT_FORMAT, CA_PARAM_TIME_FORMAT };

struct select_statement {
  struct string_list* fields;
  const char* query;
};

struct set_statement {
  enum ca_param parameter;
  union {
    enum ca_param_value enum_value;
    char* string_value;
  } v;
};

struct statement {
  enum StatementType type;

  union {
    struct select_statement select;
    struct set_statement set;
    struct query_statement query;
    struct query_correlate_statement query_correlate;
  } u;

  struct statement* next;
};

/*****************************************************************************/

extern char CA_time_format[64];
extern enum ca_param_value CA_output_format;
extern enum ca_param_value CA_isolation_level;

/*****************************************************************************/

int CA_parse_script(struct ca_query_parse_context* context, FILE* input);

void CA_process_statement(struct ca_query_parse_context* context,
                          struct statement* stmt);

/*****************************************************************************/

void CA_output_char(int ch);

void CA_output_string(const char* string);

void CA_output_json_string(const char* string, size_t length);

void CA_output_float4(float number);

void CA_output_float8(double number);

void CA_output_uint64(uint64_t number);

void CA_output_time_float4(struct iovec* iov);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !CA_QUERY_H_ */
