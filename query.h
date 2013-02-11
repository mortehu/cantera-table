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
  CA_PARAM_VALUE_PLAIN,
  CA_PARAM_VALUE_CSV
};

struct ca_query_parse_context
{
  void *scanner;
  struct ca_arena_info arena;
  int error;

  enum ca_param_value output_format;
  char time_format[64];

  struct ca_schema *schema;
};

enum create_table_arg_type
{
  COLUMN_DEFINITION,
  TABLE_CONSTRAINT
};

struct create_table_arg
{
  enum create_table_arg_type type;

  union
    {
      struct column_definition *column;
    } u;

  struct create_table_arg *next;
};

enum expression_type
{
  EXPR_CONSTANT,

  EXPR_ADD,
  EXPR_AND,
  EXPR_ASTERISK,
  EXPR_CAST,
  EXPR_DISTINCT,
  EXPR_DIV,
  EXPR_EQUAL,
  EXPR_EQUAL_SELECT,
  EXPR_EXISTS,
  EXPR_FUNCTION_CALL,
  EXPR_GREATER_EQUAL,
  EXPR_GREATER_THAN,
  EXPR_IDENTIFIER,
  EXPR_IN_LIST,
  EXPR_IN_SELECT,
  EXPR_IS_NULL,
  EXPR_LESS_EQUAL,
  EXPR_LESS_THAN,
  EXPR_LIKE,
  EXPR_NOT_LIKE,
  EXPR_MUL,
  EXPR_NEGATIVE,
  EXPR_NOT,
  EXPR_NOT_EQUAL,
  EXPR_OR,
  EXPR_SELECT,
  EXPR_SUB,

  EXPR_FIELD
};

struct column_definition
{
  const char *name;
  int type;
  int not_null;
  int primary_key;
};

struct expression_value
{
  enum ca_type type;
  union
    {
      int64_t integer;
      float float4;
      double float8;
      char *numeric;
      char *string_literal;
      char *identifier;
      struct column_type *type;
      struct select_statement *select;
      struct iovec iov;
      uint32_t field_index;
    } d;
};

struct expression
{
  enum expression_type type;

  struct expression_value value;

  int _not;
  /* char *identifier2; */

  struct expression *lhs, *rhs;

  struct expression *next;
  struct expression *last;
};

struct select_item
{
  struct expression expression;
  char *alias;
};

struct select_variable
{
  const char *name;
  uint32_t field_index;
  enum ca_type type;

  struct select_variable *next;
};

enum ca_sql_statement_type
{
  CA_SQL_CREATE_TABLE,
  CA_SQL_DROP_TABLE,
  CA_SQL_INSERT,
  CA_SQL_SELECT,
  CA_SQL_SET,
  CA_SQL_QUERY
};

struct create_table_statement
{
  const char *name;
  struct ca_table_declaration declaration;
};

struct drop_table_statement
{
  const char *name;
};

struct select_statement
{
  struct select_item *list;
  char *from;
  struct expression *where;

  int64_t limit, offset;
};

struct insert_statement
{
  const char *table_name;
  struct expression *values;
};

struct query_statement
{
  const char *query;
  const char *index_table_name;
  const char *summary_table_name;
  int64_t limit;
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
      struct create_table_statement create_table;
      struct drop_table_statement drop_table;
      struct insert_statement insert;
      struct select_statement select;
      struct set_statement set;
      struct query_statement query;
    } u;

  struct statement *next;
};

/*****************************************************************************/

const char *
ca_cast_to_text (struct ca_query_parse_context *context,
                 const struct expression_value *value);

int
ca_compare_equal (struct expression_value *result,
                  const struct expression_value *lhs,
                  const struct expression_value *rhs);

int
ca_compare_like (struct expression_value *result,
                 const struct expression_value *lhs,
                 const struct expression_value *rhs);

int
CA_select (struct ca_query_parse_context *context,
           struct select_statement *stmt);

typedef int (*ca_expression_function) (struct expression_value *result,
                                       struct ca_query_parse_context *context,
                                       const struct iovec *field_values);

typedef int (*ca_output_function) (uint32_t field_index, const char *value);

#define CA_EXPRESSION_PRINT 0x0001

ca_expression_function
ca_expression_compile (const char *name,
                       struct expression *expr,
                       const struct ca_field *fields,
                       size_t field_count,
                       ca_output_function output_function);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !CA_QUERY_H_ */
