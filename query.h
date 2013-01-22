#ifndef CA_QUERY_H_
#define CA_QUERY_H_ 1

#include "arena.h"
#include "ca-table.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ca_query_parse_context
{
  void *scanner;
  struct ca_arena_info arena;
  int error;

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
  EXPR_PARENTHESIS,
  EXPR_SELECT,
  EXPR_SUB,

  EXPR_INTEGER,
  EXPR_NUMERIC,
  EXPR_STRING_LITERAL,

  EXPR_FIELD
};

struct column_definition
{
  const char *name;
  int type;
  int not_null;
  int primary_key;
};

struct expression
{
  enum expression_type type;
  union
    {
      long integer;
      char *numeric;
      char *string_literal;
      char *identifier;
      struct column_type *type;
      struct select_statement *select;
      struct ca_field *field;
    } d;

  int _not;
  char *identifier2;

  struct expression *lhs, *rhs;

  struct expression *next;
  struct expression *last;
};

struct select_item
{
  struct expression *expression;
  char *alias;
  struct select_item *next;
};

struct select_statement
{
  struct select_item *list;
  char *from;
  struct expression *where;
};

int
CA_select (struct ca_schema *schema, struct select_statement *stmt);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !CA_QUERY_H_ */
