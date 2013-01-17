#ifndef CA_QUERY_H_
#define CA_QUERY_H_ 1

#include "arena.h"

struct ca_query_parse_context
{
  void *scanner;
  struct ca_arena_info arena;
  int error;
};

int
ca_query_parse (const char *query);

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

struct column_definition
{
  const char *name;
  int type;
  int not_null;
};

#endif /* !CA_QUERY_H_ */
