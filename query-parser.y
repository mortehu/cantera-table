%pure-parser

%locations
%defines
%error-verbose

%parse-param { struct ca_query_parse_context *context }
%lex-param { void* scanner  }

%union
{
  void *p;
  long l;
}

%{
#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <stdio.h>
#include <string.h>

#include "arena.h"
#include "ca-table.h"
#include "schema.h"
#include "query.h"

#define ALLOC(t) do { t = ca_arena_calloc(&context->arena, sizeof(*t)); } while(0)

void
yyerror(YYLTYPE *loc, struct ca_query_parse_context *context, const char *message);

#define scanner context->scanner
%}

%token AND
%token CREATE
%token FROM
%token NOT
%token OID
%token OR
%token PATH
%token SELECT
%token SHOW
%token TABLE
%token TABLES
%token TIME_FLOAT4
%token UTF8BOM
%token WHERE
%token _NULL

%token Identifier
%token Integer
%token Numeric
%token StringLiteral

%type<p> Numeric Identifier StringLiteral
%type<p> createTableArg
%type<p> createTableArgs
%type<p> columnDefinition
%type<p> expression
%type<p> whereClause
%type<p> selectItem
%type<p> selectList

%type<l> Integer
%type<l> notNull
%type<l> columnType

%left '=' '<' '>' '+' '-' '*' '/' LIKE AND OR UMINUS

%start document
%%
document
    : bom statements
    ;
bom : UTF8BOM
    |
    ;

statements
    : statements statement
    | statement
    ;

statement
    : ';'
    | SHOW TABLES
      {
        ca_schema_show_tables ();
      }
    | CREATE TABLE Identifier '(' createTableArgs ')' PATH StringLiteral ';'
      {
        struct ca_table_declaration declaration;
        struct create_table_arg *arg;
        size_t i = 0;

        memset (&declaration, 0, sizeof (declaration));
        declaration.path = $8;

        for (arg = $5; arg; arg = arg->next)
          {
            if (arg->type == COLUMN_DEFINITION)
              ++declaration.field_count;
          }

        declaration.fields = ca_arena_calloc (&context->arena, sizeof (struct ca_field) * declaration.field_count);

        for (arg = $5; arg; arg = arg->next)
          {
            if (arg->type != COLUMN_DEFINITION)
              continue;

            strncpy (declaration.fields[i].name, arg->u.column->name, CA_NAMEDATALEN - 1);

            ++i;
          }
          ++declaration.field_count;

        ca_schema_add_table ($3, &declaration);
      }
    | SELECT selectList FROM Identifier whereClause
      {
        struct select_statement *stmt;
        ALLOC(stmt);
        stmt->list = $2;
        stmt->from = $4;
        stmt->where = $5;

        CA_select (stmt); 
      }
    ;

createTableArgs
    : createTableArg ',' createTableArgs
      {
        struct create_table_arg *left;
        left = $1;
        left->next = $3;;
        $$ = left;
      }
    | createTableArg
      {
        $$ = $1;
      }
    ;

notNull
    :           { $$ = 0; }
    | _NULL     { $$ = 0; }
    | NOT _NULL { $$ = 1; }
    ;

columnType
    : TIME_FLOAT4 { $$ = CA_TIME_SERIES; }
    ;

columnDefinition
    : Identifier columnType notNull
      {
        struct column_definition *col;
        ALLOC(col);
        col->name = $1;
        col->type = $2;
        col->not_null = $3;
        $$ = col;
      }
    ;

createTableArg
    : columnDefinition
      {
        struct create_table_arg *arg;
        ALLOC(arg);
        arg->type = COLUMN_DEFINITION;
        arg->u.column = $1;
        $$ = arg;
      }
    ;

selectList
    : selectItem
      {
        $$ = $1;
      }
    | selectItem ',' selectList
      {
        struct select_item *left;
        left = $1;
        left->next = $3;;
        $$ = left;
      }
    ;

selectItem
    : expression
      {
        struct select_item *item;
        ALLOC(item);
        item->expression = $1;
        item->alias = 0;
        item->next = 0;
        $$ = item;
      }
    ;


expression
    : '*'
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_ASTERISK;
        $$ = expr;
      }
    | Integer
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_INTEGER;
        expr->d.integer = $1;
        $$ = expr;
      }
    | Numeric
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_NUMERIC;
        expr->d.numeric = $1;
        $$ = expr;
      }
    | StringLiteral
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_STRING_LITERAL;
        expr->d.string_literal = $1;
        $$ = expr;
      }
    | Identifier
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_IDENTIFIER;
        expr->d.identifier = $1;
        $$ = expr;
      }
    | OID
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_OID;
        $$ = expr;
      }
    | expression '=' expression
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_EQUAL;
        expr->lhs = $1;
        expr->rhs = $3;
        $$ = expr;
      }
    ;

whereClause
    :                  { $$ = 0; }
    | WHERE expression { $$ = $2; }
    ;
%%
#include <stdio.h>

extern unsigned int character;
extern unsigned int line;

void
yyerror(YYLTYPE *loc, struct ca_query_parse_context *context, const char *message)
{
  ca_set_error ("%s", message);

  context->error = 1;
}
