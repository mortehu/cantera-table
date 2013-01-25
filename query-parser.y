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
#include "query.h"

#define ALLOC(t) do { t = ca_arena_calloc(&context->arena, sizeof(*t)); } while(0)

int
yylex ();

void
yyerror (YYLTYPE *loc, struct ca_query_parse_context *context, const char *message);

#define scanner context->scanner
%}

%token AND CREATE FROM NOT OR PATH SELECT SHOW TABLE TABLES TEXT
%token TIME_FLOAT4 UTF8BOM WHERE _NULL
%token PRIMARY
%token KEY
%token INDEX
%token QUERY
%token SUMMARY
%token WITH
%token SORTED_UINT

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
%type<l> primaryKey
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
    : statements statement ';'
    | statement ';'
    ;

statement
    :
    | SHOW TABLES
      {
        #if 0
        ca_schema_show_tables ();
        #endif
      }
    | CREATE TABLE Identifier '(' createTableArgs ')' PATH StringLiteral
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

            if (arg->u.column->not_null)
              declaration.fields[i].flags |= CA_FIELD_NOT_NULL;

            if (arg->u.column->primary_key)
              declaration.fields[i].flags |= CA_FIELD_PRIMARY_KEY;

            ++i;
          }

        if (-1 == ca_schema_create_table (context->schema, $3, &declaration))
          context->error = 1;
      }
    | QUERY StringLiteral WITH '(' INDEX '=' Identifier ',' SUMMARY '=' Identifier ')'
      {
        if (-1 == ca_schema_query (context->schema, $2, $7, $11))
          fprintf (stderr, "Error: %s\n", ca_last_error ());
      }
    | SELECT selectList FROM Identifier whereClause
      {
        struct select_statement *stmt;
        ALLOC(stmt);
        stmt->list = $2;
        stmt->from = $4;
        stmt->where = $5;

        if (-1 == CA_select (context->schema, stmt))
          fprintf (stderr, "Error: %s\n", ca_last_error ());
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

columnType
    : TEXT        { $$ = CA_TEXT; }
    | TIME_FLOAT4 { $$ = CA_TIME_SERIES; }
    | SORTED_UINT { $$ = CA_SORTED_UINT; }
    ;

notNull
    :           { $$ = 0; }
    | _NULL     { $$ = 0; }
    | NOT _NULL { $$ = 1; }
    ;

primaryKey
    :             { $$ = 0; }
    | PRIMARY KEY { $$ = 1; }
    ;

columnDefinition
    : Identifier columnType notNull primaryKey
      {
        struct column_definition *col;
        ALLOC(col);
        col->name = $1;
        col->type = $2;
        col->not_null = $3;
        col->primary_key = $4;
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
        expr->type = EXPR_CONSTANT;
        expr->value.type = CA_INT64;
        expr->value.d.integer = $1;
        $$ = expr;
      }
    | Numeric
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_CONSTANT;
        expr->value.type = CA_NUMERIC;
        expr->value.d.numeric = $1;
        $$ = expr;
      }
    | StringLiteral
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_CONSTANT;
        expr->value.type = CA_TEXT;
        expr->value.d.string_literal = $1;
        $$ = expr;
      }
    | Identifier
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_IDENTIFIER;
        expr->value.d.identifier = $1;
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
yyerror (YYLTYPE *loc, struct ca_query_parse_context *context, const char *message)
{
  ca_set_error ("%s", message);

  context->error = 1;
}
