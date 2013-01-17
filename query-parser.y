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
#include <stdio.h>
#include <string.h>

#include "arena.h"
#include "error.h"
#include "schema.h"
#include "smalltable.h"
#include "query.h"

#define ALLOC(t) do { t = ca_arena_calloc(&context->arena, sizeof(*t)); } while(0)

void
yyerror(YYLTYPE *loc, struct ca_query_parse_context *context, const char *message);

#define scanner context->scanner
%}

%token AND
%token CREATE
%token NOT
%token OR
%token PATH
%token SHOW
%token TABLE
%token TABLES
%token TIME_FLOAT4
%token UTF8BOM
%token _NULL

%token Identifier
%token Integer
%token Numeric
%token StringLiteral

%type<p> Numeric Identifier StringLiteral
%type<p> createTableArg
%type<p> createTableArgs
%type<p> columnDefinition

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
