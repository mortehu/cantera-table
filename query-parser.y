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

#define ALLOC(t) do { if (!(t = ca_arena_calloc(&context->arena, sizeof(*t)))) { context->error = 1; YYABORT; } } while(0)

int
yylex ();

void
yyerror (YYLTYPE *loc, struct ca_query_parse_context *context, const char *message);

#define scanner context->scanner
%}

%token AND CREATE FROM INDEX KEY LIMIT NOT OFFSET_SCORE OR PATH PRIMARY QUERY
%token SELECT SHOW SUMMARY TABLE TABLES TEXT TIME TIMESTAMP TIME_FLOAT4 UTF8BOM
%token WHERE WITH ZONE _NULL
%token OFFSET FETCH FIRST NEXT ROW ROWS ONLY
%token TRUE FALSE
%token INSERT INTO VALUES
%token DROP
%token SET OUTPUT FORMAT CSV JSON
%token BEGIN_ COMMIT LOCK

%token Identifier
%token Integer
%token Numeric
%token StringLiteral

%type<p> Numeric Identifier StringLiteral
%type<p> columnDefinition
%type<p> createTableArg
%type<p> createTableArgs
%type<p> expression
%type<p> expressionList
%type<p> selectItem
%type<p> selectList
%type<p> topStatements
%type<p> statement
%type<p> whereClause

%type<l> Integer
%type<l> columnType
%type<l> fetchClause
%type<l> notNull
%type<l> offsetClause
%type<l> primaryKey
%type<l> runtimeParameter
%type<l> runtimeParameterValue

%left '=' '<' '>' '+' '-' '*' '/' LIKE AND OR UMINUS

%%
document
    : bom topStatements
    ;

topStatements
    : topStatements statement ';'
      {
        CA_process_statement (context, $2);
        fflush (stdout);
      }
    | statement ';'
      {
        CA_process_statement (context, $1);
        fflush (stdout);
      }
    ;

bom : UTF8BOM
    |
    ;

statement
    : BEGIN_
      {
        struct statement *stmt;

        ALLOC (stmt);
        stmt->type = CA_SQL_BEGIN;

        $$ = stmt;
      }
    | COMMIT
      {
        struct statement *stmt;

        ALLOC (stmt);
        stmt->type = CA_SQL_COMMIT;

        $$ = stmt;
      }
    | CREATE TABLE Identifier '(' createTableArgs ')' WITH '(' PATH '=' StringLiteral ')'
      {
        struct statement *stmt;
        struct create_table_statement *create;

        struct create_table_arg *arg;
        size_t i = 0;

        ALLOC (stmt);
        stmt->type = CA_SQL_CREATE_TABLE;
        create = &stmt->u.create_table;
        create->name = $3;

        create->declaration.path = $11;

        for (arg = $5; arg; arg = arg->next)
          {
            if (arg->type == COLUMN_DEFINITION)
              ++create->declaration.field_count;
          }

        create->declaration.fields = ca_arena_calloc (&context->arena, sizeof (struct ca_field) * create->declaration.field_count);

        for (arg = $5; arg; arg = arg->next)
          {
            struct ca_field *field;

            if (arg->type != COLUMN_DEFINITION)
              continue;

            field = &create->declaration.fields[i];

            strncpy (field->name, arg->u.column->name, CA_NAMEDATALEN - 1);

            if (arg->u.column->not_null)
              field->flags |= CA_FIELD_NOT_NULL;

            if (arg->u.column->primary_key)
              field->flags |= (CA_FIELD_PRIMARY_KEY | CA_FIELD_NOT_NULL);

            field->type = arg->u.column->type;

            ++i;
          }

        $$ = stmt;
      }
    | DROP TABLE Identifier
      {
        struct statement *stmt;
        struct drop_table_statement *drop;

        ALLOC (stmt);
        stmt->type = CA_SQL_DROP_TABLE;
        drop = &stmt->u.drop_table;
        drop->name = $3;

        $$ = stmt;
      }
    | QUERY StringLiteral WITH '(' INDEX '=' Identifier ',' SUMMARY '=' Identifier ')' fetchClause
      {
        struct statement *stmt;
        struct query_statement *query;

        ALLOC (stmt);
        stmt->type = CA_SQL_QUERY;
        query = &stmt->u.query;
        query->query = $2;
        query->index_table_name = $7;
        query->summary_table_name = $11;
        query->limit = $13;

        $$ = stmt;
      }
    | INSERT INTO Identifier VALUES '(' expressionList ')'
      {
        struct statement *stmt;
        struct insert_statement *insert;

        ALLOC (stmt);
        stmt->type = CA_SQL_INSERT;
        insert = &stmt->u.insert;
        insert->table_name = $3;
        insert->values = $6;

        $$ = stmt;
      }
    | LOCK
      {
        struct statement *stmt;

        ALLOC (stmt);
        stmt->type = CA_SQL_LOCK;

        $$ = stmt;
      }
    | SHOW TABLES
      {
        struct statement *stmt;
        struct select_statement *select;
        struct select_item *list;

        /* This statement maps directly to SELECT * FROM ca_catalog.ca_tables */

        ALLOC(list);
        list->expression.type = EXPR_ASTERISK;

        ALLOC(stmt);
        stmt->type = CA_SQL_SELECT;
        select = &stmt->u.select;
        select->list = list;
        select->from = "ca_catalog.ca_tables";
        select->limit = -1;

        $$ = stmt;
      }
    | SELECT selectList FROM Identifier whereClause offsetClause fetchClause
      {
        struct statement *stmt;
        struct select_statement *select;

        ALLOC(stmt);
        stmt->type = CA_SQL_SELECT;
        select = &stmt->u.select;
        select->list = $2;
        select->from = $4;
        select->where = $5;
        select->offset = $6;
        select->limit = $7;

        $$ = stmt;
      }
    | SET runtimeParameter runtimeParameterValue
      {
        struct statement *stmt;
        struct set_statement *set;

        ALLOC (stmt);
        stmt->type = CA_SQL_SET;
        set = &stmt->u.set;
        set->parameter = $2;
        set->v.enum_value = $3;

        $$ = stmt;
      }
    | SET TIME FORMAT StringLiteral
      {
        struct statement *stmt;
        struct set_statement *set;

        ALLOC (stmt);
        stmt->type = CA_SQL_SET;
        set = &stmt->u.set;
        set->parameter = CA_PARAM_TIME_FORMAT;
        set->v.string_value = $4;

        $$ = stmt;
      }
    ;

runtimeParameter
    : OUTPUT FORMAT { $$ = CA_PARAM_OUTPUT_FORMAT; }
    ;

runtimeParameterValue
    : CSV           { $$ = CA_PARAM_VALUE_CSV; }
    | JSON          { $$ = CA_PARAM_VALUE_JSON; }
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
    : TEXT                     { $$ = CA_TEXT; }
    | TIMESTAMP WITH TIME ZONE { $$ = CA_TIMESTAMPTZ; }
    | TIME_FLOAT4              { $$ = CA_TIME_FLOAT4; }
    | OFFSET_SCORE             { $$ = CA_OFFSET_SCORE; }
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
        left->expression.next = $3;;
        $$ = left;
      }
    ;

selectItem
    : expression
      {
        struct select_item *item;
        ALLOC(item);
        memcpy (&item->expression, $1, sizeof (item->expression));
        $$ = item;
      }
    ;

expressionList
    : expression
      {
        $$ = $1;
      }
    | expression ',' expressionList
      {
        struct expression *left;
        left = $1;
        left->next = $3;
        $$ = left;
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
    | expression LIKE expression
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_LIKE;
        expr->lhs = $1;
        expr->rhs = $3;
        $$ = expr;
      }
    | expression AND expression
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_AND;
        expr->lhs = $1;
        expr->rhs = $3;
        $$ = expr;
      }
    | expression OR expression
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_OR;
        expr->lhs = $1;
        expr->rhs = $3;
        $$ = expr;
      }
    | TRUE
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_CONSTANT;
        expr->value.type = CA_BOOLEAN;
        expr->value.d.integer = 1;
        $$ = expr;
      }
    | FALSE
      {
        struct expression *expr;
        ALLOC(expr);
        expr->type = EXPR_CONSTANT;
        expr->value.type = CA_BOOLEAN;
        $$ = expr;
      }
    | '(' expression ')'
      {
        $$ = $2;
      }
    ;

whereClause
    :                  { $$ = 0; }
    | WHERE expression { $$ = $2; }
    ;

rows
    : ROW
    | ROWS
    |
    ;

firstOrNext
    : FIRST
    | NEXT
    ;

offsetClause
    :                     { $$ = 0; }
    | OFFSET Integer rows { $$ = $2; }
    ;

fetchClause
    :                                     { $$ = -1; }
    | FETCH firstOrNext Integer rows ONLY { $$ = $3; }
    | LIMIT Integer                       { $$ = $2; }
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
