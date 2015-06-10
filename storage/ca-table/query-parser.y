%pure-parser

%locations
%defines
%error-verbose

%code requires
{
struct ca_query_parse_context;
}

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

#include "storage/ca-table/arena.h"
#include "storage/ca-table/error.h"
#include "storage/ca-table/query.h"

#define ALLOC(t) do { if (!(t = ca_arena_calloc(&context->arena, sizeof(*t)))) { context->error = 1; YYABORT; } } while(0)

int
yylex ();

void
yyerror (YYLTYPE *loc, struct ca_query_parse_context *context, const char *message);

#define scanner context->scanner
%}

%token AND CREATE FROM INDEX KEY LIMIT NOT OFFSET_SCORE OR PATH PRIMARY QUERY
%token SHOW TABLES TEXT TIME UTF8BOM KEYS
%token WHERE WITH
%token OFFSET FETCH FIRST NEXT ROW ROWS ONLY
%token TRUE FALSE
%token INTO VALUES
%token SELECT
%token SET OUTPUT FORMAT CSV JSON
%token CORRELATE
%token THRESHOLDS FOR

%token Identifier
%token Integer
%token Numeric
%token StringLiteral

%type<p> Numeric Identifier StringLiteral
%type<p> statement
%type<p> topStatements
%type<p> identifierList
%type<p> integerList
%type<p> thresholdClause

%type<l> Integer
%type<l> keysClause
%type<l> fetchClause
%type<l> offsetClause
%type<l> runtimeParameter
%type<l> runtimeParameterValue

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
    : QUERY keysClause StringLiteral thresholdClause fetchClause offsetClause
      {
        struct statement *stmt;
        struct query_statement *query;

        ALLOC (stmt);
        stmt->type = kStatementQuery;
        query = &stmt->u.query;
        query->keys_only = $2;
        query->query = $3;
        query->thresholds = $4;
        query->limit = $5;
        query->offset = $6;

        $$ = stmt;
      }
    | CORRELATE QUERY StringLiteral ',' StringLiteral
      {
        struct statement *stmt;
        struct query_correlate_statement *query;

        ALLOC (stmt);
        stmt->type = kStatementCorrelate;
        query = &stmt->u.query_correlate;
        query->query_A = $3;
        query->query_B = $5;

        $$ = stmt;
      }
    | SELECT identifierList FROM StringLiteral
      {
        struct statement *stmt;
        struct select_statement *select;

        ALLOC (stmt);
        stmt->type = kStatementSelect;
        select = &stmt->u.select;
        select->fields = $2;
        select->query = $4;

        $$ = stmt;
      }
    | SET runtimeParameter runtimeParameterValue
      {
        struct statement *stmt;
        struct set_statement *set;

        ALLOC (stmt);
        stmt->type = kStatementSet;
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
        stmt->type = kStatementSet;
        set = &stmt->u.set;
        set->parameter = CA_PARAM_TIME_FORMAT;
        set->v.string_value = $4;

        $$ = stmt;
      }
    ;

runtimeParameter
    : OUTPUT FORMAT               { $$ = CA_PARAM_OUTPUT_FORMAT; }
    ;

runtimeParameterValue
    : CSV           { $$ = CA_PARAM_VALUE_CSV; }
    | JSON          { $$ = CA_PARAM_VALUE_JSON; }
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

thresholdClause
    : { $$ = 0; }
    | THRESHOLDS integerList FOR KEY StringLiteral
      {
        struct threshold_clause* th;
        ALLOC(th);
        th->values = $2;
        th->key = $5;
        $$ = th;
      }
    ;

integerList
    : Numeric ',' integerList
      {
        struct double_list* il;
        ALLOC(il);
        il->value = strtod($1, 0);
        il->next = $3;
        $$ = il;
      }
    | Integer ',' integerList
      {
        struct double_list* il;
        ALLOC(il);
        il->value = $1;
        il->next = $3;
        $$ = il;
      }
    | Numeric
      {
        struct double_list* il;
        ALLOC(il);
        il->value = strtod($1, 0);
        $$ = il;
      }
    | Integer
      {
        struct double_list* il;
        ALLOC(il);
        il->value = $1;
        $$ = il;
      }
    ;

identifierList
    : Identifier ',' identifierList
      {
        struct string_list* sl;
        ALLOC(sl);
        sl->value = $1;
        sl->next = $3;
        $$ = sl;
      }
    | Identifier
      {
        struct string_list* sl;
        ALLOC(sl);
        sl->value = $1;
        $$ = sl;
      }
    ;

keysClause
    :          { $$ = 0; }
    | KEYS FOR { $$ = 1; }
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
  ca_set_error ("%u:%u: %s", line, character, message);

  context->error = 1;
}
