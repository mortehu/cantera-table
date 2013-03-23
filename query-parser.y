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
%token SHOW SUMMARY TABLES TEXT TIME TIMESTAMP UTF8BOM
%token WHERE WITH ZONE _NULL
%token OFFSET FETCH FIRST NEXT ROW ROWS ONLY
%token TRUE FALSE
%token INTO VALUES
%token DROP
%token SET OUTPUT FORMAT CSV JSON
%token SAMPLE
%token CORRELATE

%token Identifier
%token Integer
%token Numeric
%token StringLiteral

%type<p> Numeric Identifier StringLiteral
%type<p> statement
%type<p> topStatements

%type<l> Integer
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
    : QUERY StringLiteral fetchClause offsetClause
      {
        struct statement *stmt;
        struct query_statement *query;

        ALLOC (stmt);
        stmt->type = CA_SQL_QUERY;
        query = &stmt->u.query;
        query->query = $2;
        query->limit = $3;

        $$ = stmt;
      }
    | CORRELATE QUERY StringLiteral ',' StringLiteral
      {
        struct statement *stmt;
        struct query_correlate_statement *query;

        ALLOC (stmt);
        stmt->type = CA_SQL_QUERY_CORRELATE;
        query = &stmt->u.query_correlate;
        query->query_A = $3;
        query->query_B = $5;

        $$ = stmt;
      }
    | SAMPLE StringLiteral
      {
        struct statement *stmt;
        struct sample_statement *sample;

        ALLOC (stmt);
        stmt->type = CA_SQL_SAMPLE;
        sample = &stmt->u.sample;
        sample->key = $2;

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
