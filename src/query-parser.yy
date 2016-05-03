%pure-parser

%locations
%defines
%error-verbose

%code requires
{
#include "src/query.h"

using namespace cantera::table;
}

%parse-param { QueryParseContext *context }
%lex-param { void* scanner  }

%union
{
  LinkedList<double>* double_list;
  Query* query;
  QueryList* query_list;
  Statement* statement;
  const char* c;
  ThresholdClause* threshold_clause;

  RuntimeParameter runtime_parameter;
  RuntimeParameterValue runtime_parameter_value;
  double d;
  long l;
}

%{
#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <cstdio>
#include <cstring>
#include <ctime>

#include <kj/debug.h>

template <typename T>
void Allocate(QueryParseContext* context, T*& t) {
  t = &context->arena.allocate<T>();
}

#define ALLOC(t) do { Allocate(context, t); } while (0)

extern int yylex (YYSTYPE * yylval_param,YYLTYPE * yylloc_param ,void* yyscanner);

void
yyerror (YYLTYPE *loc, QueryParseContext *context, const char *message);

#define scanner context->scanner
%}

%token AND CREATE FROM INDEX KEY LIMIT NOT OFFSET_SCORE OR PATH PRIMARY QUERY
%token AND_NOT SHOW TABLES TEXT TIME UTF8BOM KEYS
%token WHERE WITH SUMMARIES
%token OFFSET FETCH FIRST NEXT ROW ROWS ONLY
%token TRUE FALSE
%token INTO VALUES ORDER_BY
%token SELECT MAX MIN RANDOM_SAMPLE
%token SET OUTPUT FORMAT CSV JSON
%token CORRELATE PARSE
%token THRESHOLDS FOR

%token Date
%token Identifier
%token Integer
%token Numeric
%token StringLiteral

%type<d> number

%type<c> StringLiteral Identifier Date Numeric

%type<double_list> integerList
%type<query> query subQuery subQueryList
%type<query_list> queryList
%type<statement> statement
%type<threshold_clause> thresholdClause

%type<l> Integer
%type<l> fetchClause
%type<l> keysClause
%type<l> offsetClause
%type<l> optionalWithSummaries
%type<runtime_parameter> runtimeParameter
%type<runtime_parameter_value> runtimeParameterValue

%left '+' OR '-' AND_NOT AND

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
    : QUERY keysClause query thresholdClause fetchClause offsetClause
      {
        Statement *stmt;
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
    | CORRELATE QUERY query ',' query
      {
        Statement *stmt;
        struct query_correlate_statement *query;

        ALLOC (stmt);
        stmt->type = kStatementCorrelate;
        query = &stmt->u.query_correlate;
        query->query_A = $3;
        query->query_B = $5;

        $$ = stmt;
      }
    | PARSE subQueryList
      {
        Statement* stmt;
        ALLOC(stmt);
        stmt->type = kStatementParse;
        stmt->u.parse.query = $2;

        $$ = stmt;
      }
    | SELECT queryList FROM query optionalWithSummaries
      {
        Statement *stmt;
        struct select_statement *select;

        ALLOC (stmt);
        stmt->type = kStatementSelect;
        select = &stmt->u.select;
        select->fields = $2;
        select->query = $4;
        select->with_summaries = $5;

        $$ = stmt;
      }
    | SET runtimeParameter runtimeParameterValue
      {
        Statement *stmt;
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
        Statement *stmt;
        struct set_statement *set;

        ALLOC (stmt);
        stmt->type = kStatementSet;
        set = &stmt->u.set;
        set->parameter = CA_PARAM_TIME_FORMAT;
        set->v.string_value = $4;

        $$ = stmt;
      }
    ;

number
    : Integer { $$ = $1; }
    | Numeric { $$ = strtod($1, NULL); }
    | Date
      {
        struct tm token_tm;
        memset(&token_tm, 0, sizeof(token_tm));
        strptime($1, "%Y-%m-%d", &token_tm);
        $$ = timegm(&token_tm) / 86400;
      }
    ;

query
    : '(' subQueryList ')' { $$ = $2; }
    ;

subQuery
    : '(' subQueryList ')' { $$ = $2; }
    | Identifier
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryLeaf;
        q->identifier = $1;
        $$ = q;
      }
    | KEY '=' Identifier
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryKey;
        q->identifier = $3;
        $$ = q;
      }
    | MAX '(' subQuery ')'
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryUnaryOperator;
        q->operator_type = kOperatorMax;
        q->lhs = $3;
        $$ = q;
      }
    | MIN '(' subQuery ')'
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryUnaryOperator;
        q->operator_type = kOperatorMin;
        q->lhs = $3;
        $$ = q;
      }
    | RANDOM_SAMPLE '(' subQuery ',' Integer ')'
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorRandomSample;
        q->lhs = $3;
        q->value = $5;
        $$ = q;
      }
    | '~' subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryUnaryOperator;
        q->operator_type = kOperatorNegate;
        q->lhs = $2;
        $$ = q;
      }
    | subQuery '=' number
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorEQ;
        q->lhs = $1;
        q->value = $3;
        $$ = q;
      }
    | subQuery '>' number
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorGT;
        q->lhs = $1;
        q->value = $3;
        $$ = q;
      }
    | subQuery '>' '=' number
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorGE;
        q->lhs = $1;
        q->value = $4;
        $$ = q;
      }
    | subQuery '<' number
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorLT;
        q->lhs = $1;
        q->value = $3;
        $$ = q;
      }
    | subQuery '<' '=' number
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorLE;
        q->lhs = $1;
        q->value = $4;
        $$ = q;
      }
    | subQuery '>' subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorGT;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQuery '<' subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorLT;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQuery '[' number ',' number ']'
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorInRange;
        q->lhs = $1;
        q->value = $3;
        q->value2 = $5;
        $$ = q;
      }
    | subQueryList ORDER_BY subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorOrderBy;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQueryList '+' subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorOr;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQueryList OR subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorOr;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQueryList '-' subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorSubtract;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQueryList AND_NOT subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorSubtract;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    | subQueryList AND subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorAnd;
        q->lhs = $1;
        q->rhs = $3;
        $$ = q;
      }
    ;

subQueryList
    : subQueryList subQuery
      {
        struct Query *q;
        ALLOC(q);
        q->type = kQueryBinaryOperator;
        q->operator_type = kOperatorAnd;
        q->lhs = $1;
        q->rhs = $2;
        $$ = q;
      }
    | subQuery { $$ = $1; }
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

optionalWithSummaries
    :                { $$ = 0; }
    | WITH SUMMARIES { $$ = 1; }
    ;

thresholdClause
    : { $$ = 0; }
    | THRESHOLDS integerList FOR KEY StringLiteral
      {
        ThresholdClause* th;
        ALLOC(th);
        th->values = $2;
        th->key = $5;
        $$ = th;
      }
    ;

integerList
    : Numeric ',' integerList
      {
        LinkedList<double>* il;
        ALLOC(il);
        il->value = strtod($1, 0);
        il->next = $3;
        $$ = il;
      }
    | Integer ',' integerList
      {
        LinkedList<double>* il;
        ALLOC(il);
        il->value = $1;
        il->next = $3;
        $$ = il;
      }
    | Numeric
      {
        LinkedList<double>* il;
        ALLOC(il);
        il->value = strtod($1, 0);
        $$ = il;
      }
    | Integer
      {
        LinkedList<double>* il;
        ALLOC(il);
        il->value = $1;
        $$ = il;
      }
    ;

queryList
    : subQueryList ',' queryList
      {
        QueryList* ql;
        ALLOC(ql);
        ql->query = $1;
        ql->next = $3;
        $$ = ql;
      }
    | subQueryList
      {
        QueryList* ql;
        ALLOC(ql);
        ql->query = $1;
        $$ = ql;
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

void yyerror(YYLTYPE* loc, QueryParseContext* context, const char* message) {
  KJ_FAIL_REQUIRE(message, line, character);
}
