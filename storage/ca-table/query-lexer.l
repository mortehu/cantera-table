%{
#include <ctype.h>
#include <string.h>

#include "storage/ca-table/arena.h"
#include "storage/ca-table/query-parser.h"
#include "storage/ca-table/query.h"

unsigned int character;
unsigned int line = 0;

int
yyparse();

void
yyerror(YYLTYPE *loc, struct ca_query_parse_context *context, const char *message);

static void
comment(yyscan_t yyscanner);

static int
stringliteral(yyscan_t yyscanner);

static void
strtolower(char *c);
%}
%option reentrant
%option noyywrap
%option bison-bridge
%option bison-locations
%option never-interactive
A [aA]
B [bB]
C [cC]
D [dD]
E [eE]
F [fF]
G [gG]
H [hH]
I [iI]
J [jJ]
K [kK]
L [lL]
M [mM]
N [nN]
O [oO]
P [pP]
Q [qQ]
R [rR]
S [sS]
T [tT]
U [uU]
V [vV]
W [wW]
X [xX]
Y [yY]
Z [zZ]
%%
"/*"                               { character += 2; comment (yyscanner); }
--[^\n]*

{A}{N}{D}                          { character += yyleng; return AND; }
{C}{O}{R}{R}{E}{L}{A}{T}{E}        { character += yyleng; return CORRELATE; }
{C}{S}{V}                          { character += yyleng; return CSV; }
{F}{A}{L}{S}{E}                    { character += yyleng; return FALSE; }
{F}{E}{T}{C}{H}                    { character += yyleng; return FETCH; }
{F}{I}{R}{S}{T}                    { character += yyleng; return FIRST; }
{F}{O}{R}                          { character += yyleng; return FOR; }
{F}{R}{O}{M}                       { character += yyleng; return FROM; }
{F}{O}{R}{M}{A}{T}                 { character += yyleng; return FORMAT; }
{J}{S}{O}{N}                       { character += yyleng; return JSON; }
{K}{E}{Y}                          { character += yyleng; return KEY; }
{K}{E}{Y}{S}                       { character += yyleng; return KEYS; }
{L}{I}{M}{I}{T}                    { character += yyleng; return LIMIT; }
{N}{E}{X}{T}                       { character += yyleng; return NEXT; }
{N}{O}{T}                          { character += yyleng; return NOT; }
{O}{F}{F}{S}{E}{T}                 { character += yyleng; return OFFSET; };
{O}{U}{T}{P}{U}{T}                 { character += yyleng; return OUTPUT; }
{P}{A}{T}{H}                       { character += yyleng; return PATH; }
{Q}{U}{E}{R}{Y}                    { character += yyleng; return QUERY; }
{R}{O}{W}                          { character += yyleng; return ROW; }
{R}{O}{W}{S}                       { character += yyleng; return ROWS; }
{S}{E}{L}{E}{C}{T}                 { character += yyleng; return SELECT; }
{S}{E}{T}                          { character += yyleng; return SET; }
{S}{H}{O}{W}                       { character += yyleng; return SHOW; }
{T}{E}{X}{T}                       { character += yyleng; return TEXT; }
{T}{H}{R}{E}{S}{H}{O}{L}{D}{S}     { character += yyleng; return THRESHOLDS; }
{T}{I}{M}{E}                       { character += yyleng; return TIME; }
{V}{A}{L}{U}{E}{S}                 { character += yyleng; return VALUES; }
{W}{I}{T}{H}                       { character += yyleng; return WITH; }

0x[A-Fa-f0-9]*      { yylval->l = strtol (yytext + 2, 0, 16); character += yyleng; return Integer; }
-?[0-9]+              { yylval->l = strtol (yytext, 0, 0); character += yyleng; return Integer; }
-?[0-9]+\.[0-9]+      { yylval->p = ca_arena_strdup (yyextra, yytext); character += yyleng; return Numeric; }

\' { return stringliteral (yyscanner); }
\" { return stringliteral (yyscanner); }

[A-Za-z_#][A-Za-z0-9_.]* { yylval->p = ca_arena_strdup (yyextra, yytext); strtolower (yylval->p); character += yyleng; return Identifier; }
[ \t\r\026]+             { character += yyleng; }

\n                       { ++line; character = 1; }
\357\273\277             { return UTF8BOM; }
.                        { ++character; return *yytext; }
<<EOF>>                  { return 0; }
%%
static void
comment(yyscan_t yyscanner)
{
  struct yyguts_t *yyg = (struct yyguts_t *) yyscanner;
  int c, last = -1;

  while (EOF != (c = input (yyscanner)))
  {
    ++character;

    if (last == '*' && c == '/')
      return;

    last = c;

    if (c == '\n')
    {
      ++line;
      character = 1;
    }
  }

  unput (c);
}

static int
stringliteral(yyscan_t yyscanner)
{
  struct yyguts_t *yyg = (struct yyguts_t *) yyscanner;
  int quote_char, ch;
  char* result = NULL;
  size_t result_size = 0, result_alloc = 0;

  quote_char = yytext[0];

  while (EOF != (ch = input (yyscanner)))
    {
      ++character;

      if (ch == quote_char)
        {
          ch = input (yyscanner);

          if (ch != quote_char)
            break;
        }

      if (ch == '\n')
        {
          ++line;
          character = 1;
        }

      if (result_size == result_alloc) {
        result_alloc = result_alloc * 3 / 2 + 256;
        char* new_result = realloc(result, result_alloc);
        if (!new_result) goto fail;
        result = new_result;
      }

      result[result_size++] = ch;
    }

  unput (ch);

  if (!(yylval->p = ca_arena_strndup (yyextra, result, result_size)))
    goto fail;

  free(result);

  return (quote_char == '"') ? Identifier : StringLiteral;

fail:

  free(result);

  return -1;
}

static void
strtolower(char *c)
{
  for (; *c; ++c)
    *c = tolower (*c);
}

int
CA_parse_script(struct ca_query_parse_context *context, FILE *input)
{
  YY_BUFFER_STATE buf;
  int result = -1;

  yylex_init (&context->scanner);

  ca_arena_init (&context->arena);

  if (NULL != (buf = yy_create_buffer (input, YY_BUF_SIZE, context->scanner)))
  {
    buf->yy_is_interactive = 1;

    character = 1;
    line = 1;

    yy_switch_to_buffer (buf, context->scanner);
    yyset_extra (&context->arena, context->scanner);
    result = yyparse (context);
    yy_delete_buffer (buf, context->scanner);
  }

  ca_arena_free (&context->arena);
  yylex_destroy (context->scanner);

  if (result >= 0 && context->error)
    result = -1;

  return result;
}
