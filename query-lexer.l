%{
#if HAVE_CONFIG_H
#  include "config.h"
#endif

#include <ctype.h>
#include <string.h>

#include "arena.h"
#include "memory.h"
#include "query-parser.h"
#include "query.h"

static unsigned int character;
static unsigned int line = 0;

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
"/*"                              { character += 2; comment (yyscanner); }
--[^\n]*

{A}{N}{D}                         { character += yyleng; return AND; }
{C}{R}{E}{A}{T}{E}                { character += yyleng; return CREATE; }
{F}{R}{O}{M}                      { character += yyleng; return FROM; }
{N}{O}{T}                         { character += yyleng; return NOT; }
{N}{U}{L}{L}                      { character += yyleng; return _NULL; }
{K}{E}{Y}                         { character += yyleng; return KEY; }
{O}{R}                            { character += yyleng; return OR; }
{P}{A}{T}{H}                      { character += yyleng; return PATH; }
{P}{R}{I}{M}{A}{R}{Y}             { character += yyleng; return PRIMARY; }
{S}{E}{L}{E}{C}{T}                { character += yyleng; return SELECT; }
{S}{H}{O}{W}                      { character += yyleng; return SHOW; }
{T}{A}{B}{L}{E}                   { character += yyleng; return TABLE; }
{T}{A}{B}{L}{E}{S}                { character += yyleng; return TABLES; }
{T}{E}{X}{T}                      { character += yyleng; return TEXT; }
{T}{I}{M}{E}_{F}{L}{O}{A}{T}4     { character += yyleng; return TIME_FLOAT4; }
{W}{H}{E}{R}{E}                   { character += yyleng; return WHERE; }

0x[A-Fa-f0-9]*      { yylval->l = strtol (yytext + 2, 0, 16); character += yyleng; return Integer; }
[0-9]+              { yylval->l = strtol (yytext, 0, 0); character += yyleng; return Integer; }
[0-9]+\.[0-9]+      { yylval->p = ca_arena_strdup (yyextra, yytext); character += yyleng; return Numeric; }

\' { return stringliteral (yyscanner); }
\" |

[A-Za-z_#][A-za-z0-9_]*  { yylval->p = ca_arena_strdup (yyextra, yytext); strtolower (yylval->p); character += yyleng; return Identifier; }
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
  char *result = NULL;
  size_t result_alloc = 0, result_size = 0;

  quote_char = yytext[0];

  while (EOF != (ch = input (yyscanner)))
    {
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

      if (result_size == result_alloc
          && -1 == ARRAY_GROW (&result, &result_alloc))
        {
          goto fail;
        }

      result[result_size++] = ch;
    }

  unput (ch);

  if (!(yylval->p = ca_arena_strndup (yyextra, result, result_size)))
    goto fail;

  free (result);

  return StringLiteral;

fail:

  free (result);

  return -1;
}

static void
strtolower(char *c)
{
  while (*c)
    {
      *c = tolower (*c);
      ++c;
    }
}

int
ca_schema_parse_script(struct ca_schema *schema, FILE *input)
{
  struct ca_query_parse_context context;
  YY_BUFFER_STATE buf;
  int result = -1;

  memset (&context, 0, sizeof (context));
  context.schema = schema;

  yylex_init (&context.scanner);

  ca_arena_init (&context.arena);

  if (0 != (buf = yy_create_buffer (input, YY_BUF_SIZE, context.scanner)))
  {
    character = 1;
    line = 1;

    yy_switch_to_buffer (buf, context.scanner);
    yyset_extra (&context.arena, context.scanner);
    result = yyparse (&context);
    yy_delete_buffer (buf, context.scanner);
  }

  ca_arena_free (&context.arena);
  yylex_destroy (context.scanner);

  if (result >= 0 && context.error)
    result = -1;

  return result;
}
