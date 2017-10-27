%{
#include <cctype>
#include <cstring>
#include <string>

#include <kj/debug.h>

#include "src/query-parser.hh"
#include "src/query.h"

unsigned int character;
unsigned int line = 0;

int
yyparse();

void
yyerror(YYLTYPE *loc, QueryParseContext *context, const char *message);

static void
comment(yyscan_t yyscanner);

static int
stringliteral(yyscan_t yyscanner);
%}
%option extra-type="kj::Arena*"
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
{A}{N}{D}\ {N}{O}{T}               { character += yyleng; return AND_NOT; }
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
{M}{A}{X}                          { character += yyleng; return MAX; }
{M}{I}{N}                          { character += yyleng; return MIN; }
{N}{E}{X}{T}                       { character += yyleng; return NEXT; }
{N}{O}{T}                          { character += yyleng; return NOT; }
{O}{F}{F}{S}{E}{T}                 { character += yyleng; return OFFSET; }
{O}{U}{T}{P}{U}{T}                 { character += yyleng; return OUTPUT; }
{O}{R}                             { character += yyleng; return OR; }
{O}{R}{D}{E}{R}\ {B}{Y}            { character += yyleng; return ORDER_BY; }
{P}{A}{R}{A}{L}{L}{E}{L}           { character += yyleng; return PARALLEL; }
{P}{A}{R}{S}{E}                    { character += yyleng; return PARSE; }
{P}{A}{T}{H}                       { character += yyleng; return PATH; }
{Q}{U}{E}{R}{Y}                    { character += yyleng; return QUERY; }
{R}{A}{N}{D}{O}{M}_{S}{A}{M}{P}{L}{E} { character += yyleng; return RANDOM_SAMPLE; }
{R}{O}{W}                          { character += yyleng; return ROW; }
{R}{O}{W}{S}                       { character += yyleng; return ROWS; }
{S}{E}{L}{E}{C}{T}                 { character += yyleng; return SELECT; }
{S}{E}{T}                          { character += yyleng; return SET; }
{S}{H}{O}{W}                       { character += yyleng; return SHOW; }
{S}{U}{M}{M}{A}{R}{I}{E}{S}        { character += yyleng; return SUMMARIES; }
{T}{E}{X}{T}                       { character += yyleng; return TEXT; }
{T}{H}{R}{E}{S}{H}{O}{L}{D}{S}     { character += yyleng; return THRESHOLDS; }
{T}{I}{M}{E}                       { character += yyleng; return TIME; }
{V}{A}{L}{U}{E}{S}                 { character += yyleng; return VALUES; }
{W}{I}{T}{H}                       { character += yyleng; return WITH; }

0x[A-Fa-f0-9]*      { yylval->l = strtol (yytext + 2, 0, 16); character += yyleng; return Integer; }
[1-9][0-9]*-[01][0-9]-[0123][0-9] { yylval->c = yyextra->copyString(kj::StringPtr(yytext, yyleng)).cStr(); character += yyleng; return Date; }
-?[0-9]+            { yylval->l = strtol (yytext, 0, 0); character += yyleng; return Integer; }
-?[0-9]+\.[0-9]+    { yylval->c = yyextra->copyString(kj::StringPtr(yytext, yyleng)).cStr(); character += yyleng; return Numeric; }

\' { return stringliteral (yyscanner); }
\" { return stringliteral (yyscanner); }

[A-Za-z_#.:%@/][A-Za-z0-9_.:%@/-]* { yylval->c = yyextra->copyString(kj::StringPtr(yytext, yyleng)).cStr(); character += yyleng; return Identifier; }
[ \t\r\026]+                  { character += yyleng; }

\n                       { ++line; character = 1; }
\357\273\277             { return UTF8BOM; }
.                        { ++character; return *yytext; }
<<EOF>>                  { return 0; }
%%
static void comment(yyscan_t yyscanner) {
  struct yyguts_t* yyg = (struct yyguts_t*)yyscanner;
  int c, last = -1;

  while (EOF != (c = yyinput(yyscanner))) {
    ++character;

    if (last == '*' && c == '/') return;

    last = c;

    if (c == '\n') {
      ++line;
      character = 1;
    }
  }

  unput(c);
}

static int stringliteral(yyscan_t yyscanner) {
  struct yyguts_t* yyg = (struct yyguts_t*)yyscanner;
  int quote_char, ch;
  std::string result;

  quote_char = yytext[0];

  while (EOF != (ch = yyinput(yyscanner))) {
    ++character;

    if (ch == quote_char) {
      ch = yyinput(yyscanner);

      if (ch != quote_char) break;
    }

    if (ch == '\n') {
      ++line;
      character = 1;
    }

    result.push_back(ch);
  }

  unput(ch);

  yylval->c = yyextra->copyString(kj::StringPtr(result.data(), result.size())).cStr();

  return (quote_char == '"') ? Identifier : StringLiteral;
}

namespace cantera {
namespace table {

void CA_parse_script(QueryParseContext* context, FILE* input) {
  YY_BUFFER_STATE buf;

  yylex_init(&context->scanner);

  if (NULL != (buf = yy_create_buffer(input, YY_BUF_SIZE, context->scanner))) {
    buf->yy_is_interactive = 1;

    character = 1;
    line = 1;

    yy_switch_to_buffer(buf, context->scanner);
    yyset_extra(&context->arena, context->scanner);
    KJ_REQUIRE(0 == yyparse(context));
    yy_delete_buffer(buf, context->scanner);
  }

  yylex_destroy(context->scanner);
}

}  // namespace table
}  // namespace cantera
