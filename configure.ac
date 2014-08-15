AC_INIT(cantera-table,1.0.0)

AC_CANONICAL_TARGET
AC_USE_SYSTEM_EXTENSIONS

AM_INIT_AUTOMAKE([-Wall -Werror])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_MACRO_DIR([m4])

AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL
AC_PROG_INSTALL
AC_PROG_MAKE_SET

AM_PROG_LEX
AC_PROG_YACC
AX_LIB_READLINE

AC_GNU_SOURCE

AC_DEFINE(__STDC_CONSTANT_MACROS, [], [Force definition of constant macros for C++])
AC_DEFINE(__STDC_FORMAT_MACROS, [], [Force definition of format macros for C++])
AC_DEFINE(__STDC_LIMIT_MACROS, [], [Force definition of limit macros for C++])

AS_AC_EXPAND(BINDIR, $bindir)
AS_AC_EXPAND(DATADIR, $datadir)
AS_AC_EXPAND(DATAROOTDIR, $datarootdir)
AS_AC_EXPAND(DOCDIR, $docdir)
AS_AC_EXPAND(DVIDIR, $dvidir)
AS_AC_EXPAND(HTMLDIR, $htmldir)
AS_AC_EXPAND(INCLUDEDIR, $includedir)
AS_AC_EXPAND(INFODIR, $infodir)
AS_AC_EXPAND(LIBDIR, $libdir)
AS_AC_EXPAND(LIBEXECDIR, $libexecdir)
AS_AC_EXPAND(LOCALEDIR, $localedir)
AS_AC_EXPAND(LOCALSTATEDIR, $localstatedir)
AS_AC_EXPAND(MANDIR, $mandir)
AS_AC_EXPAND(OLDINCLUDEDIR, $oldincludedir)
AS_AC_EXPAND(PDFDIR, $pdfdir)
AS_AC_EXPAND(PSDIR, $psdir)
AS_AC_EXPAND(SBINDIR, $sbindir)
AS_AC_EXPAND(SHAREDSTATEDIR, $sharedstatedir)
AS_AC_EXPAND(SYSCONFDIR, $sysconfdir)

AC_DEFINE_UNQUOTED([BINDIR], ["$BINDIR"], [user executables])
AC_DEFINE_UNQUOTED([SBINDIR], ["$SBINDIR"], [system admin executables])
AC_DEFINE_UNQUOTED([LIBEXECDIR], ["$LIBEXECDIR"], [program executables])
AC_DEFINE_UNQUOTED([SYSCONFDIR], ["$SYSCONFDIR"], [read-only single-machine data])
AC_DEFINE_UNQUOTED([SHAREDSTATEDIR], ["$SHAREDSTATEDIR"], [modifiable architecture-independent data])
AC_DEFINE_UNQUOTED([LOCALSTATEDIR], ["$LOCALSTATEDIR"], [modifiable single-machine data])
AC_DEFINE_UNQUOTED([LIBDIR], ["$LIBDIR"], [object cod libraries])
AC_DEFINE_UNQUOTED([INCLUDEDIR], ["$INCLUDEDIR"], [C header files])
AC_DEFINE_UNQUOTED([OLDINCLUDEDIR], ["$OLDINCLUDEDIR"], [C header files for non-gcc])
AC_DEFINE_UNQUOTED([DATAROOTDIR], ["$DATAROOTDIR"], [read-only arch.-independent data root])
AC_DEFINE_UNQUOTED([DATADIR], ["$DATADIR"], [read-only architecture-independent data])
AC_DEFINE_UNQUOTED([INFODIR], ["$INFODIR"], [info documentation])
AC_DEFINE_UNQUOTED([LOCALEDIR], ["$LOCALEDIR"], [locale-dependent data])
AC_DEFINE_UNQUOTED([MANDIR], ["$MANDIR"], [man documentation])
AC_DEFINE_UNQUOTED([DOCDIR], ["$DOCDIR"], [documentation root])
AC_DEFINE_UNQUOTED([HTMLDIR], ["$HTMLDIR"], [html documentation])
AC_DEFINE_UNQUOTED([DVIDIR], ["$DVIDIR"], [dvi documentation])
AC_DEFINE_UNQUOTED([PDFDIR], ["$PDFDIR"], [pdf documentation])
AC_DEFINE_UNQUOTED([PSDIR], ["$PSDIR"], [ps documentation])

AC_LANG_PUSH([C++])
AX_CXX_COMPILE_STDCXX_11([noext])
AC_LANG_POP([C++])

AC_CONFIG_HEADERS([config.h])
AC_OUTPUT([Makefile])
