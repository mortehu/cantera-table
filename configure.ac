AC_INIT(cantera-table,1.0.0)
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

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

AC_DEFINE(__STDC_CONSTANT_MACROS, [], [Force definition of constant macros for C++])
AC_DEFINE(__STDC_FORMAT_MACROS, [], [Force definition of format macros for C++])
AC_DEFINE(__STDC_LIMIT_MACROS, [], [Force definition of limit macros for C++])

AC_LANG_PUSH([C++])
AC_CHECK_HEADERS([llvm/Support/TargetSelect.h llvm/Target/TargetSelect.h llvm/Support/IRBuilder.h llvm/IRBuilder.h])
AC_LANG_POP([C++])

AC_CONFIG_HEADERS([config.h])
AC_OUTPUT([Makefile])
