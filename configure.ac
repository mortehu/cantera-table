AC_INIT([ca-table], [1.2.1])
AM_INIT_AUTOMAKE([-Wall foreign subdir-objects])

AC_CONFIG_MACRO_DIR([m4])

AM_MAINTAINER_MODE([disable])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

PKG_CHECK_MODULES([CA_CAS], [ca-cas])
PKG_CHECK_MODULES([CAPNP], [capnp >= 0.6])
PKG_CHECK_MODULES([CAPNP_RPC], [capnp-rpc >= 0.6])
PKG_CHECK_MODULES([JSONCPP], [jsoncpp])
PKG_CHECK_MODULES([LIBCOLUMNFILE], [libcolumnfile])
PKG_CHECK_MODULES([YAML], [yaml-cpp >= 0.5])

AC_PROG_CC
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_MAKE_SET
AC_PROG_YACC
AM_PROG_AR
AM_PROG_LEX

# Quite weirdly on Mac OS X the fdatasync() function is not declared
# in system headers but it passes the link check. So check for both
# conditions.
AC_CHECK_FUNCS(fdatasync)
AC_CHECK_DECLS(fdatasync)

LT_INIT

AX_AC_EXPAND([SYSCONFDIR], $sysconfdir)
AC_DEFINE_UNQUOTED([SYSCONFDIR], ["$SYSCONFDIR"], [read-only single-machine data])

AC_LANG_PUSH([C++])
AX_CXX_COMPILE_STDCXX_14([], [])
AC_LANG_POP([C++])

AC_CONFIG_HEADERS([config.h third_party/oroch/oroch/config.h])
AC_CONFIG_FILES([Makefile ca-table.pc])
AC_OUTPUT
