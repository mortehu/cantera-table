AC_INIT([ca-table], [1.1])
AM_INIT_AUTOMAKE([foreign subdir-objects])

AC_CONFIG_MACRO_DIR([m4])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CANONICAL_HOST

AC_GNU_SOURCE

PKG_CHECK_MODULES([JSONCPP], [jsoncpp])
PKG_CHECK_MODULES([CRYPTO], [libcrypto])
PKG_CHECK_MODULES([PYTHON2], [python-2.7])
PKG_CHECK_MODULES([PYTHON3], [python-3.4])
PKG_CHECK_MODULES([YAML], [yaml-cpp])

AC_PROG_SWIG(2.0)
SWIG_ENABLE_CXX
SWIG_PYTHON

AC_ARG_ENABLE([python],
[  --enable-python   Build Python module],
[case "${enableval}" in
  yes) python=true ;;
  no)  python=false ;;
  *) AC_MSG_ERROR([bad value ${enableval} for --enable-python]) ;;
esac],[python=true])
AM_CONDITIONAL([PYTHON], [test x$python = xtrue])

AC_CHECK_HEADERS([linux/fiemap.h])

# For libdivsufsort
AC_DEFINE(PROJECT_VERSION_FULL, [PACKAGE_VERSION], [Define to the version of this package.])

AM_PROG_AR
AM_PROG_CC_C_O
AM_PROG_LEX
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_MAKE_SET
AC_PROG_YACC

LT_INIT

AC_LANG_PUSH([C++])
AX_CXX_COMPILE_STDCXX_14([ext])
AC_LANG_POP([C++])

AX_AC_EXPAND([BINDIR], $bindir)
AX_AC_EXPAND([LIBDIR], $libdir)
AX_AC_EXPAND([LOCALSTATEDIR], $localstatedir)
AX_AC_EXPAND([SYSCONFDIR], $sysconfdir)
AX_AC_EXPAND([DATAROOTDIR], $datarootdir)

AC_DEFINE_UNQUOTED([BINDIR], ["$BINDIR"], [Path of user executables])
AC_DEFINE_UNQUOTED([LIBDIR], ["$LIBDIR"], [Path of object code libraries])
AC_DEFINE_UNQUOTED([LOCALSTATEDIR], ["$LOCALSTATEDIR"], [Path of local state])
AC_DEFINE_UNQUOTED([SYSCONFDIR], ["$SYSCONFDIR"], [Path of system configuration])
AC_DEFINE_UNQUOTED([DATAROOTDIR], ["$DATAROOTDIR"], [Path of data])
AC_DEFINE_UNQUOTED([MAXSYSLOGMSGLEN], [10240], [Define to the maximum message length to pass to syslog.])

AX_LIB_READLINE

AC_USE_SYSTEM_EXTENSIONS

AC_DEFINE(__STDC_CONSTANT_MACROS, [], [Force definition of constant macros for C++])
AC_DEFINE(__STDC_FORMAT_MACROS, [], [Force definition of format macros for C++])
AC_DEFINE(__STDC_LIMIT_MACROS, [], [Force definition of limit macros for C++])

ACX_CHECK_FORMAT_ATTRIBUTE
ACX_CHECK_UNUSED_ATTRIBUTE
ACX_TYPE_U_CHAR
ACX_TYPE_SOCKLEN_T
ACX_TYPE_IN_ADDR_T
ACX_TYPE_IN_PORT_T
ACX_CHECK_MEMCMP_SIGNED

AH_BOTTOM(
AHX_CONFIG_FORMAT_ATTRIBUTE
AHX_CONFIG_UNUSED_ATTRIBUTE
AHX_CONFIG_FSEEKO
AHX_CONFIG_IPV6_MIN_MTU
AHX_CONFIG_MAXHOSTNAMELEN
AHX_CONFIG_W32_FD_SET_T
)

CFLAGS="$CFLAGS -fuse-ld=gold"
CXXFLAGS="$CXXFLAGS -fuse-ld=gold"

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
