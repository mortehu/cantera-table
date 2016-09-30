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

AC_CHECK_FUNCS(get_current_dir_name)
AC_CHECK_FUNCS(fmemopen)
AC_CHECK_FUNCS(fallocate)
AC_CHECK_FUNCS(fdatasync)
AC_CHECK_FUNCS(fputs_unlocked fwrite_unlocked)
# Quite weirdly on Mac OS X the fdatasync() function is not declared
# in system headers but it passes the above check. So check if it is
# declared as well.
AC_CHECK_DECLS(fdatasync)

LT_INIT

# Debian patches libtool to reset these to no.
# Cancel the effect of Debian's patch here.
link_all_deplibs=yes
link_all_deplibs_CXX=yes

AX_AC_EXPAND([SYSCONFDIR], $sysconfdir)
AC_DEFINE_UNQUOTED([SYSCONFDIR], ["$SYSCONFDIR"], [read-only single-machine data])

AC_LANG_PUSH([C++])
AX_CXX_COMPILE_STDCXX_14([], [])
AC_LANG_POP([C++])

AC_CONFIG_HEADERS([config.h third_party/oroch/oroch/config.h])
AC_CONFIG_FILES([Makefile ca-table.pc])
AC_OUTPUT
