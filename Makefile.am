lib_LTLIBRARIES = \
  libca-table.la

bin_PROGRAMS = \
  ca-dump \
  ca-load \
  ca-shell

ca_table_includedir = $(includedir)/ca-table
ca_table_include_HEADERS = \
  src/ca-table.h

check_PROGRAMS = \
  src/format_test \
  src/table-backend-leveldb-table_test \
  src/table-backend-writeonce_test \
  src/ca-load_test

noinst_PROGRAMS = \
  src/format_benchmark

noinst_LIBRARIES =

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = ca-table.pc

BUILT_SOURCES = \
  src/query-lexer.cc \
  src/query-parser.cc \
  src/query-parser.hh

TESTS = $(check_PROGRAMS)

AM_YFLAGS = -d

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -Isrc $(CAPNP_CFLAGS) $(JSONCPP_CFLAGS) $(ZSTD_CFLAGS)
AM_LDFLAGS = -lpthread

include third_party/gtest/Makefile.am

EXTRA_DIST = \
  LICENSE \
  README.md \
  debian/ca-table.dirs \
  debian/ca-table.install \
  debian/changelog \
  debian/compat \
  debian/control \
  debian/copyright \
  debian/libca-table-dev.dirs \
  debian/libca-table-dev.install \
  debian/libca-table1.dirs \
  debian/libca-table1.install \
  debian/rules \
  debian/source \
  debian/source/format \
  third_party/evenk/LICENSE \
  third_party/evenk/README.md \
  third_party/oroch/LICENSE \
  third_party/oroch/README.md

libca_table_la_SOURCES = \
  src/context.cc \
  src/context.h \
  src/format.cc \
  src/keywords.cc \
  src/keywords.h \
  src/merge.cc \
  src/output.cc \
  src/parse.cc \
  src/query.h \
  src/rle.c \
  src/rle.h \
  src/schema.cc \
  src/schema.h \
  src/table-backend-leveldb-table.cc \
  src/table-backend-leveldb-table.h \
  src/table-backend-writeonce.cc \
  src/table-backend-writeonce.h \
  src/table-backend.cc \
  src/table-backend.h \
  src/table-write.cc \
  src/table.cc \
  src/util.cc \
  src/util.h \
  third_party/evenk/evenk/backoff.h \
  third_party/evenk/evenk/basic.h \
  third_party/evenk/evenk/bounded_queue.h \
  third_party/evenk/evenk/conqueue.h \
  third_party/evenk/evenk/futex.h \
  third_party/evenk/evenk/spinlock.h \
  third_party/evenk/evenk/synch.h \
  third_party/evenk/evenk/synch_queue.h \
  third_party/evenk/evenk/task.h \
  third_party/evenk/evenk/thread.h \
  third_party/evenk/evenk/thread_pool.h \
  third_party/oroch/oroch/bitfor.h \
  third_party/oroch/oroch/bitpck.h \
  third_party/oroch/oroch/bitpfr.h \
  third_party/oroch/oroch/common.h \
  third_party/oroch/oroch/config.h \
  third_party/oroch/oroch/config.h.in \
  third_party/oroch/oroch/integer_codec.h \
  third_party/oroch/oroch/integer_group.h \
  third_party/oroch/oroch/integer_traits.h \
  third_party/oroch/oroch/naught.h \
  third_party/oroch/oroch/normal.h \
  third_party/oroch/oroch/offset.h \
  third_party/oroch/oroch/origin.h \
  third_party/oroch/oroch/varint.h \
  third_party/oroch/oroch/zigzag.h
libca_table_la_LIBADD = \
  $(CAPNP_LIBS) \
  $(YAML_LIBS) \
  $(ZSTD_LIBS) \
  -lleveldb \
  -lre2
libca_table_la_LDFLAGS = \
  -no-undefined \
  -version-info 2:0:0 \
  -export-symbols-regex '.*_ZNK?7cantera5table.*'

ca_shell_SOURCES = \
  src/ca-shell.cc \
  src/correlate.cc \
  src/select.cc \
  src/select.h \
  src/statement.cc \
  src/query.cc \
  src/query-parser.yy \
  src/query-lexer.ll
ca_shell_LDADD = \
  libca-table.la \
  $(CA_CAS_LIBS) \
  $(CAPNP_RPC_LIBS) \
  $(JSONCPP_LIBS) \
  -lreadline

ca_load_SOURCES = \
  src/ca-load.cc
ca_load_LDADD = \
  libca-table.la \
  $(LIBCOLUMNFILE_LIBS) \
  -lre2

ca_dump_SOURCES = \
  src/ca-dump.cc
ca_dump_LDADD = \
  libca-table.la \
  -lre2

src_format_test_SOURCES = \
  src/format_test.cc
src_format_test_LDADD = \
  libca-table.la \
  third_party/gtest/libgtest.a

src_format_benchmark_SOURCES = \
  src/format_benchmark.cc
src_format_benchmark_LDADD = \
  libca-table.la

src_table_backend_leveldb_table_test_SOURCES = \
  src/table-backend-leveldb-table_test.cc
src_table_backend_leveldb_table_test_LDADD = \
  libca-table.la \
  third_party/gtest/libgtest.a

src_table_backend_writeonce_test_SOURCES = \
  src/table-backend-writeonce_test.cc
src_table_backend_writeonce_test_LDADD = \
  libca-table.la \
  third_party/gtest/libgtest.a

src_ca_load_test_SOURCES = \
  src/ca-load_test.cc
src_ca_load_test_LDADD = \
  libca-table.la \
  third_party/gtest/libgtest.a
