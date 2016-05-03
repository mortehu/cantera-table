lib_LTLIBRARIES = \
  libca-table.la

bin_PROGRAMS = \
  ca-dump \
  ca-load \
  ca-shell

ca_table_includedir = $(includedir)/ca-table
ca_table_include_HEADERS = \
  src/ca-table.h

noinst_PROGRAMS = \
  src/format_benchmark

noinst_LIBRARIES =

BUILT_SOURCES = \
  src/query-lexer.cc \
  src/query-parser.cc

AM_YFLAGS = -d

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -Isrc $(JSONCPP_CFLAGS)
AM_LDFLAGS = -lpthread

include third_party/gtest/Makefile.am

EXTRA_DIST = \
  LICENSE \
  README.md \
  third_party/oroch/LICENSE \
  third_party/oroch/README.md

libca_table_la_SOURCES = \
  src/delegate.h \
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
  src/table-write.cc \
  src/table.cc \
  src/thread-pool.h \
  src/util.cc \
  src/util.h \
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
  -lleveldb \
  -lre2
libca_table_la_LDFLAGS = \
  -no-undefined \
  -version-info 1:0:1 \
  -export-symbols-regex '^_ZN7cantera5table.*'

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
  $(YAML_LIBS) \
  -lreadline \
  -ltcmalloc

ca_load_SOURCES = \
  src/ca-load.cc
ca_load_LDADD = \
  libca-table.la \
  $(CAPNP_LIBS) \
  $(LIBCOLUMNFILE_LIBS) \
  $(YAML_LIBS) \
  -lre2

ca_dump_SOURCES = \
  src/ca-dump.cc
ca_dump_LDADD = \
  libca-table.la \
  $(CAPNP_LIBS) \
  $(YAML_LIBS) \
  -lre2

src_format_benchmark_SOURCES = \
  src/format_benchmark.cc
src_format_benchmark_LDADD = \
  libca-table.la \
  $(CAPNP_LIBS) \
  $(YAML_LIBS)
