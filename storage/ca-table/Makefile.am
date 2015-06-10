man1_MANS += storage/ca-table/doc/ca-shell.1 storage/ca-table/doc/ca-sql.1

BUILT_SOURCES += storage/ca-table/query-lexer.c storage/ca-table/query-parser.c

AM_YFLAGS = -d

bin_PROGRAMS += \
  storage/ca-table/ca-dump \
  storage/ca-table/ca-load \
  storage/ca-table/ca-shell

noinst_PROGRAMS += \
  storage/ca-table/format_benchmark

noinst_LTLIBRARIES += storage/ca-table/libca-table.la

storage_ca_table_libca_table_la_SOURCES = \
  storage/ca-table/arena.c storage/ca-table/arena.h \
  storage/ca-table/crc32c.c storage/ca-table/crc32c.h \
  storage/ca-table/error.cc storage/ca-table/error.h \
  storage/ca-table/format.cc \
  storage/ca-table/keywords.cc \
  storage/ca-table/merge.cc \
  storage/ca-table/output.cc \
  storage/ca-table/parse.cc \
  storage/ca-table/rle.c \
  storage/ca-table/schema.cc storage/ca-table/schema.h \
  storage/ca-table/table-backend-leveldb-table.cc \
  storage/ca-table/table-backend-writeonce.cc \
  storage/ca-table/table-write.cc \
  storage/ca-table/table.cc
storage_ca_table_libca_table_la_LIBADD = \
  -lleveldb

storage_ca_table_ca_shell_CPPFLAGS = \
  $(AM_CPPFLAGS) \
  $(JSONCPP_CFLAGS)
storage_ca_table_ca_shell_SOURCES = \
  storage/ca-table/ca-shell.cc \
  storage/ca-table/correlate.cc \
  storage/ca-table/select.cc \
  storage/ca-table/statement.cc \
  storage/ca-table/query.cc \
  storage/ca-table/query-parser.y \
  storage/ca-table/query-lexer.l
storage_ca_table_ca_shell_LDADD = \
  storage/ca-table/libca-table.la \
  third_party/capnproto/libcapnp-rpc.la \
  third_party/capnproto/libcapnp.la \
  base/libbase.la \
  $(JSONCPP_LIBS) \
  $(YAML_LIBS) \
  -lreadline

storage_ca_table_ca_load_SOURCES = storage/ca-table/ca-load.cc
storage_ca_table_ca_load_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la

storage_ca_table_ca_dump_SOURCES = storage/ca-table/ca-dump.cc
storage_ca_table_ca_dump_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la

storage_ca_table_format_benchmark_SOURCES = \
  storage/ca-table/format_benchmark.cc
storage_ca_table_format_benchmark_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la

TABLE_TESTS = \
  storage/ca-table/ca-load_test \
  storage/ca-table/format_test \
  storage/ca-table/table-backend-leveldb-table_test \
  storage/ca-table/table-backend-writeonce_test \
  storage/ca-table/tests/arena-00 \
  storage/ca-table/tests/crc32c-00

check_PROGRAMS += $(TABLE_TESTS)

storage_ca_table_table_backend_leveldb_table_test_SOURCES = \
  storage/ca-table/table-backend-leveldb-table_test.cc
storage_ca_table_table_backend_leveldb_table_test_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la \
  third_party/gtest/libgtest.a

storage_ca_table_table_backend_writeonce_test_SOURCES = \
  storage/ca-table/table-backend-writeonce_test.cc
storage_ca_table_table_backend_writeonce_test_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la \
  third_party/gtest/libgtest.a

storage_ca_table_ca_load_test_SOURCES = \
  storage/ca-table/ca-load_test.cc
storage_ca_table_ca_load_test_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la \
  third_party/gtest/libgtest.a

storage_ca_table_format_test_SOURCES = \
  storage/ca-table/format_test.cc
storage_ca_table_format_test_LDADD = \
  storage/ca-table/libca-table.la \
  base/libbase.la \
  third_party/gtest/libgtest.a

storage_ca_table_tests_arena_00_LDADD = storage/ca-table/libca-table.la base/libbase.la
storage_ca_table_tests_crc32c_00_LDADD = storage/ca-table/libca-table.la base/libbase.la

ANALYZE_SRC += \
  $(storage_ca_table_ca_dump_SOURCES) \
  $(storage_ca_table_ca_load_SOURCES) \
  $(storage_ca_table_ca_load_test_SOURCES) \
  $(storage_ca_table_format_benchmark_SOURCES) \
  $(storage_ca_table_format_test_SOURCES) \
  $(storage_ca_table_libca_table_la_SOURCES) \
  $(storage_ca_table_table_backend_leveldb_table_test_SOURCES) \
  $(storage_ca_table_table_backend_writeonce_test_SOURCES)
