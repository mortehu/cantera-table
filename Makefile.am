BUILT_SOURCES = query-lexer.c query-parser.c
man1_MANS = doc/ca-shell.1 doc/ca-sql.1

AM_YFLAGS = -d

lib_LTLIBRARIES = libca-table.la
include_HEADERS = ca-table.h ca-functions.h

bin_PROGRAMS = \
  ca-shell \
  ca-sort \
  ca-table-lockd \
  ts-extract \
  ts-load \
  ts-merge

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -I.
AM_CFLAGS = -Wall -Werror -g
AM_CXXFLAGS = -Wall -Werror -g

libca_table_la_SOURCES = \
  arena.c arena.h \
  cast.c \
  crc32c.c crc32c.h \
  error.c error.h \
  fifo.c \
  file-buffer.c \
  format.c \
  functions.c \
  hashmap.c hashmap.h \
  llvm-expression.cc \
  llvm-vm.cc \
  lock.c \
  memory.c memory.h \
  merge.c \
  output.c \
  parse.c \
  query.c \
  schema.c schema.h \
  select.c \
  table-sort.c \
  sort.cc \
  statement.c \
  table-backend-flexi.c \
  table-backend-log.c \
  table-backend-writeonce.c \
  table-write.c \
  table.c \
  type.c \
  update.c
libca_table_la_LDFLAGS = $(LLVM_LDFLAGS) $(LLVM_LIBS) -pthread

ca_table_lockd_SOURCES = lockd.c

ca_shell_SOURCES = ca-shell.c query-parser.y query-lexer.l
ca_shell_LDFLAGS = -lreadline
ca_shell_LDADD = libca-table.la

ca_sort_SOURCES = ca-sort.c
ca_sort_LDADD = libca-table.la

ts_extract_SOURCES = ts-extract.c
ts_extract_LDADD = libca-table.la

ts_load_SOURCES = ts-load.c
ts_load_LDADD = libca-table.la

ts_merge_SOURCES = ts-merge.c
ts_merge_LDADD = libca-table.la

TESTS = \
  tests/arena-00 \
  tests/correlation-00 \
  tests/crc32c-00 \
  tests/fifo-00 \
  tests/flexi-sort-00 \
  tests/hashmap-00 \
  tests/log-sort-00 \
  tests/rank-correlation-00 \
  tests/schema-00 \
  tests/wo-sort-00
check_PROGRAMS = $(TESTS)

tests_arena_00_LDADD = libca-table.la
tests_correlation_00_LDADD = libca-table.la
tests_crc32c_00_LDADD = libca-table.la
tests_fifo_00_LDADD = libca-table.la
tests_flexi_sort_00_LDADD = libca-table.la
tests_hashmap_00_LDADD = libca-table.la
tests_log_sort_00_LDADD = libca-table.la
tests_rank_correlation_00_LDADD = libca-table.la
tests_schema_00_LDADD = libca-table.la
tests_wo_sort_00_LDADD = libca-table.la
