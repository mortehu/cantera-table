BUILT_SOURCES = query-lexer.c query-parser.c

AM_YFLAGS = -d

lib_LTLIBRARIES = libca-table.la
include_HEADERS = ca-table.h

bin_PROGRAMS = \
  ca-shell \
  ca-sort \
  ts-extract \
  ts-load \
  ts-merge

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -I/usr/lib/llvm-3.2/include -I.
AM_CFLAGS = -Wall -Werror -g
AM_CXXFLAGS = -std=c++0x -Wall -Werror -g

libca_table_la_SOURCES = \
  arena.c arena.h \
  crc32.c crc32.h \
  data.c \
  error.c error.h \
  expression-llvm.cc \
  fifo.c \
  io.c io.h \
  hashmap.c hashmap.h \
  merge.c \
  memory.c memory.h \
  schema.c schema.h \
  select.c \
  sort.c \
  table-backend-writeonce.c \
  table-write.c \
  table.c
libca_table_la_LDFLAGS = `llvm-config-3.2 --ldflags --libs core jit native`

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

TESTS = tests/fifo-00
check_PROGRAMS = tests/fifo-00

tests_fifo_00_SOURCES = tests/fifo-00.c
tests_fifo_00_LDADD = libca-table.la
