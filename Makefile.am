BUILT_SOURCES = query-lexer.c query-parser.c

AM_YFLAGS = -d

lib_LTLIBRARIES = libca-table.la
include_HEADERS = ca-table.h

bin_PROGRAMS = \
  ca-shell \
  ts-correlate \
  ts-extract \
  ts-load \
  ts-merge \
  ts-sort

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -D_GNU_SOURCE
AM_CFLAGS = -Wall -O3 -g

libca_table_la_SOURCES = \
  arena.c arena.h \
  crc32.c crc32.h \
  data.c \
  error.c error.h \
  io.c io.h \
  memory.c memory.h \
  schema.c schema.h \
  select.c \
  smalltable.c \
  table-write.c

ca_shell_SOURCES = ca-shell.c query-parser.y query-lexer.l
ca_shell_LDFLAGS = -lreadline
ca_shell_LDADD = libca-table.la

ts_correlate_SOURCES = ts-correlate.c
ts_correlate_LDFLAGS = -lm
ts_correlate_LDADD = libca-table.la

ts_extract_SOURCES = ts-extract.c
ts_extract_LDADD = libca-table.la

ts_load_SOURCES = ts-load.c
ts_load_LDADD = libca-table.la

ts_merge_SOURCES = ts-merge.c
ts_merge_LDADD = libca-table.la

ts_sort_SOURCES = ts-sort.c
ts_sort_LDADD = libca-table.la
