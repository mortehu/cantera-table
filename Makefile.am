lib_LTLIBRARIES = libsmalltable.la
include_HEADERS = smalltable.h

bin_PROGRAMS = \
  ts-correlate \
  ts-extract \
  ts-load \
  ts-merge \
  ts-sort

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -D_GNU_SOURCE -Wall
AM_CFLAGS = -Wall -O3 -g

libsmalltable_la_SOURCES = \
  crc32.c crc32.h \
  io.c io.h \
  memory.c memory.h \
  smalltable.c smalltable.h

ts_correlate_SOURCES = ts-correlate.c
ts_correlate_LDFLAGS = -lm
ts_correlate_LDADD = libsmalltable.la

ts_extract_SOURCES = ts-extract.c
ts_extract_LDADD = libsmalltable.la

ts_load_SOURCES = ts-load.c
ts_load_LDADD = libsmalltable.la

ts_merge_SOURCES = ts-merge.c
ts_merge_LDADD = libsmalltable.la

ts_sort_SOURCES = ts-sort.c
ts_sort_LDADD = libsmalltable.la
