lib_LTLIBRARIES = libsmalltable.la
include_HEADERS = smalltable.h

bin_PROGRAMS = ts-load ts-compact

ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = -D_GNU_SOURCE -Wall

libsmalltable_la_SOURCES = \
  crc32.c crc32.h \
  io.c io.h \
  memory.c memory.h \
  smalltable.c smalltable.h

ts_load_SOURCES = ts-load.c
ts_load_LDADD = libsmalltable.la

ts_compact_SOURCES = ts-compact.c
ts_compact_LDADD = libsmalltable.la
