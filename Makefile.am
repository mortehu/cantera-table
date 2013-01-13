noinst_LIBRARIES = libcommon.a

bin_PROGRAMS = ts-load ts-compact

AM_CPPFLAGS = -D_GNU_SOURCE -Wall

libcommon_a_SOURCES = journal.c journal.h array.c array.h

ts_load_SOURCES = ts-load.c
ts_load_LDADD = libcommon.a

ts_compact_SOURCES = ts-compact.c
ts_compact_LDADD = libcommon.a
