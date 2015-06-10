EXTRA_LTLIBRARIES =
bin_PROGRAMS =
bin_SCRIPTS =
check_PROGRAMS =
lib_LTLIBRARIES =
man1_MANS =
nobase_dist_pkgdata_DATA =
nobase_lib_LTLIBRARIES =
noinst_LIBRARIES =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
noinst_SCRIPTS =

EXTRA_INSTALL_TARGETS =

AM_TESTS_ENVIRONMENT = CPUPROFILE=; export CPUPROFILE; PYTHONPATH=$(srcdir)/generated/python:$(srcdir)/python/.libs; export PYTHONPATH; DATAROOTDIR=$(srcdir); export DATAROOTDIR;

BUILT_SOURCES =

TESTSCRIPTS =

ANALYZE_SRC =
ANALYZE_CXX_SRC = $(filter %.cc, $(ANALYZE_SRC))
ANALYZE_PLIST = $(ANALYZE_CXX_SRC:%.cc=%.plist)

TESTS = $(check_PROGRAMS) $(TESTSCRIPTS)

AM_CPPFLAGS = -I$(srcdir) -Igenerated -Ithird_party/capnproto/c++/src
AM_CFLAGS = -g -Wall -march=native
AM_CXXFLAGS = -g -Wall -march=native
AM_LDFLAGS = -lpthread

ACLOCAL_AMFLAGS = -I m4

include $(srcdir)/base/Makefile.am
include $(srcdir)/storage/ca-table/Makefile.am
include $(srcdir)/third_party/capnproto/Makefile.am
include $(srcdir)/third_party/gtest/Makefile.am

install-exec-local: $(EXTRA_INSTALL_TARGETS)

$(ANALYZE_PLIST): %.plist: %.cc
	@echo " CXXSA  " $@
	@$(CXXCOMPILE) -Wall -Wextra --analyze $< $(OUTPUT_OPTION)

analyze: $(ANALYZE_PLIST)

.PHONY: analyze
