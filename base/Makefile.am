noinst_LTLIBRARIES += \
  base/libbase.la \
  base/libcrypto.la
noinst_PROGRAMS += \
  base/concurrency-lock-bench \
  base/concurrency-queue-bench
check_PROGRAMS += \
  base/async_test \
  base/cat_test \
  base/columnfile_test \
  base/escape_test \
  base/file_test \
  base/macros_test \
  base/sha1_test \
  base/string_test \
  base/stringref_test

base_async_test_SOURCES = \
  base/async_test.cc
base_async_test_LDADD = \
  base/libbase.la \
  third_party/gtest/libgtest.a

base_cat_test_SOURCES = \
  base/cat_test.cc
base_cat_test_LDADD = \
  third_party/gtest/libgtest.a

base_columnfile_test_SOURCES = \
  base/columnfile_test.cc
base_columnfile_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libbase.la

base_concurrency_lock_bench_SOURCES = \
  base/concurrency-lock-bench.cc
base_concurrency_queue_bench_SOURCES = \
  base/concurrency-queue-bench.cc

base_escape_test_SOURCES = \
  base/escape_test.cc
base_escape_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libbase.la

base_macros_test_SOURCES = \
  base/macros_test.cc
base_macros_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libbase.la

base_string_test_SOURCES = \
  base/string_test.cc
base_string_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libbase.la

base_stringref_test_SOURCES = \
  base/stringref_test.cc
base_stringref_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libbase.la

base_file_test_SOURCES = \
  base/file_test.cc
base_file_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libbase.la

base_sha1_test_SOURCES = \
  base/sha1_test.cc
base_sha1_test_LDADD = \
  third_party/gtest/libgtest.a \
  base/libcrypto.la \
  base/libbase.la

base_libbase_la_SOURCES = \
  base/async.cc \
  base/base64.cc \
  base/columnfile.cc \
  base/error.cc \
  base/escape.cc \
  base/file.cc \
  base/hash.cc \
  base/io.cc \
  base/k-means.c \
  base/priority.cc \
  base/progress.cc \
  base/random.cc \
  base/socket.cc \
  base/string.cc \
  base/system.cc \
  base/topology.cc
base_libbase_la_LIBADD = \
  third_party/capnproto/libkj.la \
  -lsnappy -lhwloc

base_libcrypto_la_SOURCES = \
  base/sha1.cc
base_libcrypto_la_LIBADD = \
  $(CRYPTO_LIBS)

ANALYZE_SRC += \
  $(base_async_test_SOURCES) \
  $(base_cat_test_SOURCES) \
  $(base_columnfile_test_SOURCES) \
  $(base_escape_test_SOURCES) \
  $(base_file_test_SOURCES) \
  $(base_libbase_la_SOURCES) \
  $(base_libcrypto_la_SOURCES) \
  $(base_sha1_test_SOURCES) \
  $(base_string_test_SOURCES) \
  $(base_stringref_test_SOURCES)
