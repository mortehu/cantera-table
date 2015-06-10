noinst_PROGRAMS += \
  third_party/capnproto/capnp \
  third_party/capnproto/capnpc-c++ \
  third_party/capnproto/capnpc-capnp

noinst_LTLIBRARIES += \
  third_party/capnproto/libcapnp-rpc.la \
  third_party/capnproto/libcapnp.la \
  third_party/capnproto/libcapnpc.la \
  third_party/capnproto/libkj.la

third_party_capnproto_libcapnpc_la_SOURCES = \
  third_party/capnproto/c++/src/capnp/compiler/md5.h \
  third_party/capnproto/c++/src/capnp/compiler/md5.c++ \
  third_party/capnproto/c++/src/capnp/compiler/error-reporter.h \
  third_party/capnproto/c++/src/capnp/compiler/error-reporter.c++ \
  third_party/capnproto/c++/src/capnp/compiler/lexer.capnp.h \
  third_party/capnproto/c++/src/capnp/compiler/lexer.capnp.c++ \
  third_party/capnproto/c++/src/capnp/compiler/lexer.h \
  third_party/capnproto/c++/src/capnp/compiler/lexer.c++ \
  third_party/capnproto/c++/src/capnp/compiler/grammar.capnp.h \
  third_party/capnproto/c++/src/capnp/compiler/grammar.capnp.c++ \
  third_party/capnproto/c++/src/capnp/compiler/parser.h \
  third_party/capnproto/c++/src/capnp/compiler/parser.c++ \
  third_party/capnproto/c++/src/capnp/compiler/node-translator.h \
  third_party/capnproto/c++/src/capnp/compiler/node-translator.c++ \
  third_party/capnproto/c++/src/capnp/compiler/compiler.h \
  third_party/capnproto/c++/src/capnp/compiler/compiler.c++ \
  third_party/capnproto/c++/src/capnp/schema-parser.c++

third_party_capnproto_capnp_SOURCES = \
  third_party/capnproto/c++/src/capnp/compiler/capnp.c++ \
  third_party/capnproto/c++/src/capnp/compiler/module-loader.c++
third_party_capnproto_capnp_LDADD = \
  third_party/capnproto/libcapnpc.la \
  third_party/capnproto/libcapnp.la \
  third_party/capnproto/libkj.la

third_party_capnproto_capnpc_capnp_SOURCES = \
  third_party/capnproto/c++/src/capnp/compiler/capnpc-capnp.c++
third_party_capnproto_capnpc_capnp_LDADD = \
  third_party/capnproto/libcapnp.la \
  third_party/capnproto/libkj.la

third_party_capnproto_capnpc_c___SOURCES = \
  third_party/capnproto/c++/src/capnp/compiler/capnpc-c++.c++
third_party_capnproto_capnpc_c___LDADD = \
  third_party/capnproto/libcapnp.la \
  third_party/capnproto/libkj.la

third_party_capnproto_libcapnp_la_SOURCES = \
  third_party/capnproto/c++/src/capnp/any.c++ \
  third_party/capnproto/c++/src/capnp/arena.c++ \
  third_party/capnproto/c++/src/capnp/blob.c++ \
  third_party/capnproto/c++/src/capnp/c++.capnp.c++ \
  third_party/capnproto/c++/src/capnp/dynamic.c++ \
  third_party/capnproto/c++/src/capnp/layout.c++ \
  third_party/capnproto/c++/src/capnp/list.c++ \
  third_party/capnproto/c++/src/capnp/message.c++ \
  third_party/capnproto/c++/src/capnp/schema-loader.c++ \
  third_party/capnproto/c++/src/capnp/schema.c++ \
  third_party/capnproto/c++/src/capnp/schema.capnp.c++ \
  third_party/capnproto/c++/src/capnp/serialize-packed.c++ \
  third_party/capnproto/c++/src/capnp/serialize.c++ \
  third_party/capnproto/c++/src/capnp/stringify.c++

third_party_capnproto_libcapnp_rpc_la_SOURCES = \
  third_party/capnproto/c++/src/capnp/serialize-async.c++ \
  third_party/capnproto/c++/src/capnp/capability.c++ \
  third_party/capnproto/c++/src/capnp/dynamic-capability.c++ \
  third_party/capnproto/c++/src/capnp/rpc.c++ \
  third_party/capnproto/c++/src/capnp/rpc.capnp.c++ \
  third_party/capnproto/c++/src/capnp/rpc-twoparty.c++ \
  third_party/capnproto/c++/src/capnp/rpc-twoparty.capnp.c++ \
  third_party/capnproto/c++/src/capnp/persistent.capnp.c++ \
  third_party/capnproto/c++/src/capnp/ez-rpc.c++

third_party_capnproto_libkj_la_SOURCES = \
  third_party/capnproto/c++/src/kj/arena.c++ \
  third_party/capnproto/c++/src/kj/array.c++ \
  third_party/capnproto/c++/src/kj/async-io.c++ \
  third_party/capnproto/c++/src/kj/async-unix.c++ \
  third_party/capnproto/c++/src/kj/async.c++ \
  third_party/capnproto/c++/src/kj/common.c++ \
  third_party/capnproto/c++/src/kj/debug.c++ \
  third_party/capnproto/c++/src/kj/exception.c++ \
  third_party/capnproto/c++/src/kj/io.c++ \
  third_party/capnproto/c++/src/kj/main.c++ \
  third_party/capnproto/c++/src/kj/memory.c++ \
  third_party/capnproto/c++/src/kj/mutex.c++ \
  third_party/capnproto/c++/src/kj/parse/char.c++ \
  third_party/capnproto/c++/src/kj/refcount.c++ \
  third_party/capnproto/c++/src/kj/string-tree.c++ \
  third_party/capnproto/c++/src/kj/string.c++ \
  third_party/capnproto/c++/src/kj/thread.c++ \
  third_party/capnproto/c++/src/kj/time.c++ \
  third_party/capnproto/c++/src/kj/units.c++
