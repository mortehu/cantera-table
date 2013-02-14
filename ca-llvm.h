#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <llvm/Function.h>

#include "query.h"

#if (LLVM_VERSION_MAJOR > 3) || (LLVM_VERSION_MINOR >= 2)
#  define LLVM_TYPE llvm::Type
#else
#  define LLVM_TYPE const llvm::Type
#endif

namespace ca_llvm
{
  extern llvm::Function *f_ca_compare_like;
  extern llvm::Function *f_strcmp;

  extern LLVM_TYPE *t_int1;
  extern LLVM_TYPE *t_int8;
  extern LLVM_TYPE *t_int8_pointer;
  extern LLVM_TYPE *t_int16;
  extern LLVM_TYPE *t_int16_pointer;
  extern LLVM_TYPE *t_int32;
  extern LLVM_TYPE *t_int32_pointer;
  extern LLVM_TYPE *t_int64;
  extern LLVM_TYPE *t_int64_pointer;

  extern LLVM_TYPE *t_pointer;
  extern LLVM_TYPE *t_size;

  extern LLVM_TYPE *t_float;
  extern LLVM_TYPE *t_double;

  /* t_int32    header
   * t_int8[4]  padding
   * t_int8[8]  data0
   * t_int8[8]  data1 */
  extern LLVM_TYPE *t_expression_value;
  extern LLVM_TYPE *t_expression_value_pointer;

  /* t_pointer
   * t_size */
  extern LLVM_TYPE *t_iovec;

  struct context
    {
      llvm::IRBuilder<> *builder;
      llvm::Module *module;
      llvm::Value *arena;
      llvm::Value *field_values;
      const struct ca_field *fields;

      llvm::Value *
      subexpression_compile (struct expression *expr, enum ca_type *return_type);
    };
};
