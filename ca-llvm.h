#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <llvm/ExecutionEngine/ExecutionEngine.h>
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
  extern LLVM_TYPE *t_float_pointer;
  extern LLVM_TYPE *t_double;
  extern LLVM_TYPE *t_double_pointer;

  /* t_int32    header
   * t_int8[4]  padding
   * t_int8[8]  data0
   * t_int8[8]  data1 */
  extern LLVM_TYPE *t_expression_value;
  extern LLVM_TYPE *t_expression_value_pointer;

  /* t_pointer
   * t_size */
  extern LLVM_TYPE *t_iovec;
  extern LLVM_TYPE *t_iovec_pointer;

  typedef std::pair<std::string, std::vector<enum ca_type>> function_signature;
  typedef std::pair<enum ca_type, enum ca_type> cast_signature;

  struct function
    {
      enum ca_type return_type;
      llvm::Function *handle;
    };

  extern std::map<function_signature, function> functions;
  extern std::map<cast_signature, llvm::Function *> casts;

  LLVM_TYPE *
  llvm_type_for_ca_type (enum ca_type type);

  llvm::Function *
  register_function (const char *name, void *pointer,
                     enum ca_type return_type, ...);

  struct context
    {
      llvm::ExecutionEngine *engine;
      llvm::IRBuilder<> *builder;
      llvm::Module *module;
      llvm::Value *context;
      llvm::Value *field_values;
      const struct ca_field *fields;

      void
      function_init (void);

      llvm::Value *
      cast_compile (llvm::Value *input, enum ca_type input_type,
                    enum ca_type output_type);

      llvm::Value *
      subexpression_compile (struct expression *expr, enum ca_type *return_type);
    };
};
