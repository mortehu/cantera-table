#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <llvm/DerivedTypes.h>
#include <llvm/LLVMContext.h>

#if HAVE_LLVM_SUPPORT_IRBUILDER_H
#  include <llvm/Support/IRBuilder.h>
#elif HAVE_LLVM_IRBUILDER_H
#  include <llvm/IRBuilder.h>
#endif

#define UINT64_ALIGNMENT 8 /* XXX: Not on ia32 */

#include "ca-table.h"
#include "ca-llvm.h"
#include "query.h"

namespace ca_llvm {

llvm::Value *
subexpression_compile (llvm::IRBuilder<> *builder, llvm::Module *module,
                       struct expression *expr,
                       llvm::Value *result,
                       llvm::Value *arena,
                       llvm::Value *field_values)
{
  for (;;)
    {
      switch (expr->type)
        {
        case EXPR_CONSTANT:

            {
              llvm::Value *constant;

              constant = builder->CreateIntToPtr(llvm::ConstantInt::get (t_pointer, (ptrdiff_t) &expr->value),
                                                 t_int8_pointer);

              builder->CreateMemCpy (result,
                                     constant,
                                     sizeof (expr->value), UINT64_ALIGNMENT);
            }

          return llvm::ConstantInt::get (t_int32, 0);

        case EXPR_PARENTHESIS:

          expr = expr->lhs;

          continue;

        default:

          ca_set_error ("Expression type %d not supported", expr->type);

          return NULL;
        }
    }
}

} /* namespace ca_llvm */
