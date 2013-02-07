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
                       const struct ca_field *fields,
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

        case EXPR_FIELD:

            {
              llvm::Value *result_type, *field_iov, *field_ptr;
              unsigned int field_index, type;

              field_index = expr->value.d.field_index;
              type = fields[field_index].type;

              result_type = builder->CreateStructGEP (result, 0);
              field_iov = builder->CreateGEP (field_values, llvm::ConstantInt::get(t_int32, field_index));
              field_ptr = builder->CreateStructGEP (field_iov, 0);

              builder->CreateStore (llvm::ConstantInt::get (t_int32, type), result_type);

              switch (fields[field_index].type)
                {
                case CA_TEXT:

                    {
                      llvm::Value *result_text = builder->CreateStructGEP (result, 2);
                      llvm::Value *text = builder->CreateLoad (field_ptr);

                      builder->CreateStore (text,
                                            builder->CreatePointerCast (result_text, t_int64_pointer));
                    }

                  break;

                case CA_INT64:
                case CA_UINT64:

                    {
                      llvm::Value *result_int = builder->CreateStructGEP (result, 2);
                      llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);
                      llvm::Value *data_pointer = builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int64_pointer));

                      builder->CreateStore (data_pointer,
                                            builder->CreatePointerCast (result_int, t_int64_pointer));
                    }

                  break;

                case CA_BOOLEAN:

                    {
                      llvm::Value *result_int = builder->CreateStructGEP (result, 2);
                      llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);
                      llvm::Value *data_pointer = builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int8_pointer));
                      data_pointer = builder->CreateIntCast (data_pointer, t_int64, false);

                      builder->CreateStore (data_pointer,
                                            builder->CreatePointerCast (result_int, t_int64_pointer));

                    }

                  break;

                default:

                  ca_set_error ("subexpression_compile: Unhandled field value type %d", type);

                  return NULL;
                }
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
