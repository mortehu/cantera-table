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

namespace ca_llvm
{
  llvm::Value *
  subexpression_compile (llvm::IRBuilder<> *builder, llvm::Module *module,
                        struct expression *expr,
                        const struct ca_field *fields,
                        llvm::Value *result,
                        llvm::Value *arena,
                        llvm::Value *field_values,
                        enum ca_type *return_type)
  {
    enum ca_type lhs_type, rhs_type;
    llvm::Value *lhs = NULL, *rhs = NULL;

    *return_type = CA_INVALID; /* Significes "any type" */

    switch (expr->type)
      {
      case EXPR_ADD:
      case EXPR_AND:
      case EXPR_CAST:
      case EXPR_DIV:
      case EXPR_EQUAL:
      case EXPR_GREATER_EQUAL:
      case EXPR_GREATER_THAN:
      case EXPR_LESS_EQUAL:
      case EXPR_LESS_THAN:
      case EXPR_LIKE:
      case EXPR_NOT_LIKE:
      case EXPR_MUL:
      case EXPR_NOT_EQUAL:
      case EXPR_OR:
      case EXPR_SUB:

        lhs = builder->CreateAlloca (t_expression_value);
        rhs = builder->CreateAlloca (t_expression_value);

        if (!(subexpression_compile (builder, module, expr->lhs,
                                     fields, lhs, arena,
                                     field_values,
                                     &lhs_type)))
          return NULL;

        if (!(subexpression_compile (builder, module, expr->rhs,
                                     fields, rhs, arena,
                                     field_values,
                                     &rhs_type)))
          return NULL;

        break;

      default:

        ;
      }

    switch (expr->type)
      {
      case EXPR_CONSTANT:

          {
            llvm::Value *type = builder->CreateStructGEP (result, 0);
            llvm::Value *value0 = builder->CreateStructGEP (result, 1);

            builder->CreateStore (llvm::ConstantInt::get (t_int32, expr->value.type), type);

            switch (expr->value.type)
              {
              case CA_BOOLEAN:
              case CA_INT8:
              case CA_UINT8:
              case CA_INT16:
              case CA_UINT16:
              case CA_INT32:
              case CA_UINT32:
              case CA_INT64:
              case CA_UINT64:
              case CA_TIMESTAMPTZ:

                builder->CreateStore (llvm::ConstantInt::get (t_int64, expr->value.d.integer),
                                      value0);

                break;

              case CA_FLOAT4:

                builder->CreateStore (llvm::ConstantFP::get (t_float, expr->value.d.float4),
                                      builder->CreateBitCast (value0, t_float));

                break;

              case CA_FLOAT8:

                builder->CreateStore (llvm::ConstantFP::get (t_double, expr->value.d.float8),
                                      builder->CreateBitCast (value0, t_double));

                break;

              case CA_TEXT:
              case CA_NUMERIC:

                builder->CreateStore (llvm::ConstantInt::get (t_pointer, (ptrdiff_t) expr->value.d.string_literal), value0);

                break;

              default:

                ca_set_error ("subexpression_compile: Unhandled constant value type %d", expr->value.type);

                return NULL;
              }

            *return_type = expr->value.type;
          }

        break;

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
              case CA_TIME_FLOAT4:

                  {
                    llvm::Value *field_length;

                    field_length = builder->CreateStructGEP (field_iov, 1);

                    llvm::Value *result_pointer = builder->CreateStructGEP (result, 1);
                    llvm::Value *result_length = builder->CreateStructGEP (result, 2);
                    llvm::Value *pointer = builder->CreateLoad (field_ptr);
                    llvm::Value *length = builder->CreateLoad (field_length);

                    builder->CreateStore (pointer, result_pointer);
                    builder->CreateStore (length, result_length);
                  }

                break;

              case CA_TEXT:

                  {
                    llvm::Value *result_text = builder->CreateStructGEP (result, 1);
                    llvm::Value *text = builder->CreateLoad (field_ptr);

                    builder->CreateStore (text, result_text);
                  }

                break;

              case CA_INT16:
              case CA_UINT16:

                  {
                    llvm::Value *result_int = builder->CreateStructGEP (result, 1);
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);
                    llvm::Value *data_pointer = builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int16_pointer));

                    builder->CreateStore (data_pointer, result_int);
                  }

                break;

              case CA_INT32:
              case CA_UINT32:

                  {
                    llvm::Value *result_int = builder->CreateStructGEP (result, 1);
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);
                    llvm::Value *data_pointer = builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int32_pointer));

                    builder->CreateStore (data_pointer, result_int);
                  }

                break;

              case CA_INT64:
              case CA_UINT64:
              case CA_TIMESTAMPTZ:

                  {
                    llvm::Value *result_int = builder->CreateStructGEP (result, 1);
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);
                    llvm::Value *data_pointer = builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int64_pointer));

                    builder->CreateStore (data_pointer, result_int);
                  }

                break;

              case CA_BOOLEAN:

                  {
                    llvm::Value *result_int = builder->CreateStructGEP (result, 1);
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);
                    llvm::Value *data_pointer = builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int8_pointer));
                    data_pointer = builder->CreateIntCast (data_pointer, t_int64, false);

                    builder->CreateStore (data_pointer, result_int);
                  }

                break;

              default:

                ca_set_error ("subexpression_compile: Unhandled field value type %d", type);

                return NULL;
              }

            *return_type = fields[field_index].type;
          }

        break;

      case EXPR_EQUAL:

        if (lhs_type != rhs_type)
          {
            ca_set_error ("Arguments to the equality operator must be of equal types");

            return NULL;
          }

        builder->CreateCall3 (f_ca_compare_equal, result, lhs, rhs);
        *return_type = CA_BOOLEAN;

        break;

      case EXPR_LIKE:

        if (lhs_type != CA_TEXT || rhs_type != CA_TEXT)
          {
            ca_set_error ("Arguments to LIKE must be of type TEXT");

            return NULL;
          }

        builder->CreateCall3 (f_ca_compare_like, result, lhs, rhs);
        *return_type = CA_BOOLEAN;

        break;

      case EXPR_AND:

          {
            llvm::Value *result_int = builder->CreateStructGEP (result, 1);

            if (lhs_type != CA_BOOLEAN || rhs_type != CA_BOOLEAN)
              {
                ca_set_error ("Arguments to OR must be of type BOOLEAN");

                return NULL;
              }

            builder->CreateStore (llvm::ConstantInt::get (t_int32, CA_BOOLEAN), builder->CreateStructGEP (result, 0));
            builder->CreateStore (builder->CreateAnd (builder->CreateLoad (builder->CreateStructGEP (lhs, 1)),
                                                      builder->CreateLoad (builder->CreateStructGEP (rhs, 1))),
                                  result_int);
          }

        break;

      case EXPR_OR:

          {
            llvm::Value *result_int = builder->CreateStructGEP (result, 1);

            if (lhs_type != CA_BOOLEAN || rhs_type != CA_BOOLEAN)
              {
                ca_set_error ("Arguments to OR must be of type BOOLEAN");

                return NULL;
              }

            builder->CreateStore (llvm::ConstantInt::get (t_int32, CA_BOOLEAN), builder->CreateStructGEP (result, 0));
            builder->CreateStore (builder->CreateOr (builder->CreateLoad (builder->CreateStructGEP (lhs, 1)),
                                                     builder->CreateLoad (builder->CreateStructGEP (rhs, 1))),
                                  result_int);

          }

        break;

      default:

        ca_set_error ("Expression type %d not supported", expr->type);

        return NULL;
      }

    return llvm::ConstantInt::get (t_int32, 0);
  }
} /* namespace ca_llvm */
