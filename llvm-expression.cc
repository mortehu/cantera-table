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

        if (!(lhs = subexpression_compile (builder, module, expr->lhs,
                                           fields, arena,
                                           field_values,
                                           &lhs_type)))
          return NULL;

        if (!(rhs = subexpression_compile (builder, module, expr->rhs,
                                           fields, arena,
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

        *return_type = expr->value.type;

        switch (expr->value.type)
          {
          case CA_BOOLEAN:

            return llvm::ConstantInt::get (t_int1, expr->value.d.integer);

          case CA_INT8:
          case CA_UINT8:
          case CA_INT16:
          case CA_UINT16:
          case CA_INT32:
          case CA_UINT32:
          case CA_INT64:
          case CA_UINT64:
          case CA_TIMESTAMPTZ:

            return llvm::ConstantInt::get (t_int64, expr->value.d.integer);

          case CA_FLOAT4:

            return llvm::ConstantFP::get (t_float, expr->value.d.float4);

          case CA_FLOAT8:

            return llvm::ConstantFP::get (t_double, expr->value.d.float8);

          case CA_TEXT:
          case CA_NUMERIC:

            return llvm::ConstantInt::get (t_pointer, (ptrdiff_t) expr->value.d.string_literal);

          default:

            ca_set_error ("subexpression_compile: Unhandled constant value type %d", expr->value.type);

            return NULL;
          }

        break;

      case EXPR_FIELD:

          {
            llvm::Value *field_iov, *field_ptr;
            unsigned int field_index, type;

            field_index = expr->value.d.field_index;
            type = fields[field_index].type;

            field_iov = builder->CreateGEP (field_values, llvm::ConstantInt::get(t_int32, field_index));
            field_ptr = builder->CreateStructGEP (field_iov, 0);

            *return_type = fields[field_index].type;

            switch (fields[field_index].type)
              {
              case CA_TIME_FLOAT4:

                return field_iov;

              case CA_TEXT:

                return builder->CreateLoad (field_ptr);

              case CA_BOOLEAN:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateIntCast (builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int8_pointer)), t_int1, false);
                  }

              case CA_INT8:
              case CA_UINT8:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int8_pointer));
                  }

                break;

              case CA_INT16:
              case CA_UINT16:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int16_pointer));
                  }

                break;

              case CA_INT32:
              case CA_UINT32:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int32_pointer));
                  }

                break;

              case CA_INT64:
              case CA_UINT64:
              case CA_TIMESTAMPTZ:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_int64_pointer));
                  }

                break;

              default:

                ca_set_error ("subexpression_compile: Unhandled field value type %d", type);

                return NULL;
              }
          }

        break;

      case EXPR_EQUAL:

        if (lhs_type != rhs_type)
          {
            ca_set_error ("Arguments to the equality operator must be of equal types");

            return NULL;
          }

        *return_type = CA_BOOLEAN;

        switch (lhs_type)
          {
          case CA_TEXT:

            return builder->CreateICmpEQ (llvm::ConstantInt::get (t_int32, 0),
                                          builder->CreateCall2 (f_strcmp, lhs, rhs));

          case CA_INT8:
          case CA_UINT8:
          case CA_INT16:
          case CA_UINT16:
          case CA_INT32:
          case CA_UINT32:
          case CA_INT64:
          case CA_UINT64:

            return builder->CreateICmpEQ (lhs, rhs);

          default:

            ca_set_error ("Don't know how to compare variables of type %u", lhs_type);

            return NULL;
          }

      case EXPR_LIKE:

        if (lhs_type != CA_TEXT || rhs_type != CA_TEXT)
          {
            ca_set_error ("Arguments to LIKE must be of type TEXT");

            return NULL;
          }

        *return_type = CA_BOOLEAN;

        return builder->CreateCall2 (f_ca_compare_like, lhs, rhs);

      case EXPR_AND:

        if (lhs_type != CA_BOOLEAN || rhs_type != CA_BOOLEAN)
          {
            ca_set_error ("Arguments to OR must be of type BOOLEAN");

            return NULL;
          }

        *return_type = CA_BOOLEAN;

        return builder->CreateAnd (lhs, rhs);

      case EXPR_OR:

        if (lhs_type != CA_BOOLEAN || rhs_type != CA_BOOLEAN)
          {
            ca_set_error ("Arguments to OR must be of type BOOLEAN");

            return NULL;
          }

        *return_type = CA_BOOLEAN;

        return builder->CreateOr (lhs, rhs);

      default:

        ca_set_error ("Expression type %d not supported", expr->type);

        return NULL;
      }

    return NULL;
  }
} /* namespace ca_llvm */
