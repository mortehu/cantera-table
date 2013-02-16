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
#define CASE_INTEGER_TYPE \
  case CA_INT8: \
  case CA_UINT8: \
  case CA_INT16: \
  case CA_UINT16: \
  case CA_INT32: \
  case CA_UINT32: \
  case CA_INT64: \
  case CA_UINT64: \
  case CA_TIMESTAMPTZ:

  llvm::Value *
  context::cast_compile (llvm::Value *input,
                         enum ca_type input_type,
                         enum ca_type output_type)
  {
    LLVM_TYPE *llvm_output_type;
    std::map<cast_signature, llvm::Function *>::iterator i;

    if (input_type == output_type)
      return input;

    llvm_output_type = llvm_type_for_ca_type (output_type);

    switch (output_type)
      {
      CASE_INTEGER_TYPE

        switch (input_type)
          {
          CASE_INTEGER_TYPE

            return builder->CreateIntCast (input, llvm_output_type, false);

          default:;
          }

        break;

        break;

      default:;
      }

    if (casts.end() != (i = casts.find (cast_signature (output_type, input_type))))
      return builder->CreateCall (i->second, input);

    ca_set_error ("Unsupported cast from %s to %s",
                  ca_type_to_string (input_type),
                  ca_type_to_string (output_type));

    return NULL;
  }

  llvm::Value *
  context::subexpression_compile (struct expression *expr,
                                  enum ca_type *return_type)
  {
    enum ca_type lhs_type, rhs_type;
    llvm::Value *lhs = NULL, *rhs = NULL;

    *return_type = CA_INVALID; /* Significes "any type" */

    switch (expr->type)
      {
      case EXPR_ADD:
      case EXPR_AND:
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

        rhs = builder->CreateAlloca (t_expression_value);

        if (!(rhs = subexpression_compile (expr->rhs, &rhs_type)))
          return NULL;

        /* Fall through */

      case EXPR_CAST:

        lhs = builder->CreateAlloca (t_expression_value);

        if (!(lhs = subexpression_compile (expr->lhs, &lhs_type)))
          return NULL;

        break;

      default:

        ;
      }

    switch (expr->type)
      {
      case EXPR_CAST:

        *return_type = expr->value.type;

        return cast_compile (lhs, lhs_type, *return_type);

      case EXPR_CONSTANT:

        *return_type = expr->value.type;

        switch (expr->value.type)
          {
          case CA_INVALID:

            assert (!"Got constant of type CA_INVALID");

            return NULL;

          case CA_VOID:

            assert (!"Got constant of type CA_VOID");

            return NULL;

          case CA_BOOLEAN:

            return llvm::ConstantInt::get (t_int1, expr->value.d.integer);

          case CA_TIME_FLOAT4:
          case CA_OFFSET_SCORE:

              {
                llvm::Value *result, *base, *length;

                result = builder->CreateAlloca (t_iovec);
                base = builder->CreateStructGEP (result, 0);
                length = builder->CreateStructGEP (result, 1);

                builder->CreateStore (llvm::ConstantInt::get (t_pointer, (uintptr_t) expr->value.d.iov.iov_base),
                                      base);

                builder->CreateStore (llvm::ConstantInt::get (t_size, expr->value.d.iov.iov_len),
                                      length);

                return result;
              }

            break;

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
          }

        break;

      case EXPR_FIELD:

          {
            llvm::Value *field_iov, *field_ptr;
            unsigned int field_index;

            field_index = expr->value.d.field_index;

            field_iov = builder->CreateGEP (field_values, llvm::ConstantInt::get(t_int32, field_index));
            field_ptr = builder->CreateStructGEP (field_iov, 0);

            *return_type = fields[field_index].type;

            switch (fields[field_index].type)
              {
              case CA_INVALID:

                assert (!"Got field of type CA_INVALID");

                return NULL;

              case CA_TIME_FLOAT4:
              case CA_OFFSET_SCORE:

                return field_iov;

              case CA_NUMERIC:
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

              case CA_FLOAT4:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_float_pointer));
                  }

                break;

              case CA_FLOAT8:

                  {
                    llvm::Value *data_pointer_pointer = builder->CreateLoad (field_ptr);

                    return builder->CreateLoad (builder->CreateIntToPtr (data_pointer_pointer, t_double_pointer));
                  }

                break;

              case CA_VOID:

                /* This is certainly a weird case, but we need this here to
                 * silence compiler warnings */

                return NULL;
              }
          }

        break;

      case EXPR_EQUAL:
      case EXPR_GREATER_EQUAL:
      case EXPR_GREATER_THAN:
      case EXPR_LESS_EQUAL:
      case EXPR_LESS_THAN:
      case EXPR_NOT_EQUAL:

        if (lhs_type != rhs_type)
          {
            ca_set_error ("Arguments to the equality operator must be of equal types");

            return NULL;
          }

        *return_type = CA_BOOLEAN;

        switch (lhs_type)
          {
          case CA_TEXT:

              {
                llvm::Value *cmp, *zero;

                cmp = builder->CreateCall2 (f_strcmp, lhs, rhs);
                zero = llvm::ConstantInt::get (t_int32, 0);

                switch (expr->type)
                  {
                  case EXPR_EQUAL:         return builder->CreateICmpEQ (cmp, zero);
                  case EXPR_GREATER_EQUAL: return builder->CreateICmpSGE (cmp, zero);
                  case EXPR_GREATER_THAN:  return builder->CreateICmpSGT (cmp, zero);
                  case EXPR_LESS_EQUAL:    return builder->CreateICmpSLE (cmp, zero);
                  case EXPR_LESS_THAN:     return builder->CreateICmpSLT (cmp, zero);
                  case EXPR_NOT_EQUAL:     return builder->CreateICmpNE (cmp, zero);
                  default: assert (!"bug: missing case");
                  }
              }

          case CA_INT8:
          case CA_INT16:
          case CA_INT32:
          case CA_INT64:

            switch (expr->type)
              {
              case EXPR_EQUAL:         return builder->CreateICmpEQ (lhs, rhs);
              case EXPR_GREATER_EQUAL: return builder->CreateICmpSGE (lhs, rhs);
              case EXPR_GREATER_THAN:  return builder->CreateICmpSGT (lhs, rhs);
              case EXPR_LESS_EQUAL:    return builder->CreateICmpSLE (lhs, rhs);
              case EXPR_LESS_THAN:     return builder->CreateICmpSLT (lhs, rhs);
              case EXPR_NOT_EQUAL:     return builder->CreateICmpNE (lhs, rhs);
              default: assert (!"bug: missing case");
              }

          case CA_UINT8:
          case CA_UINT16:
          case CA_UINT32:
          case CA_UINT64:

            switch (expr->type)
              {
              case EXPR_EQUAL:         return builder->CreateICmpEQ (lhs, rhs);
              case EXPR_GREATER_EQUAL: return builder->CreateICmpUGE (lhs, rhs);
              case EXPR_GREATER_THAN:  return builder->CreateICmpUGT (lhs, rhs);
              case EXPR_LESS_EQUAL:    return builder->CreateICmpULE (lhs, rhs);
              case EXPR_LESS_THAN:     return builder->CreateICmpULT (lhs, rhs);
              case EXPR_NOT_EQUAL:     return builder->CreateICmpNE (lhs, rhs);
              default: assert (!"bug: missing case");
              }

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

      case EXPR_FUNCTION_CALL:

          {
            function_signature signature;
            std::string pretty_signature;

            std::vector<llvm::Value *> arguments;
            struct expression *arg;

            /* Build signature */

            signature.first = expr->value.d.identifier;
            pretty_signature = signature.first + '(';

            for (arg = expr->lhs; arg; arg = arg->next)
              {
                llvm::Value *arg_value;
                enum ca_type arg_type;

                if (!(arg_value = subexpression_compile (arg, &arg_type)))
                  return NULL;

                signature.second.push_back (arg_type);

                arguments.push_back (arg_value);

                if (arg != expr->lhs)
                  pretty_signature += ", ";
                pretty_signature += ca_type_to_string (arg_type);
              }

            pretty_signature += ')';

            /* Search for function */

            auto i = functions.find (signature);

            if (i != functions.end ())
              {
                *return_type = i->second.return_type;

                return builder->CreateCall (i->second.handle, arguments);
              }

            ca_set_error ("Function %s not found", pretty_signature.c_str ());

            return NULL;
          }

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
