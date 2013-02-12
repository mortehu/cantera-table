#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/Verifier.h>
#include <llvm/DerivedTypes.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JIT.h>
#include <llvm/LLVMContext.h>
#include <llvm/Module.h>
#include <llvm/PassManager.h>
#include <llvm/Transforms/Scalar.h>

#if HAVE_LLVM_SUPPORT_TARGETSELECT_H
#  include <llvm/Support/TargetSelect.h>
#elif HAVE_LLVM_TARGET_TARGETSELECT_H
#  include <llvm/Target/TargetSelect.h>
#endif

#if HAVE_LLVM_SUPPORT_IRBUILDER_H
#  include <llvm/Support/IRBuilder.h>
#elif HAVE_LLVM_IRBUILDER_H
#  include <llvm/IRBuilder.h>
#endif

#if (LLVM_VERSION_MAJOR > 3) || (LLVM_VERSION_MINOR >= 2)
#  define LLVM_TYPE llvm::Type
#else
#  define LLVM_TYPE const llvm::Type
#endif

#include "ca-table.h"
#include "ca-llvm.h"
#include "query.h"

namespace ca_llvm
{
  bool initialize_done;

  llvm::Module *module;
  llvm::ExecutionEngine *engine;

  llvm::Function *f_ca_cast_to_text = NULL;
  llvm::Function *f_ca_cast_to_json = NULL;
  llvm::Function *f_ca_compare_equal = NULL;
  llvm::Function *f_ca_compare_like = NULL;

  LLVM_TYPE *t_output_value = NULL;

  LLVM_TYPE *t_int8;
  LLVM_TYPE *t_int8_pointer;
  LLVM_TYPE *t_int16;
  LLVM_TYPE *t_int32;
  LLVM_TYPE *t_int64;
  LLVM_TYPE *t_int64_pointer;

  LLVM_TYPE *t_pointer;
  LLVM_TYPE *t_size;

  LLVM_TYPE *t_float;
  LLVM_TYPE *t_double;

  /* t_int32    header
   * t_int8[4]  padding
   * t_int8[8]  data0
   * t_int8[8]  data1 */
  LLVM_TYPE *t_expression_value;
  LLVM_TYPE *t_expression_value_pointer;

  /* t_pointer
   * t_size */
  LLVM_TYPE *t_iovec;
  LLVM_TYPE *t_iovec_pointer;

  void
  initialize_types (const llvm::DataLayout *data_layout)
  {
    std::vector<LLVM_TYPE *> types;

    t_int8 =  llvm::Type::getInt8Ty (llvm::getGlobalContext ());
    t_int16 = llvm::Type::getInt16Ty (llvm::getGlobalContext ());
    t_int32 = llvm::Type::getInt32Ty (llvm::getGlobalContext ());
    t_int64 = llvm::Type::getInt64Ty (llvm::getGlobalContext ());

    t_int8_pointer = llvm::PointerType::get (t_int8, 0);
    t_int64_pointer = llvm::PointerType::get (t_int64, 0);

    if (sizeof (void *) == sizeof (int64_t))
      t_pointer = t_int64;
    else if (sizeof (void *) == sizeof (int32_t))
      t_pointer = t_int32;
    else
      assert (!"unhandled void * size");

    if (sizeof (size_t) == sizeof (int64_t))
      t_size = t_int64;
    else if (sizeof (size_t) == sizeof (int32_t))
      t_size = t_int32;
    else
      assert (!"unhandled void * size");

    t_float = llvm::Type::getFloatTy (llvm::getGlobalContext ());
    t_double = llvm::Type::getDoubleTy (llvm::getGlobalContext ());

    types.clear ();
    types.push_back (t_int32);
    types.push_back (t_int64);
    types.push_back (t_int64);
    t_expression_value
      = llvm::StructType::get (llvm::getGlobalContext (), types);
    t_expression_value_pointer
      = llvm::PointerType::get (t_expression_value, 0);

    types.clear ();
    types.push_back (t_pointer);
    types.push_back (t_size);
    t_iovec = llvm::StructType::get (llvm::getGlobalContext (), types);
    t_iovec_pointer = llvm::PointerType::get (t_iovec, 0);

    assert (sizeof (struct expression_value) == data_layout->getTypeAllocSize (t_expression_value));
    assert (sizeof (struct iovec) == data_layout->getTypeAllocSize (t_iovec));
  }

  bool
  initialize ()
  {
    std::vector<LLVM_TYPE *> argument_types;

    llvm::InitializeNativeTarget();

    module = new llvm::Module ("ca-table JIT module", llvm::getGlobalContext ());

    llvm::EngineBuilder engine_builder(module);
    std::string error_string;

    engine_builder.setErrorStr (&error_string);

    if (!(engine = engine_builder.create ()))
      {
        ca_set_error ("Failed to create execution engine: %s", error_string.c_str ());

        return false;
      }

    initialize_types (engine->getDataLayout ());

    argument_types.clear ();
    argument_types.push_back (t_pointer);
    argument_types.push_back (t_expression_value_pointer);

    f_ca_cast_to_text
      = llvm::Function::Create (llvm::FunctionType::get (t_int8_pointer, argument_types, false),
                                llvm::Function::ExternalLinkage,
                                "CA_cast_to_text", module);

    f_ca_cast_to_json
      = llvm::Function::Create (llvm::FunctionType::get (t_int8_pointer, argument_types, false),
                                llvm::Function::ExternalLinkage,
                                "CA_cast_to_json", module);

    argument_types.clear ();
    argument_types.push_back (t_expression_value_pointer);
    argument_types.push_back (t_expression_value_pointer);
    argument_types.push_back (t_expression_value_pointer);

    f_ca_compare_equal
      = llvm::Function::Create (llvm::FunctionType::get (t_int32, argument_types, false),
                                llvm::Function::ExternalLinkage,
                                "CA_compare_equal", module);
    f_ca_compare_like
      = llvm::Function::Create (llvm::FunctionType::get (t_int32, argument_types, false),
                                llvm::Function::ExternalLinkage,
                                "CA_compare_like", module);

    argument_types.clear ();
    argument_types.push_back (t_pointer);
    argument_types.push_back (t_int8_pointer);
    argument_types.push_back (t_int32);
    argument_types.push_back (t_int32);

    t_output_value = llvm::FunctionType::get (t_int32, argument_types, false);

    initialize_done = true;

    return true;
  }
} /* namespace ca_llvm */

ca_expression_function
ca_expression_compile (struct ca_query_parse_context *context,
                       const char *name,
                       struct expression *expr,
                       const struct ca_field *fields,
                       size_t field_count,
                       ca_output_function output_function)
{
  using namespace ca_llvm;

  llvm::Value *return_value = NULL;
  llvm::Value *output_value = NULL;

  unsigned int item_index = 0, item_count = 0;
  struct expression *expr_i;

  std::vector<LLVM_TYPE *> argument_types;

  if (!initialize_done && !initialize ())
    return NULL;

  auto builder = new llvm::IRBuilder<> (llvm::getGlobalContext ());

  argument_types.push_back (t_expression_value_pointer);
  argument_types.push_back (t_pointer);
  argument_types.push_back (t_iovec_pointer);
  auto function_type = llvm::FunctionType::get (t_int32, argument_types, false);

  auto function = llvm::Function::Create (function_type, llvm::Function::InternalLinkage, name, module);

  auto argument = function->arg_begin ();
  llvm::Value *result = argument++;
  llvm::Value *arena = argument++;
  llvm::Value *field_values = argument++;
  assert (argument == function->arg_end ());

  auto basic_block = llvm::BasicBlock::Create (llvm::getGlobalContext (), "entry", function);

  builder->SetInsertPoint (basic_block);

  if (output_function)
    {
      output_value = builder->CreateIntToPtr (llvm::ConstantInt::get (t_pointer, (ptrdiff_t) output_function),
                                              llvm::PointerType::get (t_output_value, 0));
    }

  for (expr_i = expr; expr_i; expr_i = expr_i->next)
    {
      if (expr_i->type == EXPR_ASTERISK)
        item_count += field_count;
      else
        ++item_count;
    }

  for (; expr; expr = expr->next)
    {
      struct select_item *si;

      si = (struct select_item *) expr;

      if (expr->type == EXPR_ASTERISK)
        {
          size_t i = 0;

          for (i = 0; i < field_count; ++i)
            {
              struct expression tmp_expr;

              tmp_expr.type = EXPR_FIELD;
              tmp_expr.value.type = (enum ca_type) fields[i].type;
              tmp_expr.value.d.field_index = i;

              if (!(return_value = subexpression_compile (builder, module, &tmp_expr, fields, result, arena, field_values)))
                return NULL;

              if (output_value)
                {
                  llvm::Value *string;

                  if (context->output_format == CA_PARAM_VALUE_JSON)
                    string = builder->CreateCall2 (f_ca_cast_to_json, arena, result);
                  else
                    string = builder->CreateCall2 (f_ca_cast_to_text, arena, result);

                  builder->CreateCall4 (output_value,
                                        llvm::ConstantInt::get (t_pointer, (uintptr_t) fields[i].name),
                                        string,
                                        llvm::ConstantInt::get (t_int32, item_index),
                                        llvm::ConstantInt::get (t_int32, item_count));
                  ++item_index;
                }
            }
        }
      else
        {
          if (!(return_value = subexpression_compile (builder, module, expr, fields, result, arena, field_values)))
            return NULL;

          if (output_value)
            {
              llvm::Value *string;

              if (context->output_format == CA_PARAM_VALUE_JSON)
                string = builder->CreateCall2 (f_ca_cast_to_json, arena, result);
              else
                string = builder->CreateCall2 (f_ca_cast_to_text, arena, result);

              builder->CreateCall4 (output_value,
                                    llvm::ConstantInt::get (t_pointer, (uintptr_t) si->alias),
                                    string,
                                    llvm::ConstantInt::get (t_int32, item_index),
                                    llvm::ConstantInt::get (t_int32, item_count));
              ++item_index;
            }
        }
    }

  builder->CreateRet (return_value);

  llvm::verifyFunction (*function);

  auto fpm = new llvm::FunctionPassManager (module);

  fpm->add (new llvm::DataLayout (*engine->getDataLayout ())); /* Freed by fpm */
  fpm->add (llvm::createBasicAliasAnalysisPass ());
  fpm->add (llvm::createInstructionCombiningPass ());
  fpm->add (llvm::createReassociatePass ());
  fpm->add (llvm::createGVNPass ());
  fpm->add (llvm::createCFGSimplificationPass ());
  fpm->doInitialization ();
  fpm->run (*function);

  delete fpm;

  return (ca_expression_function) engine->getPointerToFunction (function);
}
