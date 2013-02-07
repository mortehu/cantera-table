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

  LLVM_TYPE *t_int8;
  LLVM_TYPE *t_int8_pointer;
  LLVM_TYPE *t_int16;
  LLVM_TYPE *t_int32;
  LLVM_TYPE *t_int64;

  LLVM_TYPE *t_pointer;
  LLVM_TYPE *t_size;

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

    types.clear ();
    types.push_back (t_int32);
    types.push_back (llvm::ArrayType::get (t_int8, 4));
    types.push_back (llvm::ArrayType::get (t_int8, 8));
    types.push_back (llvm::ArrayType::get (t_int8, 8));
    t_expression_value
      = llvm::StructType::get (llvm::getGlobalContext (), types);
    t_expression_value_pointer
      = llvm::PointerType::get (t_expression_value, 0);

    types.clear ();
    types.push_back (t_pointer);
    types.push_back (t_size);
    t_iovec = llvm::StructType::get (llvm::getGlobalContext (), types);
    t_iovec_pointer
      = llvm::PointerType::get (t_iovec, 0);

    assert (sizeof (struct expression_value) == data_layout->getTypeAllocSize (t_expression_value));
    assert (sizeof (struct iovec) == data_layout->getTypeAllocSize (t_iovec));
  }

  bool
  initialize ()
  {
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

    initialize_done = true;

    return true;
  }
} /* namespace ca_llvm */

ca_expression_function
ca_expression_compile (struct expression *expr,
                      const struct ca_field *fields,
                      size_t field_count)
{
  using namespace ca_llvm;

  llvm::Value *return_value;

  std::vector<LLVM_TYPE *> argument_types;

  if (!initialize_done && !initialize ())
    return NULL;

  auto builder = new llvm::IRBuilder<> (llvm::getGlobalContext ());

  argument_types.push_back (t_expression_value_pointer);
  argument_types.push_back (t_pointer);
  argument_types.push_back (t_iovec_pointer);

  auto function_type = llvm::FunctionType::get (t_int32, argument_types, false);

  auto function = llvm::Function::Create (function_type, llvm::Function::InternalLinkage, "my_function", module);

  auto argument = function->arg_begin ();
  llvm::Value *result = argument++;
  llvm::Value *arena = argument++;
  llvm::Value *field_values = argument++;
  assert (argument == function->arg_end ());

  auto basic_block = llvm::BasicBlock::Create (llvm::getGlobalContext (), "entry", function);

  builder->SetInsertPoint (basic_block);

  if (!(return_value = subexpression_compile (builder, module, expr, result, arena, field_values)))
    return NULL;

  builder->CreateRet (return_value);

  llvm::verifyFunction (*function);

  auto fpm = new llvm::FunctionPassManager (module);

  /* XXX: From example code: fpm->add (new llvm::DataLayout (engine->getDataLayout ())); */
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
