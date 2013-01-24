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

/* The LLVM project likes to move headers between versions */
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

#include "ca-table.h"
#include "query.h"

static llvm::Value *
subexpression_compile (llvm::IRBuilder<> *builder, llvm::Module *module, struct expression *expr)
{
  llvm::Value *lhs = NULL, *rhs = NULL;

  if (expr->lhs
      && !(lhs = subexpression_compile (builder, module, expr->lhs)))
    return NULL;

  if (expr->rhs
      && !(rhs = subexpression_compile (builder, module, expr->rhs)))
    return NULL;

  switch (expr->type)
    {
    case EXPR_CONSTANT:

      switch (expr->value.type)
        {
        case CA_TEXT:

          return builder->CreateGlobalStringPtr (expr->value.d.string_literal);

        case CA_TIME_SERIES:

          ca_set_error ("Time series constants not supported yet");

          return NULL;

        case CA_TABLE_DECLARATION:

          ca_set_error ("Table declaration constants not supported yet");

          return NULL;

        case CA_INT64:
        case CA_TIME:

          return llvm::ConstantInt::get (llvm::getGlobalContext (), llvm::APInt (64, expr->value.d.integer, true /* signed */));

        case CA_NUMERIC:

          ca_set_error ("Numeric constants not supported yet");

          return NULL;

        default:

          ca_set_error ("Unknown type id %d", expr->value.type);

          return NULL;
        }

    case EXPR_VARIABLE:

        {
          switch (expr->value.d.variable->value.type)
            {
            default:

              ca_set_error ("Fields of type %d not supported yet", expr->value.d.variable->value.type);

              return NULL;
            }
        }

      return NULL;

    case EXPR_EQUAL:

      if (lhs->getType ()->getTypeID () != rhs->getType ()->getTypeID ())
        {
          ca_set_error ("Attempt to compare variables of different types");

          /* Convert integer to float like this:
             rhs = builder->CreateCast (llvm::Instruction::SIToFP, rhs, lhs->getType ()); */

          return NULL;
        }

      switch (lhs->getType ()->getTypeID())
        {
        case llvm::Type::FloatTyID:
        case llvm::Type::DoubleTyID:

          return builder->CreateFCmpOEQ (lhs, rhs);

        case llvm::Type::IntegerTyID:

          return builder->CreateICmpEQ (lhs, rhs);

        case llvm::Type::PointerTyID:

            {
#if 0
              auto callee = module->getFunction ("strcmp");

              if (!callee)
                {
                  std::vector<llvm::Type *> strcmp_args;
                  strcmp_args.push_back (llvm::Type::getInt8PtrTy (llvm::getGlobalContext()));
                  strcmp_args.push_back (llvm::Type::getInt8PtrTy (llvm::getGlobalContext()));

                  auto strcmp_ft = llvm::FunctionType::get (llvm::Type::getInt32Ty(llvm::getGlobalContext()), strcmp_args, false);

                  callee = llvm::Function::Create (strcmp_ft,
                                                   llvm::Function::ExternalLinkage, "strcmp", module);
                }

              return builder->CreateCall2 (callee, lhs, rhs);
#endif
            }

        default:

          ca_set_error ("Don't know how to compare type (%d)", lhs->getType ()->getTypeID ());

          return NULL;
        }

    default:

      ca_set_error ("Expression type %d not supported", expr->type);

      return NULL;
    }
}

ca_expression_function
ca_expression_compile (struct expression *expr)
{
  std::string error_string;
  llvm::Value *return_value;

  llvm::InitializeNativeTarget();

  auto module = new llvm::Module ("My JIT Module", llvm::getGlobalContext ());
  auto builder = new llvm::IRBuilder<> (llvm::getGlobalContext ());

  auto engine = llvm::EngineBuilder (module).setErrorStr (&error_string).create ();

  if (!engine)
    {
      ca_set_error ("Failed to create execution engine: %s", error_string.c_str ());

      return NULL;
    }

  std::vector<const llvm::Type *> argument_types;

  auto function_type = llvm::FunctionType::get ((const llvm::Type *) llvm::Type::getInt32Ty (llvm::getGlobalContext ()),
                                                argument_types, false);

  auto function = llvm::Function::Create (function_type, llvm::Function::InternalLinkage, "my_function", module);

  auto basic_block = llvm::BasicBlock::Create (llvm::getGlobalContext (), "entry", function);

  builder->SetInsertPoint (basic_block);

  if (!(return_value = subexpression_compile (builder, module, expr)))
    return NULL;

  builder->CreateRet (return_value);

  llvm::verifyFunction (*function);

#if 0
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
#endif

  return (ca_expression_function) engine->getPointerToFunction (function);
}
