#include "src/context.h"

namespace cantera {
namespace table {
namespace internal {

thread_local Context* Context::context_ = nullptr;

}  // namespace internal
}  // namespace table
}  // namespace cantera
