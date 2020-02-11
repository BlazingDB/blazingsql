#ifndef TRAITS_RUNTIME_TRAITS
#define TRAITS_RUNTIME_TRAITS

#include <cstddef>
#include <cstdint>
#include <cudf/types.h>
#include <cudf/types.hpp>
#include <string>


namespace ral {
namespace traits {

cudf::size_type get_dtype_size_in_bytes(cudf::type_id dtype);

}  // namespace traits
}  // namespace ral

#endif
