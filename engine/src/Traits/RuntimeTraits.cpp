#include <cudf/types.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>

#include "Traits/RuntimeTraits.h"
#include "Utils.cuh"

namespace ral {
namespace traits {

struct size_of_functor{
	template <typename T>
  constexpr std::enable_if_t<!cudf::is_fixed_width<T>(), cudf::size_type> operator()() const {
    RAL_FAIL("Invalid, non fixed-width element type.");
  }

  template <typename T>
  constexpr std::enable_if_t<cudf::is_fixed_width<T>(), cudf::size_type> operator()() const
      noexcept {
    return sizeof(T);
  }
};

cudf::size_type get_dtype_size_in_bytes(cudf::type_id dtype) {
	if (dtype == cudf::type_id::EMPTY) {
		return 0;
	}
	
	return cudf::experimental::type_dispatcher(cudf::data_type{dtype}, size_of_functor{});
}

}  // namespace traits
}  // namespace ral
