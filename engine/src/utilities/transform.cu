#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/utilities/traits.hpp>
#include <rmm/thrust_rmm_allocator.h>
#include <thrust/transform.h>
#pragma GCC diagnostic pop

#include "transform.hpp"
#include "error.hpp"

namespace ral {
namespace utilities {

struct length_to_end_functor {
  template<typename T>
  std::enable_if_t<std::is_integral<T>::value && !cudf::is_boolean<T>()>
  operator()(cudf::mutable_column_view& length, const cudf::column_view & start, cudaStream_t stream = 0)
  {
    thrust::transform(rmm::exec_policy(stream)->on(stream),
                      length.begin<T>(),
                      length.end<T>(),
                      start.begin<T>(),
                      length.begin<T>(),
                      [] __device__ (auto len_, auto start_){
                        return start_ + len_;
                      });
  }

  template<typename T>
  std::enable_if_t<!std::is_integral<T>::value || cudf::is_boolean<T>()>
  operator()(cudf::mutable_column_view& length, const cudf::column_view & start, cudaStream_t stream = 0)
  {
    RAL_FAIL("Only integer types supported");
  }
};

void transform_length_to_end(cudf::mutable_column_view& length, const cudf::column_view & start) {
  RAL_EXPECTS(length.type() == start.type(), "Mistmatched type between length and start columns");

  cudf::type_dispatcher(length.type(), length_to_end_functor{}, length, start);
}

struct start_to_zero_based_indexing_functor {
  template<typename T>
  std::enable_if_t<std::is_integral<T>::value && !cudf::is_boolean<T>()>
  operator()(cudf::mutable_column_view& start, cudaStream_t stream = 0)
  {
    thrust::transform(rmm::exec_policy(stream)->on(stream),
                      start.begin<T>(),
                      start.end<T>(),
                      start.begin<T>(),
                      [] __device__ (auto start_){
                        return start_ - 1;
                      });
  }

  template<typename T>
  std::enable_if_t<!std::is_integral<T>::value || cudf::is_boolean<T>()>
  operator()(cudf::mutable_column_view& start, cudaStream_t stream = 0)
  {
    RAL_FAIL("Only integer types supported");
  }
};

void transform_start_to_zero_based_indexing(cudf::mutable_column_view& start) {
  cudf::type_dispatcher(start.type(), start_to_zero_based_indexing_functor{}, start);
}

}  // namespace utilities
}  // namespace ral
