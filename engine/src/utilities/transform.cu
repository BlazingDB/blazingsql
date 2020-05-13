#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/utilities/traits.hpp>
#include <rmm/rmm.h>
#include <rmm/thrust_rmm_allocator.h>
#include <thrust/transform.h>
#include "transform.hpp"
#include "Utils.cuh"

namespace ral {
namespace utilities {

struct length_to_end_functor {
  template<typename T, std::enable_if_t<std::is_integral<T>::value && !cudf::is_boolean<T>()> * = nullptr>
  void operator()(cudf::mutable_column_view& target, const cudf::column_view & start, cudaStream_t stream = 0)
  {
    // auto a = thrust::counting_iterator<int>(0);
    // auto b = thrust::counting_iterator<int>(2);
    // thrust::transform(a,
    //                   b,
    //                   target.begin<int>(),
    //                   [] __device__ (int len_){
    //                     return 0;
    //                   });

    // thrust::transform(rmm::exec_policy(stream)->on(stream),
    //                   target.begin<T>(),
    //                   target.end<T>(),
    //                   start.begin<T>(),
    //                   target.begin<T>(),
    //                   [] __device__ (auto len_, auto start_){
    //                     return start_ + len_;
    //                   });
  }

  template<typename T, std::enable_if_t<!std::is_integral<T>::value || cudf::is_boolean<T>()> * = nullptr>
  void operator()(cudf::mutable_column_view& target, const cudf::column_view & start, cudaStream_t stream = 0)
  {
    RAL_FAIL("Only integer types supported");
  }
};

void inplace_length_to_end_transform(cudf::mutable_column_view& target, const cudf::column_view & start) {
  RAL_EXPECTS(target.type() == start.type(), "Mistmatched type between target and start columns");

  cudf::experimental::type_dispatcher(target.type(), length_to_end_functor{}, target, start);

  // thrust::transform(rmm::exec_policy(0)->on(0),
  //                 target.begin<int>(),
  //                 target.end<int>(),
  //                 start.begin<int>(),
  //                 target.begin<int>(),
  //                 [] __device__ (auto len_, auto start_){
  //                   return start_ + len_;
  //                 });
}

}  // namespace utilities
}  // namespace ral
