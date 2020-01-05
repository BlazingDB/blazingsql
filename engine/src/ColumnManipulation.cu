
//TODO: in theory  we want to get rid of this
// we should be using permutation iterators when we can

#include "ColumnManipulation.cuh"

#include <thrust/functional.h>
#include <thrust/device_ptr.h>
#include <thrust/device_vector.h>
#include <thrust/copy.h>
#include <thrust/gather.h>
#include <thrust/remove.h>
#include <thrust/iterator/counting_iterator.h>

#include <thrust/execution_policy.h>
#include <thrust/iterator/iterator_adaptor.h>
#include <thrust/iterator/transform_iterator.h>

#include "Utils.cuh"
#include "cuDF/safe_nvcategory_gather.hpp"
#include <cudf/legacy/bitmask.hpp>
#include "Traits/RuntimeTraits.h"

#include <cudf/types.hpp>

const size_t NUM_ELEMENTS_PER_THREAD_GATHER_BITS = 32;
template <typename BitContainer, typename Index>
__global__ void gather_bits(
		const Index*        __restrict__ indices,
		const BitContainer* __restrict__ bit_data,
		BitContainer* __restrict__ gathered_bits,
		cudf::size_type                    num_indices
){
	size_t thread_index = blockIdx.x * blockDim.x + threadIdx.x ;
	size_t element_index = NUM_ELEMENTS_PER_THREAD_GATHER_BITS * thread_index;
	while( element_index < num_indices){

		BitContainer current_bits;

		for(size_t bit_index = 0; bit_index < NUM_ELEMENTS_PER_THREAD_GATHER_BITS; bit_index++){
			//NOTE!!! if we assume that sizeof BitContainer is smaller than the required padding of 64bytes for valid_ptrs
			//then we dont have to do this check
			if((element_index + bit_index) < num_indices){
				Index permute_index = indices[element_index + bit_index];
				bool is_bit_set;
                if(permute_index >=  0){
					//this next line is failing

                    is_bit_set = bit_data != nullptr? bit_data[permute_index / NUM_ELEMENTS_PER_THREAD_GATHER_BITS] & (1u << (permute_index % NUM_ELEMENTS_PER_THREAD_GATHER_BITS)):true;
					//is_bit_set = true;
				}else{
                    is_bit_set = false;
				}
				current_bits = (current_bits  & ~(1u<<bit_index)) | (static_cast<unsigned int>(is_bit_set) << bit_index); //seems to work
				/*
				 * this is effectively what hte code above is doing but it should help us avoid thread divergence
				if(is_bit_set){
					current_bit|= 1<<bit_index;
				}else{
					current_bit&=  ~ (1<< bit_index);
				}
				 */
			}

		}
		gathered_bits[thread_index] = current_bits;
		thread_index += blockDim.x * gridDim.x;
		element_index = NUM_ELEMENTS_PER_THREAD_GATHER_BITS * thread_index;
	}
}

void materialize_valid_ptrs(cudf::column * input, cudf::column * output, cudf::column * row_indices){
	// TODO percy cudf0.12 port to cudf::column
//    //if (output->valid != nullptr and input->valid != nullptr){
//    if (output->valid != nullptr && row_indices->size > 0) {
//		int grid_size, block_size;

//		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSize(&grid_size,&block_size,gather_bits<unsigned int, int>));

//		gather_bits<<<grid_size, block_size>>>((int *) row_indices->data,(int *) input->valid,(int *) output->valid, row_indices->size);

//		CheckCudaErrors(cudaGetLastError());
//	}
}

//input and output shoudl be the same time
template <typename ElementIterator, typename IndexIterator>
void materialize_templated_2(cudf::column * input, cudf::column * output, cudf::column * row_indices){
	// TODO percy cudf0.12 port to cudf::column
//		materialize_valid_ptrs(input,output,row_indices);

//		thrust::detail::normal_iterator<thrust::device_ptr<ElementIterator> > element_iter =
//				thrust::detail::make_normal_iterator(thrust::device_pointer_cast((ElementIterator *) input->data));

//		thrust::detail::normal_iterator<thrust::device_ptr<IndexIterator> > index_iter =
//				thrust::detail::make_normal_iterator(thrust::device_pointer_cast((IndexIterator *) row_indices->data));

//		thrust::detail::normal_iterator<thrust::device_ptr<ElementIterator> > output_iter =
//				thrust::detail::make_normal_iterator(thrust::device_pointer_cast((ElementIterator *) output->data));

//		thrust::gather_if(index_iter, index_iter + row_indices->size,
//											index_iter,
//											element_iter,
//											output_iter,
//											[] __device__ (const IndexIterator x) {
//												return x >= 0;
//											});

//		if (output->size == 0 || output->valid == nullptr) {
//			output->null_count = 0;
//		}
//		else {
//			int count;
//			if (output->valid) {
//				gdf_error result = gdf_count_nonzero_mask(output->valid, output->size, &count);
//				assert(result == GDF_SUCCESS);
//				output->null_count = output->size - static_cast<cudf::size_type>(count);
//			} else{
//				output->null_count = 0;
//			}
//		}

//	if( input->dtype == GDF_STRING_CATEGORY ){
//    ral::safe_nvcategory_gather_for_string_category(output, input->dtype_info.category);
//	}
}

template <typename ElementIterator>
void materialize_templated_1(cudf::column * input, cudf::column * output, cudf::column * row_indices){
	// TODO percy cudf0.12 port to cudf::column
//	int column_width = ral::traits::get_dtype_size_in_bytes(row_indices);
//	if(column_width == 1){
//		return materialize_templated_2<ElementIterator,int8_t>(input,output,row_indices);
//	}else if(column_width == 2){
//		return materialize_templated_2<ElementIterator,int16_t>(input,output,row_indices);
//	}else if(column_width == 4){
//		return materialize_templated_2<ElementIterator,int32_t>(input,output,row_indices);
//	}else if(column_width == 8){
//		return materialize_templated_2<ElementIterator,int64_t>(input,output,row_indices);
//	}

//	throw std::runtime_error("In materialize_templated_1 function: unsupported type");
}


void materialize_column(cudf::column * input, cudf::column * output, cudf::column * row_indices){
	// TODO percy cudf0.12 port to cudf::column
//	int column_width = ral::traits::get_dtype_size_in_bytes(input);
//	if(column_width == 1){
//		return materialize_templated_1<int8_t>(input,output,row_indices);
//	}else if(column_width == 2){
//		return materialize_templated_1<int16_t>(input,output,row_indices);
//	}else if(column_width == 4){
//		return materialize_templated_1<int32_t>(input,output,row_indices);
//	}else if(column_width == 8){
//		return materialize_templated_1<int64_t>(input,output,row_indices);
//	}

//	throw std::runtime_error("In materialize_column function: unsupported type");
}
