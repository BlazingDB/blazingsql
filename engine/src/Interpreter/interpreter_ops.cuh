
#include <algorithm>
#include <chrono>
#include <cuda_runtime.h>
#include <cudf/datetime.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <limits>
#include <map>
#include <numeric>
#include <simt/chrono>
#include <thrust/device_vector.h>
#include <thrust/reduce.h>
#include <type_traits>
#include <vector>

#include "CalciteExpressionParsing.h"
#include "gdf_wrapper/gdf_types.cuh"
#include "interpreter_cpp.h"

namespace interops {

typedef int64_t temp_gdf_valid_type;  // until its an int32 in cudf
typedef int16_t column_index_type;

template <typename T>
__device__ __forceinline__ T getMagicNumber() {
	return T{};
}

template <>
__device__ __forceinline__ int64_t getMagicNumber<int64_t>() {
	return std::numeric_limits<int64_t>::max() - 13ll;
}

template <>
__device__ __forceinline__ double getMagicNumber<double>() {
	return 1.7976931348623123e+308;
}

using cudf::datetime::detail::datetime_component;
template <typename Timestamp, datetime_component Component>
struct extract_component_operator {
	static_assert(cudf::is_timestamp<Timestamp>(), "");

	CUDA_DEVICE_CALLABLE int16_t operator()(Timestamp const ts) const {
		using namespace simt::std::chrono;

		auto days_since_epoch = floor<days>(ts);

		auto time_since_midnight = ts - days_since_epoch;

		if(time_since_midnight.count() < 0) {
			time_since_midnight += days(1);
		}

		auto hrs_ = duration_cast<hours>(time_since_midnight);
		auto mins_ = duration_cast<minutes>(time_since_midnight - hrs_);
		auto secs_ = duration_cast<seconds>(time_since_midnight - hrs_ - mins_);

		switch(Component) {
		case datetime_component::YEAR: return static_cast<int>(year_month_day(days_since_epoch).year());
		case datetime_component::MONTH: return static_cast<unsigned>(year_month_day(days_since_epoch).month());
		case datetime_component::DAY: return static_cast<unsigned>(year_month_day(days_since_epoch).day());
		case datetime_component::WEEKDAY: return year_month_weekday(days_since_epoch).weekday().iso_encoding();
		case datetime_component::HOUR: return hrs_.count();
		case datetime_component::MINUTE: return mins_.count();
		case datetime_component::SECOND: return secs_.count();
		default: return 0;
		}
	}
};

template <typename PrimitiveType, datetime_component Component>
struct launch_extract_component {
	template <typename Element, std::enable_if_t<!cudf::is_timestamp<Element>()> * = nullptr>
	CUDA_DEVICE_CALLABLE int16_t operator()(PrimitiveType val) {
		return 0;
	}

	template <typename Timestamp, std::enable_if_t<cudf::is_timestamp<Timestamp>()> * = nullptr>
	CUDA_DEVICE_CALLABLE int16_t operator()(PrimitiveType val) {
		return extract_component_operator<Timestamp, Component>{}(Timestamp{static_cast<typename Timestamp::rep>(val)});
	}
};

struct scale_to_64_bit_functor {
	template <typename T, std::enable_if_t<!cudf::is_simple<T>()> * = nullptr>
	int64_t operator()(cudf::scalar * s) {
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		auto typed_scalar = static_cast<ScalarType *>(s);

		return 0;
	}

	template <typename T, std::enable_if_t<std::is_integral<T>::value || cudf::is_boolean<T>()> * = nullptr>
	int64_t operator()(cudf::scalar * s) {
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		auto typed_scalar = static_cast<ScalarType *>(s);

		return typed_scalar->value();
	}

	template <typename T, std::enable_if_t<std::is_floating_point<T>::value> * = nullptr>
	int64_t operator()(cudf::scalar * s) {
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		auto typed_scalar = static_cast<ScalarType *>(s);

		int64_t ret_val{};
		double * ret_val_ptr = (double *) &ret_val;
		*ret_val_ptr = typed_scalar->value();
		return ret_val;
	}

	template <typename T, std::enable_if_t<cudf::is_timestamp<T>()> * = nullptr>
	int64_t operator()(cudf::scalar * s) {
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		auto typed_scalar = static_cast<ScalarType *>(s);

		return typed_scalar->value().time_since_epoch().count();
	}
};

static __device__ __host__ __forceinline__ bool isInt(cudf::type_id type) {
	return (type == cudf::type_id::BOOL8) || (type == cudf::type_id::INT8) || (type == cudf::type_id::INT16) ||
		   (type == cudf::type_id::INT32) || (type == cudf::type_id::INT64) ||
		   (type == cudf::type_id::TIMESTAMP_DAYS) || (type == cudf::type_id::TIMESTAMP_SECONDS) ||
		   (type == cudf::type_id::TIMESTAMP_MILLISECONDS) || (type == cudf::type_id::TIMESTAMP_MICROSECONDS) ||
		   (type == cudf::type_id::TIMESTAMP_NANOSECONDS) || (type == cudf::type_id::CATEGORY);
}

static __device__ __forceinline__ bool isFloat(cudf::type_id type) {
	return (type == cudf::type_id::FLOAT32) || (type == cudf::type_id::FLOAT64);
}

/**
 * every element that is stored in the local buffer is 8 bytes in local, so signed ints are cast to int64, unsigned to
 * uint64, and floating points are all doubles
 */
class InterpreterFunctor {
public:
	~InterpreterFunctor() = default;
	InterpreterFunctor(InterpreterFunctor && other) = default;
	InterpreterFunctor(InterpreterFunctor const & other) = default;
	InterpreterFunctor & operator=(InterpreterFunctor const & other) = delete;
	InterpreterFunctor & operator=(InterpreterFunctor && other) = delete;

	InterpreterFunctor(cudf::mutable_table_view & output_table,
		const cudf::table_view & table,
		const std::vector<column_index_type> & left_input_positions_vec,
		const std::vector<column_index_type> & right_input_positions_vec,
		const std::vector<column_index_type> & output_positions_vec,
		const std::vector<column_index_type> & final_output_positions_vec,
		const std::vector<gdf_binary_operator_exp> & operators,
		const std::vector<gdf_unary_operator> & unary_operators,
		const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
		const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars,
		int threadBlockSize,
		void * temp_space,
		void * temp_valids_in_buffer,
		void * temp_valids_out_buffer,
		cudaStream_t stream)
		: num_columns{table.num_columns()}, num_operations{left_input_positions_vec.size()},
		  num_final_outputs{final_output_positions_vec.size()}, temp_valids_in_buffer{static_cast<int64_t *>(
																	temp_valids_in_buffer)},
		  temp_valids_out_buffer{static_cast<int64_t *>(temp_valids_out_buffer)}, threadBlockSize{threadBlockSize} {
		// VERY IMPORTANT!!!!
		// the temporary space MUST be allocated in space from largest to smallest elements, if you don't follow this
		// pattern you end up in
		// a situation where you can makme misaligned accesses to memory.
		int8_t * cur_temp_space = static_cast<int8_t *>(temp_space);
		column_data = (void **) cur_temp_space;
		cur_temp_space += sizeof(void *) * num_columns;
		output_data = (void **) cur_temp_space;
		cur_temp_space += sizeof(void *) * num_final_outputs;
		scalars_left = (int64_t *) cur_temp_space;
		cur_temp_space += sizeof(int64_t) * num_operations;
		scalars_right = (int64_t *) cur_temp_space;
		cur_temp_space += sizeof(int64_t) * num_operations;
		valid_ptrs = (temp_gdf_valid_type **) cur_temp_space;
		cur_temp_space += sizeof(temp_gdf_valid_type *) * num_columns;
		valid_ptrs_out = (temp_gdf_valid_type **) cur_temp_space;
		cur_temp_space += sizeof(temp_gdf_valid_type *) * num_final_outputs;
		null_counts_inputs = (cudf::size_type *) cur_temp_space;
		cur_temp_space += sizeof(cudf::size_type) * num_columns;
		null_counts_outputs = (cudf::size_type *) cur_temp_space;
		cur_temp_space += sizeof(cudf::size_type) * num_final_outputs;
		input_column_types = (cudf::type_id *) cur_temp_space;
		cur_temp_space += sizeof(cudf::type_id) * num_columns;
		input_types_left = (cudf::type_id *) cur_temp_space;
		cur_temp_space += sizeof(cudf::type_id) * num_operations;
		input_types_right = (cudf::type_id *) cur_temp_space;
		cur_temp_space += sizeof(cudf::type_id) * num_operations;
		output_types = (cudf::type_id *) cur_temp_space;
		cur_temp_space += sizeof(cudf::type_id) * num_operations;
		final_output_types = (cudf::type_id *) cur_temp_space;
		cur_temp_space += sizeof(cudf::type_id) * num_final_outputs;
		binary_operations = (gdf_binary_operator_exp *) cur_temp_space;
		cur_temp_space += sizeof(gdf_binary_operator_exp) * num_operations;
		unary_operations = (gdf_unary_operator *) cur_temp_space;
		cur_temp_space += sizeof(gdf_unary_operator) * num_operations;
		left_input_positions = (int16_t *) cur_temp_space;
		cur_temp_space += sizeof(int16_t) * num_operations;
		right_input_positions = (int16_t *) cur_temp_space;
		cur_temp_space += sizeof(int16_t) * num_operations;
		output_positions = (int16_t *) cur_temp_space;
		cur_temp_space += sizeof(int16_t) * num_operations;
		final_output_positions = (int16_t *) cur_temp_space;
		cur_temp_space += sizeof(int16_t) * num_final_outputs;


		std::vector<const void *> host_data_ptrs(num_columns);
		std::vector<const cudf::bitmask_type *> host_valid_ptrs(num_columns);
		std::vector<cudf::size_type> host_null_counts(num_columns);
		std::vector<cudf::size_type> host_null_counts_output(num_final_outputs, 0);
		for(int i = 0; i < num_columns; i++) {
			const cudf::column_view & c = table.column(i);
			host_data_ptrs[i] = c.data<void>();
			host_valid_ptrs[i] = c.null_mask();
			host_null_counts[i] = c.null_count();
		}

		CUDA_TRY(cudaMemcpyAsync(
			this->column_data, host_data_ptrs.data(), sizeof(void *) * num_columns, cudaMemcpyHostToDevice, stream));
		CUDA_TRY(cudaMemcpyAsync(this->valid_ptrs,
			host_valid_ptrs.data(),
			sizeof(cudf::bitmask_type *) * num_columns,
			cudaMemcpyHostToDevice,
			stream));
		CUDA_TRY(cudaMemcpyAsync(this->null_counts_inputs,
			host_null_counts.data(),
			sizeof(cudf::size_type) * num_columns,
			cudaMemcpyHostToDevice,
			stream));
		CUDA_TRY(cudaMemcpyAsync(this->null_counts_outputs,
			host_null_counts_output.data(),
			sizeof(cudf::size_type) * num_final_outputs,
			cudaMemcpyHostToDevice,
			stream));

		std::vector<const void *> host_out_data_ptrs(num_final_outputs);
		std::vector<const cudf::bitmask_type *> host_out_valid_ptrs(num_final_outputs);
		for(int i = 0; i < num_final_outputs; i++) {
			cudf::mutable_column_view & c = output_table.column(i);
			host_out_data_ptrs[i] = c.data<void>();
			host_out_valid_ptrs[i] = c.null_mask();
		}

		CUDA_TRY(cudaMemcpyAsync(this->output_data,
			host_out_data_ptrs.data(),
			sizeof(void *) * num_final_outputs,
			cudaMemcpyHostToDevice,
			stream));
		CUDA_TRY(cudaMemcpyAsync(this->valid_ptrs_out,
			host_out_valid_ptrs.data(),
			sizeof(cudf::bitmask_type *) * num_final_outputs,
			cudaMemcpyHostToDevice,
			stream));


		// copy over inputs
		std::vector<cudf::type_id> left_input_types_vec(num_operations);
		std::vector<cudf::type_id> right_input_types_vec(num_operations);
		std::vector<cudf::type_id> output_types_vec(num_operations);
		std::vector<cudf::type_id> output_final_types_vec(num_final_outputs);
		std::vector<int64_t> left_scalars_host(num_operations);
		std::vector<int64_t> right_scalars_host(num_operations);
		std::map<int, cudf::type_id> output_map_type;
		for(int i = 0; i < num_operations; i++) {
			column_index_type left_index = left_input_positions_vec[i];
			column_index_type right_index = right_input_positions_vec[i];
			column_index_type output_index = output_positions_vec[i];

			if(left_index >= 0 && left_index < num_columns) {
				left_input_types_vec[i] = table.column(left_index).type().id();
			} else if(left_index < 0) {
				if(left_index == SCALAR_NULL_INDEX) {
					// this was a null scalar in left weird but ok
					left_input_types_vec[i] = left_scalars[i]->type().id();
				} else if(left_index == SCALAR_INDEX) {
					// get scalars type
					left_input_types_vec[i] =
						(isInt(left_scalars[i]->type().id()) ? cudf::type_id::INT64 : cudf::type_id::FLOAT64);
					left_scalars_host[i] = cudf::experimental::type_dispatcher(
						left_scalars[i]->type(), scale_to_64_bit_functor{}, left_scalars[i].get());
				}
			} else {
				// have to get it from the output that generated it
				left_input_types_vec[i] = output_map_type[left_index];
			}

			if(right_index >= 0 && right_index < num_columns) {
				right_input_types_vec[i] = table.column(right_index).type().id();
			} else if(right_index < 0) {
				if(right_index == SCALAR_NULL_INDEX) {
					// this was a null scalar weird but ok
					// TODO: figure out if we have to do anything here, i am sure we will for coalesce
					right_input_types_vec[i] = right_scalars[i]->type().id();
				} else if(right_index == SCALAR_INDEX) {
					// get scalars type
					right_input_types_vec[i] =
						(isInt(right_scalars[i]->type().id()) ? cudf::type_id::INT64 : cudf::type_id::FLOAT64);
					right_scalars_host[i] = cudf::experimental::type_dispatcher(
						right_scalars[i]->type(), scale_to_64_bit_functor{}, right_scalars[i].get());
				} else if(right_index == UNARY_INDEX) {
					// right wont be used its a unary operation
				}
			} else {
				right_input_types_vec[i] = output_map_type[right_index];
			}

			cudf::type_id type_from_op;
			if(right_index == UNARY_INDEX) {
				// unary
				type_from_op = get_output_type(left_input_types_vec[i], unary_operators[i]);
			} else {
				// binary
				type_from_op = get_output_type(left_input_types_vec[i], right_input_types_vec[i], operators[i]);
			}

			if(is_numeric_type(type_from_op) && !is_type_float(type_from_op)) {
				output_types_vec[i] = cudf::type_id::INT64;
			} else if(is_type_float(type_from_op)) {
				output_types_vec[i] = cudf::type_id::FLOAT64;
			}

			output_map_type[output_index] = output_types_vec[i];
		}

		// put the output final positions
		for(cudf::size_type i = 0; i < output_table.num_columns(); i++) {
			output_final_types_vec[i] = output_table.column(i).type().id();
		}

		std::vector<cudf::type_id> input_column_types_vec(num_columns);
		for(cudf::size_type i = 0; i < table.num_columns(); i++) {
			input_column_types_vec[i] = table.column(i).type().id();
		}

		CUDA_TRY(cudaMemcpyAsync(left_input_positions,
			left_input_positions_vec.data(),
			left_input_positions_vec.size() * sizeof(int16_t),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(right_input_positions,
			right_input_positions_vec.data(),
			right_input_positions_vec.size() * sizeof(int16_t),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(output_positions,
			output_positions_vec.data(),
			output_positions_vec.size() * sizeof(int16_t),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(final_output_positions,
			final_output_positions_vec.data(),
			final_output_positions_vec.size() * sizeof(int16_t),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(binary_operations,
			operators.data(),
			operators.size() * sizeof(gdf_binary_operator_exp),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(unary_operations,
			unary_operators.data(),
			unary_operators.size() * sizeof(gdf_unary_operator),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(input_column_types,
			input_column_types_vec.data(),
			input_column_types_vec.size() * sizeof(cudf::type_id),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(input_types_left,
			left_input_types_vec.data(),
			left_input_types_vec.size() * sizeof(cudf::type_id),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(input_types_right,
			right_input_types_vec.data(),
			right_input_types_vec.size() * sizeof(cudf::type_id),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(output_types,
			output_types_vec.data(),
			output_types_vec.size() * sizeof(cudf::type_id),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(final_output_types,
			output_final_types_vec.data(),
			output_final_types_vec.size() * sizeof(cudf::type_id),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(scalars_left,
			left_scalars_host.data(),
			left_scalars_host.size() * sizeof(int64_t),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaMemcpyAsync(scalars_right,
			right_scalars_host.data(),
			right_scalars_host.size() * sizeof(int64_t),
			cudaMemcpyHostToDevice,
			stream));

		CUDA_TRY(cudaStreamSynchronize(stream));
	}

	__device__ __forceinline__ void operator()(
		cudf::size_type row_index, int64_t total_buffer[], cudf::size_type size) {
		int64_t * valids_in_buffer =
			temp_valids_in_buffer + (blockIdx.x * blockDim.x + threadIdx.x) * this->num_columns;
		int64_t * valids_out_buffer =
			temp_valids_out_buffer + (blockIdx.x * blockDim.x + threadIdx.x) * this->num_final_outputs;

		for(column_index_type cur_column = 0; cur_column < this->num_columns; cur_column++) {
			if(this->valid_ptrs[cur_column] == nullptr || this->null_counts_inputs[cur_column] == 0) {
				valids_in_buffer[cur_column] = UNARY_INDEX;
			} else {
				read_valid_data(cur_column, valids_in_buffer, row_index);
			}
		}

		int64_t cur_row_valids;
		for(cudf::size_type row = 0; row < 64 && row_index + row < size; row++) {
			load_cur_row_valids(valids_in_buffer, row, cur_row_valids, this->num_columns);

			for(int16_t cur_column = 0; cur_column < this->num_columns; cur_column++) {
				read_data(cur_column, total_buffer, row_index + row);
			}

			for(int16_t op_index = 0; op_index < this->num_operations; op_index++) {
				process_operator(op_index, total_buffer, row_index + row, cur_row_valids);
			}

			for(int out_index = 0; out_index < this->num_final_outputs; out_index++) {
				write_data(out_index, this->final_output_positions[out_index], total_buffer, row_index + row);
			}

			copy_row_valids_into_buffer(cur_row_valids, valids_out_buffer, row);
		}

		// write out valids here
		copy_row_valids_Into_global(valids_out_buffer, row_index);
	}

	static size_t get_temp_size(int num_inputs, int num_operations, int num_final_outputs) {
		size_t space = 0;
		space += sizeof(void *) * num_inputs;  // space for array of pointers to column data
		space += sizeof(void *) * num_final_outputs;
		space += sizeof(temp_gdf_valid_type *) * num_inputs;  // space for array of pointers  to column validity
															  // bitmasks
		space += sizeof(temp_gdf_valid_type *) * num_final_outputs;
		space += sizeof(cudf::type_id) * num_inputs;	  // space for pointers to types for of input_columns
		space += 3 * (sizeof(int16_t) * num_operations);  // space for pointers to indexes to columns e.g. left_input
														  // index, right_input index and output_index
		space += 3 * (sizeof(cudf::type_id) *
						 num_operations);  // space for pointers to types for each input_index, e.g. if input_index = 1
										   // then this value should contain column_1 type
		space += (sizeof(int16_t) * num_final_outputs);  // space for pointers to indexes to columns e.g. left_input
														 // index, right_input index and output_index
		space += (sizeof(cudf::type_id) *
				  num_final_outputs);  // space for pointers to types for each input_index, e.g. if
									   // input_index = 1 then this value should contain column_1 type
		space += sizeof(gdf_binary_operator_exp) * num_operations;
		space += sizeof(gdf_unary_operator) * num_operations;
		space += sizeof(cudf::size_type) * num_inputs;		   // space for null_counts_inputs
		space += sizeof(cudf::size_type) * num_final_outputs;  // space for null count outputs
		space += sizeof(int64_t) * num_operations * 2;		   // space for scalar inputs
		return space;
	}

private:
	__device__ __forceinline__ void copy_row_valids_into_buffer(
		int64_t row_valid, int64_t * valids_out_buffer, const cudf::size_type & row_index) {
		for(column_index_type out_index = 0; out_index < this->num_final_outputs; out_index++) {
			if(this->valid_ptrs_out[out_index] != nullptr) {
				setColumnValid(valids_out_buffer[out_index],
					row_index,
					getColumnValid(row_valid, this->final_output_positions[out_index]));
			}
		}
	}

	__device__ __forceinline__ void copy_row_valids_Into_global(
		int64_t * valids_out_buffer, const cudf::size_type & row_index) {
		for(column_index_type out_index = 0; out_index < this->num_final_outputs; out_index++) {
			if(this->valid_ptrs_out[out_index] != nullptr) {
				write_valid_data(out_index, valids_out_buffer[out_index], row_index);
			}
		}
	}

	__device__ __forceinline__ void load_cur_row_valids(
		int64_t valids_in_buffer[], cudf::size_type row, int64_t & cur_row_valids, column_index_type num_columns) {
		for(column_index_type cur_column = 0; cur_column < num_columns; cur_column++) {
			setColumnValid(cur_row_valids, cur_column, getColumnValid(valids_in_buffer[cur_column], row));
		}
	}

	/**
	 * @param buffer the local buffer which storse the information that is to be processed
	 * @param position the position in the local buffer where this data needs to be written
	 */
	template <typename LocalStorageType>
	__device__ __forceinline__ LocalStorageType get_data_from_buffer(int64_t * buffer, int position) {
		return *((LocalStorageType *) (buffer + ((position * threadBlockSize) + threadIdx.x)));
	}

	template <typename LocalStorageType>
	__device__ __forceinline__ void store_data_in_buffer(LocalStorageType data, int64_t * buffer, int position) {
		*((LocalStorageType *) (buffer + ((position * threadBlockSize) + threadIdx.x))) = data;
	}

	/**
	 * @param buffer the local buffer which storse the information that is to be processed
	 */
	template <typename ColType, typename LocalStorageType>
	__device__ __forceinline__ void device_ptr_read_into_buffer(column_index_type col_index,
		cudf::size_type row,
		const void * const * columns,
		int64_t * buffer,
		int position) {
		const ColType * col_data = static_cast<const ColType *>((columns[col_index]));
		*((LocalStorageType *) (buffer + ((position * threadBlockSize) + threadIdx.x))) =
			(LocalStorageType) __ldg(((ColType *) &col_data[row]));
	}

	/**
	 * @param buffer the local buffer which storse the information that is to be processed
	 */
	template <typename ColType, typename LocalStorageType>
	__device__ __forceinline__ void device_ptr_write_from_buffer(
		cudf::size_type row, void * columns, int64_t * buffer, int position) {
		const LocalStorageType col_data =
			*((LocalStorageType *) (buffer + ((position * threadBlockSize) + threadIdx.x)));
		((ColType *) columns)[row] = (ColType) col_data;
	}

	__device__ __forceinline__ void write_data(
		int cur_column, int cur_buffer, int64_t * buffer, cudf::size_type row_index) {
		cudf::type_id cur_type = this->final_output_types[cur_column];

		if(cur_type == cudf::type_id::BOOL8) {
			device_ptr_write_from_buffer<cudf::experimental::bool8::value_type, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::INT8) {
			device_ptr_write_from_buffer<int8_t, int64_t>(row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::INT16) {
			device_ptr_write_from_buffer<int16_t, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::INT32 || cur_type == cudf::type_id::CATEGORY) {
			device_ptr_write_from_buffer<int32_t, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::INT64) {
			device_ptr_write_from_buffer<int64_t, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::FLOAT32) {
			device_ptr_write_from_buffer<float, double>(row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::FLOAT64) {
			device_ptr_write_from_buffer<double, double>(row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::TIMESTAMP_DAYS) {
			device_ptr_write_from_buffer<cudf::timestamp_D::rep, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::TIMESTAMP_SECONDS) {
			device_ptr_write_from_buffer<cudf::timestamp_s::rep, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			device_ptr_write_from_buffer<cudf::timestamp_ms::rep, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::TIMESTAMP_MICROSECONDS) {
			device_ptr_write_from_buffer<cudf::timestamp_us::rep, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		} else if(cur_type == cudf::type_id::TIMESTAMP_NANOSECONDS) {
			device_ptr_write_from_buffer<cudf::timestamp_ns::rep, int64_t>(
				row_index, this->output_data[cur_column], buffer, cur_buffer);
		}
	}

	// TODO: a clever person would make this something that gets passed into this function so that we can do more clever
	// things than just read data from some grumpy old buffer, like read in from another one of these that does read
	// from a boring buffer, or from some perumtation iterartor hmm in fact if it could read permuted data you could
	// avoi dmaterializing intermeidate filter steps
	__device__ __forceinline__ void read_data(int cur_column, int64_t * buffer, cudf::size_type row_index) {
		cudf::type_id cur_type = this->input_column_types[cur_column];

		if(cur_type == cudf::type_id::BOOL8) {
			device_ptr_read_into_buffer<cudf::experimental::bool8::value_type, int64_t>(
				cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::INT8) {
			device_ptr_read_into_buffer<int8_t, int64_t>(cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::INT16) {
			device_ptr_read_into_buffer<int16_t, int64_t>(cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::INT32 || cur_type == cudf::type_id::CATEGORY) {
			device_ptr_read_into_buffer<int32_t, int64_t>(cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::INT64) {
			device_ptr_read_into_buffer<int64_t, int64_t>(cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::FLOAT32) {
			device_ptr_read_into_buffer<float, double>(cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::FLOAT64) {
			device_ptr_read_into_buffer<double, double>(cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::TIMESTAMP_DAYS) {
			device_ptr_read_into_buffer<cudf::timestamp_D::rep, int64_t>(
				cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::TIMESTAMP_SECONDS) {
			device_ptr_read_into_buffer<cudf::timestamp_s::rep, int64_t>(
				cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			device_ptr_read_into_buffer<cudf::timestamp_ms::rep, int64_t>(
				cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::TIMESTAMP_MICROSECONDS) {
			device_ptr_read_into_buffer<cudf::timestamp_us::rep, int64_t>(
				cur_column, row_index, this->column_data, buffer, cur_column);
		} else if(cur_type == cudf::type_id::TIMESTAMP_NANOSECONDS) {
			device_ptr_read_into_buffer<cudf::timestamp_ns::rep, int64_t>(
				cur_column, row_index, this->column_data, buffer, cur_column);
		}
	}

	__device__ __forceinline__ void read_valid_data(int cur_column, int64_t * buffer, cudf::size_type row_index) {
		int64_t * valid_in = (int64_t *) this->valid_ptrs[cur_column];
		buffer[cur_column] = valid_in[row_index / 64];
	}

	__device__ __forceinline__ void write_valid_data(int out_index, int64_t valid_data, cudf::size_type row_index) {
		int64_t * valid_out = (int64_t *) this->valid_ptrs_out[out_index];
		valid_out[row_index / 64] = valid_data;
	}

	__device__ __forceinline__ bool getColumnValid(int64_t row_valid, int cur_column) {
		return (row_valid >> cur_column) & 1;
	}
	__device__ __forceinline__ void setColumnValid(int64_t & row_valid, int cur_column, bool value) {
		// TODO: careful you might have to cast bool value to int64_t for this to actually work
		row_valid ^= ((-value) ^ row_valid) & (1UL << cur_column);
	}

	template <typename InputType,
		typename OutputType,
		typename std::enable_if_t<std::is_floating_point<InputType>::value> * = nullptr>
	__device__ __forceinline__ OutputType cast_op(InputType value) {
		return (OutputType) round(value);
	}

	template <typename InputType,
		typename OutputType,
		typename std::enable_if_t<!std::is_floating_point<InputType>::value> * = nullptr>
	__device__ __forceinline__ OutputType cast_op(InputType value) {
		return (OutputType) value;
	}

	__device__ __forceinline__ void process_operator(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, int64_t & row_valids) {
		cudf::type_id type = this->input_types_left[op_index];
		if(isFloat(type)) {
			process_operator_1<double>(op_index, buffer, row_index, row_valids);
		} else {
			process_operator_1<int64_t>(op_index, buffer, row_index, row_valids);
		}
	}

	template <typename LeftType>
	__device__ __forceinline__ void process_operator_1(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, int64_t & row_valids) {
		cudf::type_id type = this->input_types_right[op_index];
		if(isFloat(type)) {
			process_operator_2<LeftType, double>(op_index, buffer, row_index, row_valids);
		} else {
			process_operator_2<LeftType, int64_t>(op_index, buffer, row_index, row_valids);
		}
	}

	template <typename LeftType, typename RightType>
	__device__ __forceinline__ void process_operator_2(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, int64_t & row_valids) {
		cudf::type_id type = this->output_types[op_index];
		if(isFloat(type)) {
			process_operator_3<LeftType, RightType, double>(op_index, buffer, row_index, row_valids);
		} else {
			process_operator_3<LeftType, RightType, int64_t>(op_index, buffer, row_index, row_valids);
		}
	}

	template <typename LeftType, typename RightType, typename OutputTypeOperator>
	__device__ __forceinline__ void process_operator_3(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, int64_t & row_valids) {
		column_index_type right_position = this->right_input_positions[op_index];
		column_index_type left_position = this->left_input_positions[op_index];

		bool left_valid;
		bool right_valid;

		column_index_type output_position = this->output_positions[op_index];
		if(right_position != UNARY_INDEX) {
			// binary op
			LeftType left_value;

			if(left_position == SCALAR_INDEX) {
				left_value = ((LeftType *) this->scalars_left)[op_index];
				left_valid = true;
			} else if(left_position >= 0) {
				left_value = get_data_from_buffer<LeftType>(buffer, left_position);
				left_valid = getColumnValid(row_valids, left_position);
			} else if(left_position == SCALAR_NULL_INDEX) {
				// left is null do whatever
				left_valid = false;
			}

			RightType right_value;
			if(right_position == SCALAR_INDEX) {
				right_value = ((RightType *) this->scalars_right)[op_index];
				right_valid = true;
			} else if(right_position == SCALAR_NULL_INDEX) {
				right_valid = false;
			} else if(right_position >= 0) {
				right_value = get_data_from_buffer<RightType>(buffer, right_position);
				right_valid = getColumnValid(row_valids, right_position);
			}

			if(this->binary_operations[op_index] == GDF_COALESCE) {
				setColumnValid(row_valids, output_position, left_valid || right_valid);
			} else {
				setColumnValid(row_valids, output_position, left_valid && right_valid);
			}

			gdf_binary_operator_exp oper = this->binary_operations[op_index];
			if(oper == BLZ_ADD) {
				store_data_in_buffer<OutputTypeOperator>(left_value + right_value, buffer, output_position);
			} else if(oper == BLZ_SUB) {
				store_data_in_buffer<OutputTypeOperator>(left_value - right_value, buffer, output_position);
			} else if(oper == BLZ_MUL) {
				store_data_in_buffer<OutputTypeOperator>(left_value * right_value, buffer, output_position);
			} else if(oper == BLZ_DIV || oper == BLZ_FLOOR_DIV) {
				store_data_in_buffer<OutputTypeOperator>(left_value / right_value, buffer, output_position);
			} else if(oper == BLZ_COALESCE) {
				if(left_valid) {
					store_data_in_buffer<OutputTypeOperator>(left_value, buffer, output_position);
				} else {
					store_data_in_buffer<OutputTypeOperator>(right_value, buffer, output_position);
				}
			}

			/*else if(oper == BLZ_TRUE_DIV){
				//TODO: snap this requires understanding of the bitmask
			}*/
			else if(oper == BLZ_MOD) {
				// mod only makes sense with integer inputs
				store_data_in_buffer<OutputTypeOperator>(
					(int64_t) left_value % (int64_t) right_value, buffer, output_position);
			} else if(oper == BLZ_POW) {
				// oh god this is where it breaks if we are floats e do one thing
				OutputTypeOperator data = 1;
				if(isFloat((cudf::type_id) __ldg((int32_t *) &this->input_types_left[op_index])) ||
					isFloat((cudf::type_id) __ldg((int32_t *) &this->input_types_right[op_index]))) {
					data = pow((double) left_value, (double) right_value);

				} else {
					// there is no pow for ints, so lets just do it...

					LeftType base = left_value;
					// right type is the exponent
					for(int i = 0; i < right_value; i++) {
						data *= base;
					}
				}
				store_data_in_buffer<OutputTypeOperator>(data, buffer, output_position);
			} else if(oper == BLZ_EQUAL) {
				store_data_in_buffer<OutputTypeOperator>(left_value == right_value, buffer, output_position);
			} else if(oper == BLZ_NOT_EQUAL) {
				store_data_in_buffer<OutputTypeOperator>(left_value != right_value, buffer, output_position);
			} else if(oper == BLZ_LESS) {
				store_data_in_buffer<OutputTypeOperator>(left_value < right_value, buffer, output_position);
			} else if(oper == BLZ_GREATER) {
				store_data_in_buffer<OutputTypeOperator>(left_value > right_value, buffer, output_position);
			} else if(oper == BLZ_LESS_EQUAL) {
				store_data_in_buffer<OutputTypeOperator>(left_value <= right_value, buffer, output_position);
			} else if(oper == BLZ_GREATER_EQUAL) {
				store_data_in_buffer<OutputTypeOperator>(left_value >= right_value, buffer, output_position);
			} else if(oper == BLZ_LOGICAL_OR) {
				if(left_valid && right_valid) {
					store_data_in_buffer<OutputTypeOperator>(left_value || right_value, buffer, output_position);
				} else if(left_valid) {
					store_data_in_buffer<OutputTypeOperator>(left_value, buffer, output_position);
				} else {
					store_data_in_buffer<OutputTypeOperator>(right_value, buffer, output_position);
				}

			} else if(oper == BLZ_MAGIC_IF_NOT) {
				if(left_valid && left_value) {
					store_data_in_buffer<OutputTypeOperator>(right_value, buffer, output_position);
					setColumnValid(row_valids, output_position, right_valid);
				} else {
					// we want to indicate to first_non_magic to use the second value
					store_data_in_buffer<OutputTypeOperator>(
						getMagicNumber<OutputTypeOperator>(), buffer, output_position);
				}

			} else if(oper == BLZ_FIRST_NON_MAGIC) {
				if(left_value == getMagicNumber<OutputTypeOperator>()) {
					store_data_in_buffer<OutputTypeOperator>(right_value, buffer, output_position);
					setColumnValid(row_valids, output_position, right_valid);
				} else {
					store_data_in_buffer<OutputTypeOperator>(left_value, buffer, output_position);
					setColumnValid(row_valids, output_position, left_valid);
				}

			} else if(oper == BLZ_STR_LIKE || oper == BLZ_STR_SUBSTRING || oper == BLZ_STR_CONCAT) {
				store_data_in_buffer<OutputTypeOperator>(left_value, buffer, output_position);
				setColumnValid(row_valids, output_position, left_valid);
			}
		} else {
			// unary op
			gdf_unary_operator oper = this->unary_operations[op_index];

			LeftType left_value;
			bool left_valid;

			if(left_position == SCALAR_INDEX) {
				left_value = ((LeftType *) this->scalars_left)[op_index];
				left_valid = true;
			} else if(left_position >= 0) {
				left_value = get_data_from_buffer<LeftType>(buffer, left_position);
				left_valid = getColumnValid(row_valids, left_position);
			} else if(left_position == SCALAR_NULL_INDEX) {
				// left is null do whatever
				left_valid = false;
			}

			OutputTypeOperator computed = left_value;
			if(oper == BLZ_FLOOR) {
				computed = floor(left_value);
			} else if(oper == BLZ_CEIL) {
				computed = ceil(left_value);
			} else if(oper == BLZ_SIN) {
				computed = sin(left_value);
			} else if(oper == BLZ_COS) {
				computed = cos(left_value);
			} else if(oper == BLZ_ASIN) {
				computed = asin(left_value);
			} else if(oper == BLZ_ACOS) {
				computed = acos(left_value);
			} else if(oper == BLZ_TAN) {
				computed = tan(left_value);
			} else if(oper == BLZ_COTAN) {
				computed = cos(left_value) / sin(left_value);
			} else if(oper == BLZ_ATAN) {
				computed = atan(left_value);
			} else if(oper == BLZ_ABS) {
				computed = fabs(left_value);
			} else if(oper == BLZ_NOT) {
				computed = !left_value;
			} else if(oper == BLZ_LN) {
				computed = log(left_value);
			} else if(oper == BLZ_LOG) {
				computed = log10(left_value);
			} else if(oper == BLZ_YEAR) {
				cudf::type_id typeId = static_cast<cudf::type_id>(__ldg((int32_t *) &this->input_types_left[op_index]));
				computed = cudf::experimental::type_dispatcher(cudf::data_type{typeId},
					launch_extract_component<LeftType, datetime_component::YEAR>{},
					left_value);
			} else if(oper == BLZ_MONTH) {
				cudf::type_id typeId = static_cast<cudf::type_id>(__ldg((int32_t *) &this->input_types_left[op_index]));
				computed = cudf::experimental::type_dispatcher(cudf::data_type{typeId},
					launch_extract_component<LeftType, datetime_component::MONTH>{},
					left_value);
			} else if(oper == BLZ_DAY) {
				cudf::type_id typeId = static_cast<cudf::type_id>(__ldg((int32_t *) &this->input_types_left[op_index]));
				computed = cudf::experimental::type_dispatcher(
					cudf::data_type{typeId}, launch_extract_component<LeftType, datetime_component::DAY>{}, left_value);
			} else if(oper == BLZ_HOUR) {
				cudf::type_id typeId = static_cast<cudf::type_id>(__ldg((int32_t *) &this->input_types_left[op_index]));
				computed = cudf::experimental::type_dispatcher(cudf::data_type{typeId},
					launch_extract_component<LeftType, datetime_component::HOUR>{},
					left_value);
			} else if(oper == BLZ_MINUTE) {
				cudf::type_id typeId = static_cast<cudf::type_id>(__ldg((int32_t *) &this->input_types_left[op_index]));
				computed = cudf::experimental::type_dispatcher(cudf::data_type{typeId},
					launch_extract_component<LeftType, datetime_component::MINUTE>{},
					left_value);
			} else if(oper == BLZ_SECOND) {
				cudf::type_id typeId = static_cast<cudf::type_id>(__ldg((int32_t *) &this->input_types_left[op_index]));
				computed = cudf::experimental::type_dispatcher(cudf::data_type{typeId},
					launch_extract_component<LeftType, datetime_component::SECOND>{},
					left_value);
			} else if(oper == BLZ_CAST_INTEGER || oper == BLZ_CAST_BIGINT) {
				computed = cast_op<LeftType, OutputTypeOperator>(left_value);
			} else if(oper == BLZ_CAST_FLOAT || oper == BLZ_CAST_DOUBLE || oper == BLZ_CAST_DATE ||
					  oper == BLZ_CAST_TIMESTAMP || oper == BLZ_CAST_VARCHAR) {
				computed = left_value;
			} else if(oper == BLZ_IS_NULL) {
				computed = !left_valid;
			} else if(oper == BLZ_IS_NOT_NULL) {
				computed = left_valid;
			}

			if(oper == BLZ_IS_NULL || oper == BLZ_IS_NOT_NULL) {
				setColumnValid(row_valids, output_position, true);
			} else {
				setColumnValid(row_valids, output_position, left_valid);
			}
			store_data_in_buffer<OutputTypeOperator>(computed, buffer, output_position);
		}
	}

private:
	cudf::size_type num_columns;
	cudf::size_type num_final_outputs;

	void ** column_data;  // these are device side pointers to the device pointer found in gdf_column.data
	void ** output_data;
	temp_gdf_valid_type ** valid_ptrs;
	temp_gdf_valid_type ** valid_ptrs_out;

	cudf::type_id * input_column_types;
	column_index_type * left_input_positions;
	column_index_type * right_input_positions;
	column_index_type * output_positions;
	column_index_type * final_output_positions;  // should be same size as output_data, e.g. num_outputs

	int num_operations;
	cudf::type_id * input_types_left;
	cudf::type_id * input_types_right;
	cudf::type_id * output_types;
	cudf::type_id * final_output_types;
	gdf_binary_operator_exp * binary_operations;
	gdf_unary_operator * unary_operations;
	int64_t * scalars_left;  // if these scalars are not of invalid type we use them instead of input positions
	int64_t * scalars_right;

	cudf::size_type * null_counts_inputs;
	cudf::size_type * null_counts_outputs;

	int64_t * temp_valids_in_buffer;
	int64_t * temp_valids_out_buffer;

	int threadBlockSize;
};

// TODO: consider running valids at the same time as the normal
// operations to increase throughput
__global__ void transformKernel(InterpreterFunctor op, cudf::size_type size) {
	extern __shared__ int64_t total_buffer[];

	for(cudf::size_type i = (blockIdx.x * blockDim.x + threadIdx.x) * 64; i < size; i += blockDim.x * gridDim.x * 64) {
		op(i, total_buffer, size);
	}
}

} // namespace interops
