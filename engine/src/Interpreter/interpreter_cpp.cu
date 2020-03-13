#include <cudf/column/column_device_view.cuh>
#include <cudf/column/column_view.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_device_view.cuh>
#include <cudf/table/table_view.hpp>
#include <cudf/table/table_device_view.cuh>
#include <cudf/utilities/error.hpp>
#include <algorithm>
#include <stack>
#include <vector>
#include <map>
#include <regex>
#include <functional>
#include <rmm/rmm.h>

#include "CalciteExpressionParsing.h"
#include "interpreter_cpp.h"
#include "interpreter_ops.cuh"
#include "Traits/RuntimeTraits.h"

namespace interops {
namespace detail {

struct allocate_device_scalar {
	using scalar_device_ptr = typename std::unique_ptr<cudf::detail::scalar_device_view_base, std::function<void(cudf::detail::scalar_device_view_base*)>>;

	template <typename T, std::enable_if_t<cudf::is_simple<T>()> * = nullptr>
	scalar_device_ptr operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		using ScalarDeviceType = cudf::experimental::scalar_device_type_t<T>;

		ScalarDeviceType * ret = nullptr;
		RMM_TRY(RMM_ALLOC(&ret, sizeof(ScalarDeviceType), stream));

		auto typed_scalar_ptr = static_cast<ScalarType *>(&s);
		ScalarDeviceType h_scalar{typed_scalar_ptr->type(), typed_scalar_ptr->data(), typed_scalar_ptr->validity_data()};

    CUDA_TRY(cudaMemcpyAsync(ret, &h_scalar, sizeof(ScalarDeviceType), cudaMemcpyDefault, stream));

		auto deleter = [stream](cudf::detail::scalar_device_view_base * p) { RMM_TRY(RMM_FREE(p, stream)); };
		return {ret, deleter};
	}

	template <typename T, std::enable_if_t<std::is_same<T, cudf::string_view>::value> * = nullptr>
	scalar_device_ptr operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		using ScalarDeviceType = cudf::experimental::scalar_device_type_t<T>;

		ScalarDeviceType * ret = nullptr;
		RMM_TRY(RMM_ALLOC(&ret, sizeof(ScalarDeviceType), stream));

		auto typed_scalar_ptr = static_cast<ScalarType *>(&s);
		ScalarDeviceType h_scalar{typed_scalar_ptr->type(), typed_scalar_ptr->data(), typed_scalar_ptr->validity_data(), typed_scalar_ptr->size()};

		CUDA_TRY(cudaMemcpyAsync(ret, &h_scalar, sizeof(ScalarDeviceType), cudaMemcpyDefault, stream));

		auto deleter = [stream](cudf::detail::scalar_device_view_base * p) { RMM_TRY(RMM_FREE(p, stream)); };
		return {ret, deleter};
	}

	template <typename T, std::enable_if_t<std::is_same<T, cudf::dictionary32>::value> * = nullptr>
	scalar_device_ptr operator()(cudf::scalar & s, cudaStream_t stream = 0) {
		RAL_FAIL("Dictionary not yet supported");
		return nullptr;
	}
};

template <int SIZE, int REGISTER_SIZE>
int calculated_shared_memory(int num_threads_per_block) {
	return SIZE * num_threads_per_block * REGISTER_SIZE;
}

// TODO: we dont know if this is fast or not we coudl store this in a pre computed map
void calculate_grid(int * min_grid_size, int * block_size, column_index_type max_output) {
	if(max_output == 1) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<1, 8>, 0));
	} else if(max_output == 2) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<2, 8>, 0));
	} else if(max_output == 3) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<3, 8>, 0));
	} else if(max_output == 4) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<4, 8>, 0));
	} else if(max_output == 5) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<5, 8>, 0));
	} else if(max_output == 6) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<6, 8>, 0));
	} else if(max_output == 7) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<7, 8>, 0));
	} else if(max_output == 8) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<8, 8>, 0));
	} else if(max_output == 9) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<9, 8>, 0));
	} else if(max_output == 10) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<10, 8>, 0));
	} else if(max_output == 11) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<11, 8>, 0));
	} else if(max_output == 12) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<12, 8>, 0));
	} else if(max_output == 13) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<13, 8>, 0));
	} else if(max_output == 14) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<14, 8>, 0));
	} else if(max_output == 15) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<15, 8>, 0));
	} else if(max_output == 16) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<16, 8>, 0));
	} else if(max_output == 17) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<17, 8>, 0));
	} else if(max_output == 18) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<18, 8>, 0));
	} else if(max_output == 19) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<19, 8>, 0));
	} else if(max_output == 20) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<20, 8>, 0));
	} else if(max_output == 21) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<21, 8>, 0));
	} else if(max_output == 22) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<22, 8>, 0));
	} else if(max_output == 23) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<23, 8>, 0));
	} else if(max_output == 24) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<24, 8>, 0));
	} else if(max_output == 25) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<25, 8>, 0));
	} else if(max_output == 26) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<26, 8>, 0));
	} else if(max_output == 27) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<27, 8>, 0));
	} else if(max_output == 28) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<28, 8>, 0));
	} else if(max_output == 29) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<29, 8>, 0));
	} else if(max_output == 30) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<30, 8>, 0));
	} else if(max_output == 31) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<31, 8>, 0));
	} else if(max_output == 32) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<32, 8>, 0));
	} else if(max_output == 33) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<33, 8>, 0));
	} else if(max_output == 34) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<34, 8>, 0));
	} else if(max_output == 35) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<35, 8>, 0));
	} else if(max_output == 36) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<36, 8>, 0));
	} else if(max_output == 37) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<37, 8>, 0));
	} else if(max_output == 38) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<38, 8>, 0));
	} else if(max_output == 39) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<39, 8>, 0));
	} else if(max_output == 40) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<40, 8>, 0));
	} else if(max_output == 41) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<41, 8>, 0));
	} else if(max_output == 42) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<42, 8>, 0));
	} else if(max_output == 43) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<43, 8>, 0));
	} else if(max_output == 44) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<44, 8>, 0));
	} else if(max_output == 45) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<45, 8>, 0));
	} else if(max_output == 46) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<46, 8>, 0));
	} else if(max_output == 47) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<47, 8>, 0));
	} else if(max_output == 48) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<48, 8>, 0));
	} else if(max_output == 49) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<49, 8>, 0));
	} else if(max_output == 50) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<50, 8>, 0));
	} else if(max_output == 51) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<51, 8>, 0));
	} else if(max_output == 52) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<52, 8>, 0));
	} else if(max_output == 53) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<53, 8>, 0));
	} else if(max_output == 54) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<54, 8>, 0));
	} else if(max_output == 55) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<55, 8>, 0));
	} else if(max_output == 56) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<56, 8>, 0));
	} else if(max_output == 57) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<57, 8>, 0));
	} else if(max_output == 58) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<58, 8>, 0));
	} else if(max_output == 59) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<59, 8>, 0));
	} else if(max_output == 60) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<60, 8>, 0));
	} else if(max_output == 61) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<61, 8>, 0));
	} else if(max_output == 62) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<62, 8>, 0));
	} else if(max_output == 63) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<63, 8>, 0));
	} else if(max_output == 64) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<64, 8>, 0));
	}
}

struct operand_position {
	column_index_type position;
	std::string token;
};

column_index_type get_first_open_position(std::vector<bool> & open_positions, cudf::size_type start_position) {
	assert(open_positions.size() <= std::numeric_limits<column_index_type>().max());

	for(size_t i = start_position; i < open_positions.size(); i++) {
		if(open_positions[i]) {
			open_positions[i] = false;
			return static_cast<column_index_type>(i);
		}
	}
	return -1;
}

}  // namespace detail

bool is_unary_operator(operator_type op) {
	switch (op)
	{
	case operator_type::BLZ_NOT:
	case operator_type::BLZ_ABS:
	case operator_type::BLZ_FLOOR:
	case operator_type::BLZ_CEIL:
	case operator_type::BLZ_SIN:
	case operator_type::BLZ_COS:
	case operator_type::BLZ_ASIN:
	case operator_type::BLZ_ACOS:
	case operator_type::BLZ_TAN:
	case operator_type::BLZ_COTAN:
	case operator_type::BLZ_ATAN:
	case operator_type::BLZ_LN:
	case operator_type::BLZ_LOG:
	case operator_type::BLZ_YEAR:
	case operator_type::BLZ_MONTH:
	case operator_type::BLZ_DAY:
	case operator_type::BLZ_HOUR:
	case operator_type::BLZ_MINUTE:
	case operator_type::BLZ_SECOND:
	case operator_type::BLZ_IS_NULL:
	case operator_type::BLZ_IS_NOT_NULL:
	case operator_type::BLZ_CAST_INTEGER:
	case operator_type::BLZ_CAST_BIGINT:
	case operator_type::BLZ_CAST_FLOAT:
	case operator_type::BLZ_CAST_DOUBLE:
	case operator_type::BLZ_CAST_DATE:
	case operator_type::BLZ_CAST_TIMESTAMP:
	case operator_type::BLZ_CAST_VARCHAR:
		return true;
	default:
		return false;
	}
}

bool is_binary_operator(operator_type op) {
	switch (op)
	{
  case operator_type::BLZ_ADD:
  case operator_type::BLZ_SUB:
  case operator_type::BLZ_MUL:
  case operator_type::BLZ_DIV:
  case operator_type::BLZ_MOD:
  case operator_type::BLZ_POW:
  case operator_type::BLZ_ROUND:
  case operator_type::BLZ_EQUAL:
  case operator_type::BLZ_NOT_EQUAL:
  case operator_type::BLZ_LESS:
  case operator_type::BLZ_GREATER:
  case operator_type::BLZ_LESS_EQUAL:
  case operator_type::BLZ_GREATER_EQUAL:
  case operator_type::BLZ_BITWISE_AND:
  case operator_type::BLZ_BITWISE_OR:
  case operator_type::BLZ_BITWISE_XOR:
  case operator_type::BLZ_LOGICAL_AND:
  case operator_type::BLZ_LOGICAL_OR:
	case operator_type::BLZ_FIRST_NON_MAGIC:
	case operator_type::BLZ_MAGIC_IF_NOT:
	case operator_type::BLZ_STR_LIKE:
	case operator_type::BLZ_STR_SUBSTRING:
	case operator_type::BLZ_STR_CONCAT:
		return true;
	default:
		return false;
	}
}

cudf::type_id get_output_type(cudf::type_id input_left_type, operator_type op) {
	switch (op)
	{
	case operator_type::BLZ_CAST_INTEGER:
		return cudf::type_id::INT32;
	case operator_type::BLZ_CAST_BIGINT:
		return cudf::type_id::INT64;
	case operator_type::BLZ_CAST_FLOAT:
		return cudf::type_id::FLOAT32;
	case operator_type::BLZ_CAST_DOUBLE:
		return cudf::type_id::FLOAT64;
	case operator_type::BLZ_CAST_DATE:
		return cudf::type_id::TIMESTAMP_DAYS;
	case operator_type::BLZ_CAST_TIMESTAMP:
		return cudf::type_id::TIMESTAMP_NANOSECONDS;
	case operator_type::BLZ_CAST_VARCHAR:
		return cudf::type_id::STRING;
	case operator_type::BLZ_YEAR:
	case operator_type::BLZ_MONTH:
	case operator_type::BLZ_DAY:
	case operator_type::BLZ_HOUR:
	case operator_type::BLZ_MINUTE:
	case operator_type::BLZ_SECOND:
		return cudf::type_id::INT16;
	case operator_type::BLZ_SIN:
	case operator_type::BLZ_COS:
	case operator_type::BLZ_ASIN:
	case operator_type::BLZ_ACOS:
	case operator_type::BLZ_TAN:
	case operator_type::BLZ_COTAN:
	case operator_type::BLZ_ATAN:
	case operator_type::BLZ_LN:
	case operator_type::BLZ_LOG:
	case operator_type::BLZ_FLOOR:
	case operator_type::BLZ_CEIL:
		if(is_type_float(input_left_type)) {
			return input_left_type;
		} else {
			return cudf::type_id::FLOAT64;
		}
	case operator_type::BLZ_ABS:
		return input_left_type;
	case operator_type::BLZ_NOT:
	case operator_type::BLZ_IS_NULL:
	case operator_type::BLZ_IS_NOT_NULL:
		return cudf::type_id::BOOL8;
	default:
	 	assert(false);
		return cudf::type_id::EMPTY;
	}
}

cudf::type_id get_output_type(cudf::type_id input_left_type, cudf::type_id input_right_type, operator_type op) {
	switch (op)
	{
	case operator_type::BLZ_ADD:
	case operator_type::BLZ_SUB:
	case operator_type::BLZ_MUL:
	case operator_type::BLZ_DIV:
	case operator_type::BLZ_MOD:
		if(is_type_float(input_left_type) && is_type_float(input_right_type)) {
			return (ral::traits::get_dtype_size_in_bytes(input_left_type) >= ral::traits::get_dtype_size_in_bytes(input_right_type))
							? input_left_type
							: input_right_type;
		}	else if(is_type_float(input_left_type)) {
			return input_left_type;
		} else if(is_type_float(input_right_type)) {
			return input_right_type;
		} else {
			return (ral::traits::get_dtype_size_in_bytes(input_left_type) >= ral::traits::get_dtype_size_in_bytes(input_right_type))
							? input_left_type
							: input_right_type;
		}
  case operator_type::BLZ_EQUAL:
  case operator_type::BLZ_NOT_EQUAL:
  case operator_type::BLZ_LESS:
  case operator_type::BLZ_GREATER:
  case operator_type::BLZ_LESS_EQUAL:
  case operator_type::BLZ_GREATER_EQUAL:
	case operator_type::BLZ_LOGICAL_AND:
	case operator_type::BLZ_LOGICAL_OR:
		return cudf::type_id::BOOL8;
	case operator_type::BLZ_POW:
	case operator_type::BLZ_ROUND:
		return ral::traits::get_dtype_size_in_bytes(input_left_type) <= ral::traits::get_dtype_size_in_bytes(cudf::type_id::FLOAT32)
					 ? cudf::type_id::FLOAT32
					 : cudf::type_id::FLOAT64;
	case operator_type::BLZ_MAGIC_IF_NOT:
		return input_right_type;
	case operator_type::BLZ_FIRST_NON_MAGIC:
		return (ral::traits::get_dtype_size_in_bytes(input_left_type) >= ral::traits::get_dtype_size_in_bytes(input_right_type))
				   ? input_left_type
				   : input_right_type;
	case operator_type::BLZ_STR_LIKE:
		return cudf::type_id::BOOL8;
	case operator_type::BLZ_STR_SUBSTRING:
	case operator_type::BLZ_STR_CONCAT:
		return cudf::type_id::STRING;
	default:
		assert(false);
		return cudf::type_id::EMPTY;
	}
}

/**
 * Creates a physical plan for the expression that can be added to the total plan
 */
void add_expression_to_interpreter_plan(const std::vector<std::string> & tokenized_expression,
	const cudf::table_view & table,
	const std::map<column_index_type, column_index_type> & expr_idx_to_col_idx_map,
	cudf::size_type expression_position,
	cudf::size_type num_total_outputs,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,
	std::vector<operator_type> & operators,
	std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	std::vector<std::unique_ptr<cudf::scalar>> & right_scalars) {

	using namespace detail;

	cudf::size_type num_inputs = table.num_columns();
	std::vector<bool> processing_space_free(512, true);  // A place to store whether or not a processing space is occupied at any point in time
	cudf::size_type start_processing_position = num_inputs + num_total_outputs;
	std::fill_n(processing_space_free.begin(), start_processing_position, false);

	std::stack<operand_position> operand_stack;
	std::map<column_index_type, column_index_type> src_str_col_map;
	for(size_t i = 0; i < tokenized_expression.size(); i++) {
		const std::string & token = tokenized_expression[i];

		if(is_operator_token(token)) {
			operator_type operation = map_to_operator_type(token);
			if(is_binary_operator(operation)) {
				const std::string left_operand = operand_stack.top().token;
				if(!is_literal(left_operand)) {
					if(operand_stack.top().position >= start_processing_position) {
						processing_space_free[operand_stack.top().position] = true;
					}
				}
				operand_stack.pop();

				const std::string right_operand = operand_stack.top().token;
				if(!is_literal(right_operand)) {
					if(operand_stack.top().position >= start_processing_position) {
						processing_space_free[operand_stack.top().position] = true;
					}
				}
				operand_stack.pop();

				operators.push_back(operation);

				if(is_literal(left_operand) && is_literal(right_operand)) {
					RAL_FAIL("Operations between literals is not supported");
				} else if(is_literal(left_operand) ) {
					cudf::size_type right_index = get_index(right_operand);
					auto scalar_ptr = get_scalar_from_string(left_operand);

					left_inputs.push_back(scalar_ptr ? SCALAR_INDEX : SCALAR_NULL_INDEX);
					right_inputs.push_back(right_index);
					left_scalars.push_back(std::move(scalar_ptr));
					right_scalars.emplace_back(nullptr);
				} else if(is_literal(right_operand)) {
					cudf::size_type left_index = get_index(left_operand);
					auto scalar_ptr = get_scalar_from_string(right_operand);
					
					left_inputs.push_back(left_index);
					right_inputs.push_back(scalar_ptr ? SCALAR_INDEX : SCALAR_NULL_INDEX);
					left_scalars.emplace_back(nullptr);
					right_scalars.push_back(std::move(scalar_ptr));
				} else {
					cudf::size_type left_index = get_index(left_operand);
					cudf::size_type right_index = get_index(right_operand);

					left_inputs.push_back(left_index);
					right_inputs.push_back(right_index);
					left_scalars.emplace_back(nullptr);
					right_scalars.emplace_back(nullptr);
				}
			} else { // if(is_unary_operator_token(token))
				std::string left_operand = operand_stack.top().token;
				RAL_EXPECTS(!is_literal(left_operand), "Unary operations on literals is not supported");

				if(operand_stack.top().position >= start_processing_position) {
					processing_space_free[operand_stack.top().position] = true;
				}
				operand_stack.pop();

				operators.push_back(operation);

				size_t left_index = get_index(left_operand);
				left_inputs.push_back(left_index);
				right_inputs.push_back(UNARY_INDEX);
				left_scalars.emplace_back(nullptr);
				right_scalars.emplace_back(nullptr);
			}

			if(i == tokenized_expression.size() - 1) {
				// write to final output
				outputs.push_back(expression_position + num_inputs);
			} else {
				// write to temp output
				column_index_type output_position =	get_first_open_position(processing_space_free, start_processing_position);
				outputs.push_back(output_position);
				operand_stack.push({output_position, "$" + std::to_string(output_position)});
			}
		} else {
			if(is_literal(token)) {
				operand_stack.push({SCALAR_INDEX, token});
			} else {
				column_index_type mapped_idx = expr_idx_to_col_idx_map.at(get_index(token));
				operand_stack.push({mapped_idx, "$" + std::to_string(mapped_idx)});
			}
		}
	}
}

void perform_interpreter_operation(cudf::mutable_table_view & out_table,
	const cudf::table_view & table,
	const std::vector<column_index_type> & left_inputs,
	const std::vector<column_index_type> & right_inputs,
	const std::vector<column_index_type> & outputs,
	const std::vector<column_index_type> & final_output_positions,
	const std::vector<operator_type> & operators,
	const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars) {
	using namespace detail;
	cudaStream_t stream = 0;

	if (final_output_positions.empty())	{
		return;
	}
	
	assert(!left_inputs.empty());
	assert(!right_inputs.empty());
	assert(!outputs.empty());
	assert(!operators.empty());

	auto max_left_it = std::max_element(left_inputs.begin(), left_inputs.end());
	auto max_right_it = std::max_element(right_inputs.begin(), right_inputs.end());
	auto max_out_it = std::max_element(outputs.begin(), outputs.end());

	RAL_EXPECTS(std::max(std::max(*max_left_it, *max_right_it), *max_out_it) < 64, "Interops does not support plans with an input or output index greater than 63");

	column_index_type max_output = *max_out_it;

	size_t shared_memory_per_thread = (max_output + 1) * sizeof(int64_t);

	int min_grid_size, block_size;
	calculate_grid(&min_grid_size, &block_size, max_output + 1);

	size_t temp_valids_in_size = min_grid_size * block_size * table.num_columns() * sizeof(cudf::bitmask_type);
	size_t temp_valids_out_size = min_grid_size * block_size * final_output_positions.size() * sizeof(cudf::bitmask_type);
	rmm::device_buffer temp_device_valids_in_buffer(temp_valids_in_size, stream); 
	rmm::device_buffer temp_device_valids_out_buffer(temp_valids_out_size, stream); 

	// device table views
	auto device_table_view = cudf::table_device_view::create(table, stream);
	auto device_out_table_view = cudf::mutable_table_device_view::create(out_table, stream);

	// device scalar views
	using scalar_device_ptr = typename allocate_device_scalar::scalar_device_ptr;
	std::vector<scalar_device_ptr> left_device_scalars_ptrs;
	std::vector<cudf::detail::scalar_device_view_base *> left_device_scalars_raw;
	std::vector<scalar_device_ptr> right_device_scalars_ptrs;
	std::vector<cudf::detail::scalar_device_view_base *> right_device_scalars_raw;
	for (size_t i = 0; i < left_scalars.size(); i++) {
		left_device_scalars_ptrs.push_back(left_scalars[i] ? cudf::experimental::type_dispatcher(left_scalars[i]->type(), allocate_device_scalar{}, *(left_scalars[i])) : nullptr);
		left_device_scalars_raw.push_back(left_device_scalars_ptrs.back().get());

		right_device_scalars_ptrs.push_back(right_scalars[i] ? cudf::experimental::type_dispatcher(right_scalars[i]->type(), allocate_device_scalar{}, *(right_scalars[i])) : nullptr);
		right_device_scalars_raw.push_back(right_device_scalars_ptrs.back().get());
	}
	rmm::device_vector<cudf::detail::scalar_device_view_base *> left_device_scalars(left_device_scalars_raw);
	rmm::device_vector<cudf::detail::scalar_device_view_base *> right_device_scalars(right_device_scalars_raw);

	// device left, right and output types
	size_t num_operations = left_inputs.size();
	std::vector<cudf::type_id> left_input_types_vec(num_operations);
	std::vector<cudf::type_id> right_input_types_vec(num_operations);
	std::vector<cudf::type_id> output_types_vec(num_operations);
	std::map<column_index_type, cudf::type_id> output_map_type;
	for(size_t i = 0; i < num_operations; i++) {
		column_index_type left_index = left_inputs[i];
		column_index_type right_index = right_inputs[i];
		column_index_type output_index = outputs[i];

		if(left_index >= 0 && left_index < table.num_columns()) {
			left_input_types_vec[i] = table.column(left_index).type().id();
		} else if(left_index == SCALAR_NULL_INDEX) {
			left_input_types_vec[i] = cudf::type_id::EMPTY;
		} else if(left_index == SCALAR_INDEX) {
			left_input_types_vec[i] = left_scalars[i]->type().id();
		} else if(left_index == UNARY_INDEX) {
			// not possible
			assert(false);
		} else {
			// have to get it from the output that generated it
			left_input_types_vec[i] = output_map_type[left_index];
		}

		if(right_index >= 0 && right_index < table.num_columns()) {
			right_input_types_vec[i] = table.column(right_index).type().id();
		} else if(right_index == SCALAR_NULL_INDEX) {
			right_input_types_vec[i] = cudf::type_id::EMPTY;
		} else if(right_index == SCALAR_INDEX) {
			right_input_types_vec[i] = right_scalars[i]->type().id();
		} else if(right_index == UNARY_INDEX) {
			// wont be used its a unary operation
			right_input_types_vec[i] = cudf::type_id::EMPTY;
		} else {
			// have to get it from the output that generated it
			right_input_types_vec[i] = output_map_type[right_index];
		}

		cudf::type_id type_from_op = (right_index == UNARY_INDEX
																	? get_output_type(left_input_types_vec[i], operators[i])
																	: get_output_type(left_input_types_vec[i], right_input_types_vec[i], operators[i]));

		output_types_vec[i] = (is_type_float(type_from_op) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64);
		output_map_type[output_index] = output_types_vec[i];
	}
	rmm::device_vector<cudf::type_id> left_device_input_types(left_input_types_vec);
	rmm::device_vector<cudf::type_id> right_device_input_types(right_input_types_vec);
	rmm::device_vector<cudf::type_id> output_device_types(output_types_vec);

	rmm::device_vector<column_index_type> left_device_inputs(left_inputs);
	rmm::device_vector<column_index_type> right_device_inputs(right_inputs);
	rmm::device_vector<column_index_type> device_outputs(outputs);
	rmm::device_vector<column_index_type> final_device_output_positions(final_output_positions);
	rmm::device_vector<operator_type> device_operators(operators);


	InterpreterFunctor op(*device_out_table_view,
												*device_table_view,
												static_cast<cudf::size_type>(left_device_inputs.size()),
												left_device_inputs.data().get(),
												right_device_inputs.data().get(),
												device_outputs.data().get(),
												final_device_output_positions.data().get(),
												left_device_input_types.data().get(),
												right_device_input_types.data().get(),
												output_device_types.data().get(),
												device_operators.data().get(),
												left_device_scalars.data().get(),
												right_device_scalars.data().get(),
												temp_device_valids_in_buffer.data(),
												temp_device_valids_out_buffer.data());

	transformKernel<<<min_grid_size,
		block_size,
			// transformKernel<<<1
			// ,1,
		shared_memory_per_thread * block_size,
		stream>>>(op, table.num_rows());

	CUDA_TRY(cudaStreamSynchronize(stream));
}

}  // namespace interops
