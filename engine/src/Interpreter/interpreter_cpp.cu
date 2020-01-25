#include <cudf/column/column_device_view.cuh>
#include <cudf/column/column_view.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_device_view.cuh>
#include <cudf/table/table_view.hpp>
#include <cudf/table/table_device_view.cuh>
#include <cudf/utilities/error.hpp>
#include <algorithm>
#include <deque>
#include <vector>
#include <map>
#include <regex>
#include <exception>
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

	template <typename T, std::enable_if_t<cudf::is_compound<T>()> * = nullptr>
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

// std::unique_ptr<cudf::column> handle_cast_from_string(operator_type operation, const cudf::column_view& input_col) {
	// TODO percy cudf0.12 port to cudf::column and custrings
//	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
//	NVStrings * nv_strings =
//		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

//	cudf::type_id cast_type = get_output_type(cudf::type_id::CATEGORY, operation);

//	gdf_column_cpp new_input_col;
//	new_input_col.create_gdf_column(
//		cast_type, input_col->size, nullptr, ral::traits::get_dtype_size_in_bytes(cast_type));

//	switch(cast_type) {
//	case cudf::type_id::INT32: nv_strings->stoi(static_cast<int *>(new_input_col.data())); break;
//	case cudf::type_id::INT64: nv_strings->stol(static_cast<long *>(new_input_col.data())); break;
//	case cudf::type_id::FLOAT32: nv_strings->stof(static_cast<float *>(new_input_col.data())); break;
//	case cudf::type_id::FLOAT64: nv_strings->stod(static_cast<double *>(new_input_col.data())); break;
//	case cudf::type_id::TIMESTAMP_DAYS:
//		nv_strings->timestamp2long("%Y-%m-%d", NVStrings::days, static_cast<unsigned long *>(new_input_col.data()));
//		new_input_col.get_gdf_column()->dtype_info.time_unit = TIME_UNIT_NONE;
//		break;
//	// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
//	// percy this was not commented ... clean duplicated logic for timestamps
////	case cudf::type_id::TIMESTAMP_MILLISECONDS:
////		nv_strings->timestamp2long("%Y-%m-%d", NVStrings::ms, static_cast<unsigned long *>(new_input_col.data()));
////		new_input_col.get_gdf_column()->dtype_info.time_unit = TIME_UNIT_NONE;
////		break;
//	// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
//	case cudf::type_id::TIMESTAMP_MILLISECONDS:
//		// TODO: Should know when use TIME_UNIT_ns
//		nv_strings->timestamp2long(
//			"%Y-%m-%dT%H:%M:%SZ", NVStrings::ms, static_cast<unsigned long *>(new_input_col.data()));
//		new_input_col.get_gdf_column()->dtype_info.time_unit = TIME_UNIT_ms;
//		break;
//	default: assert(false);
//	}

//	NVStrings::destroy(nv_strings);

//	if(input_col->null_count) {
//		new_input_col.allocate_set_valid();
//		CheckCudaErrors(cudaMemcpy(new_input_col.valid(),
//			input_col->valid,
//			gdf_valid_allocation_size(new_input_col.size()),
//			cudaMemcpyDefault));
//		new_input_col.get_gdf_column()->null_count = input_col->null_count;
//	}

//	return new_input_col;
// }

// std::unique_ptr<cudf::column> handle_cast_to_string(const cudf::column_view & input_col) {
	// TODO percy cudf0.12 custrings this was not commented
//	NVStrings * nv_strings = nullptr;

//	if(input_col->size > 0) {
//		switch(to_type_id(input_col->dtype)) {
//		case cudf::type_id::INT32:
//			nv_strings = NVStrings::itos(static_cast<int *>(input_col->data), input_col->size, input_col->valid);
//			break;
//		case cudf::type_id::INT64:
//			nv_strings = NVStrings::ltos(static_cast<long *>(input_col->data), input_col->size, input_col->valid);
//			break;
//		case cudf::type_id::FLOAT32:
//			nv_strings = NVStrings::ftos(static_cast<float *>(input_col->data), input_col->size, input_col->valid);
//			break;
//		case cudf::type_id::FLOAT64:
//			nv_strings = NVStrings::dtos(static_cast<double *>(input_col->data), input_col->size, input_col->valid);
//			break;
//		case cudf::type_id::TIMESTAMP_DAYS:
//		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
//		// percy this was not commented
//		//case cudf::type_id::TIMESTAMP_SECONDS:
//			nv_strings = NVStrings::long2timestamp(static_cast<unsigned long *>(input_col->data),
//				input_col->size,
//				NVStrings::days,
//				"%Y-%m-%d",
//				input_col->valid);
//			break;
//		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
//		case cudf::type_id::TIMESTAMP_MILLISECONDS:
//			if(input_col->dtype_info.time_unit == TIME_UNIT_ns) {
//				nv_strings = NVStrings::long2timestamp(static_cast<unsigned long *>(input_col->data),
//					input_col->size,
//					NVStrings::ns,
//					"%Y-%m-%dT%H:%M:%SZ",
//					input_col->valid);
//			} else {
//				nv_strings = NVStrings::long2timestamp(static_cast<unsigned long *>(input_col->data),
//					input_col->size,
//					NVStrings::ms,
//					"%Y-%m-%dT%H:%M:%SZ",
//					input_col->valid);
//			}
//			break;
//		default: assert(false);
//		}
//	}

//	NVCategory * nv_category =
//		nv_strings ? NVCategory::create_from_strings(*nv_strings) : NVCategory::create_from_array(nullptr, 0);
//	NVStrings::destroy(nv_strings);

//	gdf_column_cpp new_input_col;
//	new_input_col.create_gdf_column(nv_category, nv_category->size(), "");

//	return new_input_col;
// }

// int insert_string_into_column_nvcategory(const cudf::column_view & col, const std::string & str) {
	// TODO percy cudf0.12 port to cudf::column and custrings
//	const char * str_arr[] = {str.c_str()};
//	NVStrings * temp_string = NVStrings::create_from_array(str_arr, 1);
//	NVCategory * old_category = static_cast<NVCategory *>(col->dtype_info.category);
//	NVCategory * new_category = old_category->add_strings(*temp_string);
//	col->dtype_info.category = new_category;

//	NVStrings::destroy(temp_string);
//	NVCategory::destroy(old_category);

//	CheckCudaErrors(cudaMemcpyAsync(
//		col->data, new_category->values_cptr(), sizeof(gdf_nvstring_category) * col->size, cudaMemcpyDeviceToDevice));

//	return new_category->get_value(str.c_str());
// }

std::string like_expression_to_regex_str(const std::string & like_exp) {
	if(like_exp.empty()) {
		return like_exp;
	}

	bool match_start = like_exp[0] != '%';
	bool match_end = like_exp[like_exp.size() - 1] != '%';

	std::string re = like_exp;
	static const std::regex any_string_re{R"(([^\\]?|\\{2})%)"};
	re = std::regex_replace(re, any_string_re, "$1(?:.*?)");

	static const std::regex any_char_re{R"(([^\\]?|\\{2})_)"};
	re = std::regex_replace(re, any_char_re, "$1(?:.)");

	return (match_start ? "^" : "") + re + (match_end ? "$" : "");
}

// std::unique_ptr<cudf::column> handle_match_regex(const cudf::column_view & input_col, const std::string & re) {
	// TODO percy cudf0.12 custrings this was not commented
//	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
//	NVStrings * nv_strings =
//		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

//	gdf_column_cpp new_input_col;
//	new_input_col.create_gdf_column(cudf::type_id::BOOL8,
//		input_col->size,
//		nullptr,
//		nullptr,
//		ral::traits::get_dtype_size_in_bytes(cudf::type_id::BOOL8));

//	nv_strings->contains_re(re.c_str(), static_cast<bool *>(new_input_col.data()));

//	NVStrings::destroy(nv_strings);

//	return new_input_col;
// }

// std::unique_ptr<cudf::column> handle_substring(const cudf::column_view & input_col, const std::string & str_params) {
	// TODO percy cudf0.12 custrings this was not commented
//	size_t pos = str_params.find(":");
//	int start = std::max(std::stoi(str_params.substr(0, pos)), 1) - 1;
//	int end = pos != std::string::npos ? start + std::stoi(str_params.substr(pos + 1)) : -1;

//	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
//	NVStrings * nv_strings =
//		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

//	NVStrings * new_strings = nv_strings->slice(start, end);
//	NVCategory * new_category = NVCategory::create_from_strings(*new_strings);

//	gdf_column_cpp new_input_col;
//	new_input_col.create_gdf_column(new_category, new_category->size(), "");

//	NVStrings::destroy(nv_strings);
//	NVStrings::destroy(new_strings);

//	return new_input_col;
// }

// std::unique_ptr<cudf::column> handle_concat_str_literal(const cudf::column_view & input_col, const std::string & str, bool prefix = false) {
	// TODO percy cudf0.12 custrings this was not commented
//	std::vector<const char *> str_vec{(size_t) input_col->size, str.c_str()};
//	NVStrings * temp_strings = NVStrings::create_from_array(str_vec.data(), str_vec.size());

//	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
//	NVStrings * nv_strings =
//		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

//	NVStrings * new_strings = prefix ? temp_strings->cat(nv_strings, "") : nv_strings->cat(temp_strings, "");
//	NVCategory * new_category = NVCategory::create_from_strings(*new_strings);

//	gdf_column_cpp new_input_col;
//	new_input_col.create_gdf_column(new_category, new_category->size(), "");

//	NVStrings::destroy(temp_strings);
//	NVStrings::destroy(nv_strings);
//	NVStrings::destroy(new_strings);

//	return new_input_col;
// }

// std::unique_ptr<cudf::column> handle_concat_str_col(const cudf::column_view & left_input_col, const cudf::column_view & right_input_col) {
	// TODO percy cudf0.12 port to cudf::column and custrings
//	NVCategory * left_nv_category = static_cast<NVCategory *>(left_input_col->dtype_info.category);
//	NVStrings * left_nv_strings = left_nv_category->gather_strings(
//		static_cast<nv_category_index_type *>(left_input_col->data), left_input_col->size);

//	NVCategory * right_nv_category = static_cast<NVCategory *>(right_input_col->dtype_info.category);
//	NVStrings * right_nv_strings = right_nv_category->gather_strings(
//		static_cast<nv_category_index_type *>(right_input_col->data), right_input_col->size);

//	NVStrings * new_strings = left_nv_strings->cat(right_nv_strings, "");
//	NVCategory * new_category = NVCategory::create_from_strings(*new_strings);

//	gdf_column_cpp new_input_col;
//	new_input_col.create_gdf_column(new_category, new_category->size(), "");

//	NVStrings::destroy(left_nv_strings);
//	NVStrings::destroy(right_nv_strings);
//	NVStrings::destroy(new_strings);

//	return new_input_col;
// }

}  // namespace detail

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
			return input_left_type;
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
		return cudf::type_id::BOOL8;
	case operator_type::BLZ_POW:
		return cudf::type_id::FLOAT64;
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
	column_index_type expression_position,
	column_index_type num_total_outputs,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,
	std::vector<column_index_type> & final_output_positions,
	std::vector<operator_type> & operators,
	std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	std::vector<std::unique_ptr<cudf::scalar>> & right_scalars) {

	using namespace detail;

	cudf::size_type num_inputs = table.num_columns();
	std::vector<bool> processing_space_free(512, true);  // A place to store whether or not a processing space is occupied at any point in time
	cudf::size_type start_processing_position = num_inputs + num_total_outputs;
	std::fill_n(processing_space_free.begin(), start_processing_position, false);

	std::deque<operand_position> operand_stack;
	std::map<column_index_type, column_index_type> src_str_col_map;
	for(size_t i = 0; i < tokenized_expression.size(); i++) {
		const std::string & token = tokenized_expression[i];

		if(is_operator_token(token)) {
			cudf::size_type src_str_col_idx = -1;
			bool new_input_col_added = false;

			if(is_binary_operator_token(token)) {
				const std::string & left_operand = operand_stack.back().token;
				if(!is_literal(left_operand)) {
					if(operand_stack.back().position >= start_processing_position) {
						processing_space_free[operand_stack.back().position] = true;
					}
				}
				operand_stack.pop_back();

				const std::string & right_operand = operand_stack.back().token;
				if(!is_literal(right_operand)) {
					if(operand_stack.back().position >= start_processing_position) {
						processing_space_free[operand_stack.back().position] = true;
					}
				}
				operand_stack.pop_back();

				operator_type operation = get_binary_operation(token);
				operators.push_back(operation);

				if(is_literal(left_operand) && is_literal(right_operand)) {
					RAL_FAIL("Operations between literals is not supported");
				} else if(is_literal(left_operand) && !is_string(left_operand)) {
					cudf::size_type right_index = get_index(right_operand);
					auto scalar_ptr = get_scalar_from_string(left_operand);

					left_inputs.push_back(scalar_ptr ? SCALAR_INDEX : SCALAR_NULL_INDEX);
					right_inputs.push_back(right_index);
					left_scalars.push_back(std::move(scalar_ptr));
					right_scalars.emplace_back(nullptr);
				} else if(is_literal(right_operand) && !is_string(right_operand)) {
					cudf::size_type left_index = get_index(left_operand);
					auto scalar_ptr = get_scalar_from_string(right_operand);
					
					left_inputs.push_back(left_index);
					right_inputs.push_back(scalar_ptr ? SCALAR_INDEX : SCALAR_NULL_INDEX);
					left_scalars.emplace_back(nullptr);
					right_scalars.push_back(std::move(scalar_ptr));
				} else if(is_string(left_operand) || is_string(right_operand)) {
					// std::string literal_operand = is_string(left_operand)
					// 								  ? left_operand.substr(1, left_operand.size() - 2)
					// 								  : right_operand.substr(1, right_operand.size() - 2);
					// cudf::size_type left_index = is_string(left_operand) ? get_index(right_operand) : get_index(left_operand);
					// column_index_type mapped_index = src_str_col_map.at(left_index);
					// cudf::column_view left_column = table.column(mapped_index);

					// if(operation == operator_type::BLZ_STR_LIKE) {
					// 	std::string regex = like_expression_to_regex_str(literal_operand);
					// 	gdf_column_cpp new_input_col = handle_match_regex(left_column, regex);

					// 	inputs.add_column(new_input_col);
					// 	input_columns.push_back(new_input_col.get_gdf_column());

					// 	left_index = num_inputs;
					// 	new_input_col_added = true;

					// 	right_scalars.push_back(dummy_scalar);
					// 	left_scalars.push_back(dummy_scalar);
						
					// 	right_inputs.push_back(SCALAR_NULL_INDEX);
					// 	left_inputs.push_back(left_index);
					// } else if(operation == operator_type::BLZ_STR_SUBSTRING) {
					// 	gdf_column_cpp new_input_col = handle_substring(left_column, literal_operand);

					// 	inputs.add_column(new_input_col);
					// 	input_columns.push_back(new_input_col.get_gdf_column());

					// 	src_str_col_map[num_inputs] = num_inputs;
					// 	src_str_col_idx = num_inputs;

					// 	left_index = num_inputs;
					// 	new_input_col_added = true;

					// 	right_scalars.push_back(dummy_scalar);
					// 	left_scalars.push_back(dummy_scalar);
						
					// 	right_inputs.push_back(SCALAR_NULL_INDEX);
					// 	left_inputs.push_back(left_index);
					// } else if(operation == operator_type::BLZ_STR_CONCAT) {
					// 	gdf_column_cpp new_input_col =
					// 		handle_concat_str_literal(left_column, literal_operand, is_string(left_operand));

					// 	inputs.add_column(new_input_col);
					// 	input_columns.push_back(new_input_col.get_gdf_column());

					// 	src_str_col_map[num_inputs] = num_inputs;
					// 	src_str_col_idx = num_inputs;

					// 	left_index = num_inputs;
					// 	new_input_col_added = true;

					// 	right_scalars.push_back(dummy_scalar);
					// 	left_scalars.push_back(dummy_scalar);
						
					// 	right_inputs.push_back(SCALAR_NULL_INDEX);
					// 	left_inputs.push_back(left_index);
					// } else {
					// 	int idx_position = static_cast<NVCategory *>(left_column->dtype_info.category)
					// 						   ->get_value(literal_operand.c_str());
					// 	if(idx_position == -1) {
					// 		idx_position = insert_string_into_column_nvcategory(left_column, literal_operand);
					// 	}
					// 	assert(idx_position != -1);

					// 	src_str_col_idx = mapped_index;

					// 	gdf_data data;
					// 	data.si32 = idx_position;
						
					// 	std::unique_ptr<cudf::scalar> right = {data, GDF_INT32, true};
					// 	right_scalars.push_back(right);
					// 	left_scalars.push_back(dummy_scalar);
					// 	right_inputs.push_back(right.is_valid ? SCALAR_INDEX : SCALAR_NULL_INDEX);

					// 	left_inputs.push_back(left_index);
					// }
				} else {
					cudf::size_type left_index = get_index(left_operand);
					cudf::size_type right_index = get_index(right_operand);

					// column_index_type mapped_left_index = src_str_col_map[left_index];
					// column_index_type mapped_right_index = src_str_col_map[right_index];
					// src_str_col_idx = mapped_left_index >= 0 ? mapped_left_index : mapped_right_index;
					// if(mapped_left_index >= 0 && mapped_right_index >= 0 && mapped_left_index != mapped_right_index) {
					// 	cudf::column * left_column = input_columns[mapped_left_index];
					// 	cudf::column * right_column = input_columns[mapped_right_index];

					// 	if(operation == operator_type::BLZ_STR_CONCAT) {
					// 		gdf_column_cpp new_input_col = handle_concat_str_col(left_column, right_column);

					// 		inputs.add_column(new_input_col);
					// 		input_columns.push_back(new_input_col.get_gdf_column());

					// 		src_str_col_map[num_inputs] = num_inputs;
					// 		src_str_col_idx = num_inputs;

					// 		left_index = num_inputs;
					// 		new_input_col_added = true;
					// 	} else {
					// 		gdf_column * process_columns[2] = {left_column, right_column};
					// 		gdf_column * output_columns[2] = {left_column, right_column};

					// 		CUDF_CALL(sync_column_categories(process_columns, output_columns, 2));
					// 	}
					// }

					left_inputs.push_back(left_index);
					right_inputs.push_back(right_index);
					left_scalars.emplace_back(nullptr);
					right_scalars.emplace_back(nullptr);
				}
			} else { // if(is_unary_operator_token(token))
				std::string left_operand = operand_stack.back().token;
				RAL_EXPECTS(!is_literal(left_operand), "Unary operations on literals is not supported");

				if(operand_stack.back().position >= start_processing_position) {
					processing_space_free[operand_stack.back().position] = true;
				}
				operand_stack.pop_back();

				operator_type operation = get_unary_operation(token);
				size_t left_index = get_index(left_operand);

				// column_index_type mapped_left_index = src_str_col_map[left_index];
				// if(operation == operator_type::BLZ_CAST_VARCHAR) {
				// 	if(left_index < num_inputs) {
				// 		cudf::column * left_column = input_columns[left_index];
				// 		if(left_column->type().id() != cudf::type_id::STRING) {
				// 			gdf_column_cpp new_input_col = handle_cast_to_string(left_column);

				// 			inputs.add_column(new_input_col);
				// 			input_columns.push_back(new_input_col.get_gdf_column());

				// 			src_str_col_map[num_inputs] = num_inputs;
				// 			src_str_col_idx = num_inputs;

				// 			left_index = num_inputs;
				// 			new_input_col_added = true;
				// 		} else {
				// 			src_str_col_idx = src_str_col_map[left_index];
				// 		}
				// 	} else {
				// 		RAL_FAIL("Cast to String from intermediate results is not supported yet");
				// 	}
				// } else if(operation == operator_type::BLZ_CAST_INTEGER || operation == operator_type::BLZ_CAST_BIGINT ||
				// 			operation == operator_type::BLZ_CAST_FLOAT || operation == operator_type::BLZ_CAST_DOUBLE ||
				// 			operation == operator_type::BLZ_CAST_DATE || operation == operator_type::BLZ_CAST_TIMESTAMP) {
				// 	if(mapped_left_index >= 0) {
				// 		cudf::column * left_column = input_columns[mapped_left_index];
				// 		gdf_column_cpp new_input_col = handle_cast_from_string(operation, left_column);

				// 		inputs.add_column(new_input_col);
				// 		input_columns.push_back(new_input_col.get_gdf_column());

				// 		left_index = num_inputs;
				// 		new_input_col_added = true;
				// 	}
				// }

				operators.push_back(operation);

				left_inputs.push_back(left_index);
				right_inputs.push_back(UNARY_INDEX);
				left_scalars.emplace_back(nullptr);
				right_scalars.emplace_back(nullptr);
			}

			// if(new_input_col_added) {
			// 	new_input_indices.push_back(num_inputs);

			// 	// Update plan to avoid collision between indices
			// 	for(size_t i = 0; i < left_inputs.size() - 1; i++) {
			// 		if(left_inputs[i] >= num_inputs) {
			// 			left_inputs[i]++;
			// 		}
			// 		if(right_inputs[i] >= num_inputs) {
			// 			right_inputs[i]++;
			// 		}
			// 	}
			// 	auto max_iter = std::max_element(outputs.begin(), outputs.end());
			// 	for(column_index_type i = (max_iter != outputs.end() ? *max_iter : -1); i > num_inputs; i--) {
			// 		auto iter = src_str_col_map.find(i);
			// 		if (iter != src_str_col_map.end()) {
			// 			src_str_col_map[i + 1] = iter->second;;
			// 			src_str_col_map.erase(iter);
			// 		}
			// 	}
			// 	for(size_t i = 0; i < outputs.size(); i++) {
			// 		if(outputs[i] >= num_inputs) {
			// 			outputs[i]++;
			// 		}
			// 	}
			// 	for(size_t i = 0; i < final_output_positions.size(); i++) {
			// 		if(final_output_positions[i] >= num_inputs) {
			// 			final_output_positions[i]++;
			// 		}
			// 	}
			// 	for(size_t i = 0; i < operand_stack.size(); i++) {
			// 		column_index_type position = operand_stack[i].position;
			// 		if(position >= num_inputs) {
			// 			position++;
			// 			operand_stack[i] = {position, "$" + std::to_string(position)};
			// 		}
			// 	}

			// 	num_inputs++;
			// 	start_processing_position++;
			// 	for(column_index_type i = start_processing_position; i < start_processing_position + outputs.size(); i++) {
			// 		processing_space_free[i] = false;
			// 	}
			// }

			if(i == tokenized_expression.size() - 1) {
				// write to final output
				outputs.push_back(expression_position + num_inputs);
				// if(output_column && output_column->type().id() == cudf::type_id::STRING) {
					// assert(src_str_col_idx != -1);
					// NVCategory::destroy(static_cast<NVCategory *>(output_column->dtype_info.category));
					// cudf::column * src_col = input_columns[src_str_col_idx];
					// output_column->dtype_info.category = static_cast<NVCategory *>(src_col->dtype_info.category)->copy();
				// }
			} else {
				// write to temp output
				column_index_type output_position =	get_first_open_position(processing_space_free, start_processing_position);
				outputs.push_back(output_position);
				operand_stack.push_back({output_position, "$" + std::to_string(output_position)});
				// if (src_str_col_idx != -1) {
				// 	src_str_col_map[output_position] = src_str_col_idx;
				// }
			}
		} else {
			if(is_literal(token)) {
				operand_stack.push_back({SCALAR_INDEX, token});
			} else {
				cudf::size_type mapped_idx = expr_idx_to_col_idx_map.at(get_index(token));
				operand_stack.push_back({mapped_idx, "$" + std::to_string(mapped_idx)});
				// if (table.column(mapped_idx).type().id() == cudf::type_id::STRING) {
				// 	src_str_col_map[mapped_idx] = mapped_idx;
				// }
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

	auto max_it = std::max_element(outputs.begin(), outputs.end());
	column_index_type max_output = (max_it != outputs.end() ? *max_it : 0);

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
