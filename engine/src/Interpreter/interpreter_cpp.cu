#include <cudf/utilities/error.hpp>
#include <algorithm>

#include "interpreter_cpp.h"
#include "interpreter_ops.cuh"

namespace {

template <int SIZE, int REGISTER_SIZE>
int calculated_shared_memory(int num_threads_per_block) {
	return SIZE * num_threads_per_block * REGISTER_SIZE;
}

// TODO: we dont know if this is fast or not we coudl store this in a pre computed map
void calculate_grid(int * min_grid_size, int * block_size, column_index_type max_output) {
	if(max_output == 1) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<1, 8>, 0));
	} else if(max_output == 2) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<2, 8>, 0));
	} else if(max_output == 3) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<3, 8>, 0));
	} else if(max_output == 4) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<4, 8>, 0));
	} else if(max_output == 5) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<5, 8>, 0));
	} else if(max_output == 6) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<6, 8>, 0));
	} else if(max_output == 7) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<7, 8>, 0));
	} else if(max_output == 8) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<8, 8>, 0));
	} else if(max_output == 9) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<9, 8>, 0));
	} else if(max_output == 10) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<10, 8>, 0));
	} else if(max_output == 11) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<11, 8>, 0));
	} else if(max_output == 12) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<12, 8>, 0));
	} else if(max_output == 13) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<13, 8>, 0));
	} else if(max_output == 14) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<14, 8>, 0));
	} else if(max_output == 15) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<15, 8>, 0));
	} else if(max_output == 16) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<16, 8>, 0));
	} else if(max_output == 17) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<17, 8>, 0));
	} else if(max_output == 18) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<18, 8>, 0));
	} else if(max_output == 19) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<19, 8>, 0));
	} else if(max_output == 20) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<20, 8>, 0));
	} else if(max_output == 21) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<21, 8>, 0));
	} else if(max_output == 22) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<22, 8>, 0));
	} else if(max_output == 23) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<23, 8>, 0));
	} else if(max_output == 24) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<24, 8>, 0));
	} else if(max_output == 25) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<25, 8>, 0));
	} else if(max_output == 26) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<26, 8>, 0));
	} else if(max_output == 27) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<27, 8>, 0));
	} else if(max_output == 28) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<28, 8>, 0));
	} else if(max_output == 29) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<29, 8>, 0));
	} else if(max_output == 30) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<30, 8>, 0));
	} else if(max_output == 31) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<31, 8>, 0));
	} else if(max_output == 32) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<32, 8>, 0));
	} else if(max_output == 33) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<33, 8>, 0));
	} else if(max_output == 34) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<34, 8>, 0));
	} else if(max_output == 35) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<35, 8>, 0));
	} else if(max_output == 36) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<36, 8>, 0));
	} else if(max_output == 37) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<37, 8>, 0));
	} else if(max_output == 38) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<38, 8>, 0));
	} else if(max_output == 39) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<39, 8>, 0));
	} else if(max_output == 40) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<40, 8>, 0));
	} else if(max_output == 41) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<41, 8>, 0));
	} else if(max_output == 42) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<42, 8>, 0));
	} else if(max_output == 43) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<43, 8>, 0));
	} else if(max_output == 44) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<44, 8>, 0));
	} else if(max_output == 45) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<45, 8>, 0));
	} else if(max_output == 46) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<46, 8>, 0));
	} else if(max_output == 47) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<47, 8>, 0));
	} else if(max_output == 48) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<48, 8>, 0));
	} else if(max_output == 49) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<49, 8>, 0));
	} else if(max_output == 50) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<50, 8>, 0));
	} else if(max_output == 51) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<51, 8>, 0));
	} else if(max_output == 52) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<52, 8>, 0));
	} else if(max_output == 53) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<53, 8>, 0));
	} else if(max_output == 54) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<54, 8>, 0));
	} else if(max_output == 55) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<55, 8>, 0));
	} else if(max_output == 56) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<56, 8>, 0));
	} else if(max_output == 57) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<57, 8>, 0));
	} else if(max_output == 58) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<58, 8>, 0));
	} else if(max_output == 59) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<59, 8>, 0));
	} else if(max_output == 60) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<60, 8>, 0));
	} else if(max_output == 61) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<61, 8>, 0));
	} else if(max_output == 62) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<62, 8>, 0));
	} else if(max_output == 63) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<63, 8>, 0));
	} else if(max_output == 64) {
		CUDA_TRY(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, interops::transformKernel, calculated_shared_memory<64, 8>, 0));
	}
}

}  // namespace


void perform_operation(cudf::mutable_table_view & output_table,
	const cudf::table_view & table,
	const std::vector<column_index_type> & left_inputs,
	const std::vector<column_index_type> & right_inputs,
	const std::vector<column_index_type> & outputs,
	const std::vector<column_index_type> & final_output_positions,
	const std::vector<gdf_binary_operator_exp> & operators,
	const std::vector<gdf_unary_operator> & unary_operators,
	const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars) {
	// find maximum register used
	auto max_it = std::max_element(outputs.begin(), outputs.end());
	column_index_type max_output = (max_it != outputs.end() ? *max_it : 0);

	cudaStream_t stream;
	CUDA_TRY(cudaStreamCreate(&stream));

	size_t shared_memory_per_thread = (max_output + 1) * sizeof(int64_t);

	int min_grid_size, block_size;
	calculate_grid(&min_grid_size, &block_size, max_output + 1);

	size_t temp_size = interops::InterpreterFunctor::get_temp_size(
		table.num_columns(), left_inputs.size(), final_output_positions.size());

	rmm::device_buffer temp_space(temp_size, stream); 

	size_t temp_valids_in_size = min_grid_size * block_size * table.num_columns() * sizeof(int64_t);
	size_t temp_valids_out_size = min_grid_size * block_size * final_output_positions.size() * sizeof(int64_t);

	rmm::device_buffer temp_valids_in_buffer(temp_valids_in_size, stream); 
	rmm::device_buffer temp_valids_out_buffer(temp_valids_out_size, stream); 

	interops::InterpreterFunctor op(output_table,
		table,
		left_inputs,
		right_inputs,
		outputs,
		final_output_positions,
		operators,
		unary_operators,
		left_scalars,
		right_scalars,
		block_size,
		temp_space.data(),
		temp_valids_in_buffer.data(),
		temp_valids_out_buffer.data(),
		stream);

	interops::transformKernel<<<min_grid_size,
		block_size,
		//	interops::transformKernel<<<1
		//	,1,
		shared_memory_per_thread * block_size,
		stream>>>(op, table.num_rows());

	CUDA_TRY(cudaStreamSynchronize(stream));
	CUDA_TRY(cudaStreamDestroy(stream));
}
