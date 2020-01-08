#include <algorithm>

#include "Utils.cuh"
#include "cuDF/Allocator.h"
#include "gdf_wrapper/gdf_wrapper.cuh"
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
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<1, 8>, 0));
	} else if(max_output == 2) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<2, 8>, 0));
	} else if(max_output == 3) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<3, 8>, 0));
	} else if(max_output == 4) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<4, 8>, 0));
	} else if(max_output == 5) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<5, 8>, 0));
	} else if(max_output == 6) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<6, 8>, 0));
	} else if(max_output == 7) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<7, 8>, 0));
	} else if(max_output == 8) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<8, 8>, 0));
	} else if(max_output == 9) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<9, 8>, 0));
	} else if(max_output == 10) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<10, 8>, 0));
	} else if(max_output == 11) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<11, 8>, 0));
	} else if(max_output == 12) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<12, 8>, 0));
	} else if(max_output == 13) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<13, 8>, 0));
	} else if(max_output == 14) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<14, 8>, 0));
	} else if(max_output == 15) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<15, 8>, 0));
	} else if(max_output == 16) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<16, 8>, 0));
	} else if(max_output == 17) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<17, 8>, 0));
	} else if(max_output == 18) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<18, 8>, 0));
	} else if(max_output == 19) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<19, 8>, 0));
	} else if(max_output == 20) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<20, 8>, 0));
	} else if(max_output == 21) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<21, 8>, 0));
	} else if(max_output == 22) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<22, 8>, 0));
	} else if(max_output == 23) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<23, 8>, 0));
	} else if(max_output == 24) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<24, 8>, 0));
	} else if(max_output == 25) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<25, 8>, 0));
	} else if(max_output == 26) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<26, 8>, 0));
	} else if(max_output == 27) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<27, 8>, 0));
	} else if(max_output == 28) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<28, 8>, 0));
	} else if(max_output == 29) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<29, 8>, 0));
	} else if(max_output == 30) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<30, 8>, 0));
	} else if(max_output == 31) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<31, 8>, 0));
	} else if(max_output == 32) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<32, 8>, 0));
	} else if(max_output == 33) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<33, 8>, 0));
	} else if(max_output == 34) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<34, 8>, 0));
	} else if(max_output == 35) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<35, 8>, 0));
	} else if(max_output == 36) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<36, 8>, 0));
	} else if(max_output == 37) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<37, 8>, 0));
	} else if(max_output == 38) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<38, 8>, 0));
	} else if(max_output == 39) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<39, 8>, 0));
	} else if(max_output == 40) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<40, 8>, 0));
	} else if(max_output == 41) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<41, 8>, 0));
	} else if(max_output == 42) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<42, 8>, 0));
	} else if(max_output == 43) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<43, 8>, 0));
	} else if(max_output == 44) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<44, 8>, 0));
	} else if(max_output == 45) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<45, 8>, 0));
	} else if(max_output == 46) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<46, 8>, 0));
	} else if(max_output == 47) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<47, 8>, 0));
	} else if(max_output == 48) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<48, 8>, 0));
	} else if(max_output == 49) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<49, 8>, 0));
	} else if(max_output == 50) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<50, 8>, 0));
	} else if(max_output == 51) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<51, 8>, 0));
	} else if(max_output == 52) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<52, 8>, 0));
	} else if(max_output == 53) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<53, 8>, 0));
	} else if(max_output == 54) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<54, 8>, 0));
	} else if(max_output == 55) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<55, 8>, 0));
	} else if(max_output == 56) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<56, 8>, 0));
	} else if(max_output == 57) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<57, 8>, 0));
	} else if(max_output == 58) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<58, 8>, 0));
	} else if(max_output == 59) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<59, 8>, 0));
	} else if(max_output == 60) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<60, 8>, 0));
	} else if(max_output == 61) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<61, 8>, 0));
	} else if(max_output == 62) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<62, 8>, 0));
	} else if(max_output == 63) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<63, 8>, 0));
	} else if(max_output == 64) {
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem(
			min_grid_size, block_size, transformKernel, calculated_shared_memory<64, 8>, 0));
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
	CheckCudaErrors(cudaStreamCreate(&stream));

	size_t shared_memory_per_thread = (max_output + 1) * sizeof(int64_t);

	int min_grid_size, block_size;
	calculate_grid(&min_grid_size, &block_size, max_output + 1);

	size_t temp_size = InterpreterFunctor::get_temp_size(
		table.num_columns(), left_inputs.size(), final_output_positions.size());

	char * temp_space;
	cuDF::Allocator::allocate((void **) &temp_space, temp_size, stream);

	size_t temp_valids_in_size = min_grid_size * block_size * table.num_columns() * sizeof(int64_t);
	size_t temp_valids_out_size = min_grid_size * block_size * final_output_positions.size() * sizeof(int64_t);

	int64_t *temp_valids_in_buffer, *temp_valids_out_buffer;
	cuDF::Allocator::allocate((void **) &temp_valids_in_buffer, temp_valids_in_size, stream);
	cuDF::Allocator::allocate((void **) &temp_valids_out_buffer, temp_valids_out_size, stream);

	InterpreterFunctor op(output_table,
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
		temp_space,
		stream);

	transformKernel<<<min_grid_size,
		block_size,
		//	transformKernel<<<1
		//	,1,
		shared_memory_per_thread * block_size,
		stream>>>(op, table.num_rows(), temp_valids_in_buffer, temp_valids_out_buffer);

	CheckCudaErrors(cudaStreamSynchronize(stream));

	cuDF::Allocator::deallocate(temp_space, stream);
	cuDF::Allocator::deallocate(temp_valids_in_buffer, stream);
	cuDF::Allocator::deallocate(temp_valids_out_buffer, stream);

	CheckCudaErrors(cudaGetLastError());

	CheckCudaErrors(cudaStreamDestroy(stream));
}
