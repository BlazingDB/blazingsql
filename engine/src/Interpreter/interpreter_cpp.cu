#include "interpreter_cpp.h"
#include "Interpreter/interpreter_ops.cuh"
#include "Config/Config.h"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include "cuDF/Allocator.h"
#include "../Utils.cuh"
#include "cudf/legacy/binaryop.hpp"

//TODO: a better way to handle all this thread block size is
//to get the amount of shared memory from the device and figure it out that way
//right now we are hardcoding to be able to handle pascal architecture cards


//we need to limit the number of threas per block depending on how mcuh shared memory we need per thread
typedef InterpreterFunctor<size_t> interpreter_functor_8;


/*
 *
 *
 *
 *
 *
 *
 dataframe input
 How to use this shit

 gdf_column col_input_1 = //make a column
 gdf_column col_input_2 = //make a column
 gdf_column col_input_3 = //make a column

 gdf_column col_output_1 = //allocate column
 gdf_column col_output_2 = //allocate column

 std::vector<gdf_column *> output_columns(2);
 output_columns[0] = &col_output_1;
 output_columns[1] = &col_output_2;

 std::vector<gdf_column *> input_columns(3);
 input_columns[0] = &col_input_1;
 input_columns[1] = &col_input_2;
 input_columns[2] = &col_input_3;


 temp_space starts at input.size + output.size = 5

 + * + $0 $1 $2 $1 , + sin $1 2.33   = step 0

 + * $5 $2 $1 , + $1 $2 step 1

 + $5 $1 , + $1 $2 step 2




Registers are
	0			1				2			3			4				5			6				n + 3 + 2
input_col_1, input_col_2, input_col_3, output_col_1, output_col2, processing_1, processing_2 .... processing_n

std::vector<column_index_type> & left_inputs = { 0, 5, 5, 1 ,5},
		std::vector<column_index_type> & right_inputs = { 1, 2, 1, -1, -2 },
		std::vector<column_index_type> & outputs { 5, 5, 3, 5,4 }


 std::vector<column_index_type> & final_output_positions = { 3 , 4 }

 std::vector<gdf_binary_operator_exp> & operators = { GDF_ADD, GDF_MULT, GDF_ADD, GDF_INVALID_BINARY, GDF_ADD}
		std::vector<gdf_unary_operator> & unary_operators = { BLZ_INVALID_UNARY,BLZ_INVALID_UNARY,BLZ_INVALID_UNARY,GDF_SIN,BLZ_INVALID_UNARY  }

 		std::vector<gdf_scalar> & left_scalars = { junk, junk, junk, junk, junk }
		std::vector<gdf_scalar> & right_scalars = {junk, junk ,junk , 2.33, junk }

 		std::vector<column_index_type> new_input_indices = {0 , 1, 2 }

 perform_operation(all this shit you just made);
 hola alexander comoe estas
 *
 *
 */
template<int SIZE, int REGISTER_SIZE>
int calculated_shared_memory(int num_threads_per_block){
	return SIZE * num_threads_per_block * REGISTER_SIZE;
}

//TODO: we dont know if this is fast or not we coudl store this in a pre computed map
void calculate_grid(int * min_grid_size, int * block_size, column_index_type max_output)
{
	if(max_output == 1){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<1,8>, 0 ));
	}else if(max_output == 2){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<2,8>, 0 ));
	}else if(max_output == 3){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<3,8>, 0 ));
	}else if(max_output == 4){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<4,8>, 0 ));
	}else if(max_output == 5){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<5,8>, 0 ));
	}else if(max_output == 6){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<6,8>, 0 ));
	}else if(max_output == 7){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<7,8>, 0 ));
	}else if(max_output == 8){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<8,8>, 0 ));
	}else if(max_output == 9){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<9,8>, 0 ));
	}else if(max_output == 10){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<10,8>, 0 ));
	}else if(max_output == 11){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<11,8>, 0 ));
	}else if(max_output == 12){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<12,8>, 0 ));
	}else if(max_output == 13){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<13,8>, 0 ));
	}else if(max_output == 14){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<14,8>, 0 ));
	}else if(max_output == 15){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<15,8>, 0 ));
	}else if(max_output == 16){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<16,8>, 0 ));
	}else if(max_output == 17){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<17,8>, 0 ));
	}else if(max_output == 18){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<18,8>, 0 ));
	}else if(max_output == 19){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<19,8>, 0 ));
	}else if(max_output == 20){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<20,8>, 0 ));
	}else if(max_output == 21){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<21,8>, 0 ));
	}else if(max_output == 22){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<22,8>, 0 ));
	}else if(max_output == 23){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<23,8>, 0 ));
	}else if(max_output == 24){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<24,8>, 0 ));
	}else if(max_output == 25){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<25,8>, 0 ));
	}else if(max_output == 26){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<26,8>, 0 ));
	}else if(max_output == 27){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<27,8>, 0 ));
	}else if(max_output == 28){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<28,8>, 0 ));
	}else if(max_output == 29){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<29,8>, 0 ));
	}else if(max_output == 30){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<30,8>, 0 ));
	}else if(max_output == 31){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<31,8>, 0 ));
	}else if(max_output == 32){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<32,8>, 0 ));
	}else if(max_output == 33){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<33,8>, 0 ));
	}else if(max_output == 34){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<34,8>, 0 ));
	}else if(max_output == 35){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<35,8>, 0 ));
	}else if(max_output == 36){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<36,8>, 0 ));
	}else if(max_output == 37){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<37,8>, 0 ));
	}else if(max_output == 38){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<38,8>, 0 ));
	}else if(max_output == 39){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<39,8>, 0 ));
	}else if(max_output == 40){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<40,8>, 0 ));
	}else if(max_output == 41){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<41,8>, 0 ));
	}else if(max_output == 42){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<42,8>, 0 ));
	}else if(max_output == 43){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<43,8>, 0 ));
	}else if(max_output == 44){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<44,8>, 0 ));
	}else if(max_output == 45){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<45,8>, 0 ));
	}else if(max_output == 46){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<46,8>, 0 ));
	}else if(max_output == 47){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<47,8>, 0 ));
	}else if(max_output == 48){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<48,8>, 0 ));
	}else if(max_output == 49){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<49,8>, 0 ));
	}else if(max_output == 50){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<50,8>, 0 ));
	}else if(max_output == 51){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<51,8>, 0 ));
	}else if(max_output == 52){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<52,8>, 0 ));
	}else if(max_output == 53){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<53,8>, 0 ));
	}else if(max_output == 54){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<54,8>, 0 ));
	}else if(max_output == 55){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<55,8>, 0 ));
	}else if(max_output == 56){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<56,8>, 0 ));
	}else if(max_output == 57){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<57,8>, 0 ));
	}else if(max_output == 58){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<58,8>, 0 ));
	}else if(max_output == 59){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<59,8>, 0 ));
	}else if(max_output == 60){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<60,8>, 0 ));
	}else if(max_output == 61){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<61,8>, 0 ));
	}else if(max_output == 62){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<62,8>, 0 ));
	}else if(max_output == 63){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<63,8>, 0 ));
	}else if(max_output == 64){
		CheckCudaErrors(cudaOccupancyMaxPotentialBlockSizeVariableSMem ( min_grid_size, block_size, transformKernel<interpreter_functor_8>, calculated_shared_memory<64,8>, 0 ));
	}
}

void perform_operation(	std::vector<gdf_column *> output_columns,
		std::vector<gdf_column *> input_columns,
		std::vector<column_index_type> & left_inputs,
		std::vector<column_index_type> & right_inputs,
		std::vector<column_index_type> & outputs,
		std::vector<column_index_type> & final_output_positions,
		std::vector<gdf_binary_operator_exp> & operators,
		std::vector<gdf_unary_operator> & unary_operators,


		std::vector<gdf_scalar> & left_scalars,
		std::vector<gdf_scalar> & right_scalars,
		std::vector<column_index_type> new_input_indices){

	//find maximum register used
	column_index_type max_output = 0;
	for(std::size_t i = 0; i < outputs.size(); i++){
		if(max_output < outputs[i]){
			max_output = outputs[i];
		}
	}


	char * temp_space;

	gdf_size_type num_rows = input_columns[0]->size;

	cudaStream_t stream;
	CheckCudaErrors(cudaStreamCreate(&stream));


	size_t shared_memory_per_thread = (max_output+1) * sizeof(int64_t);
	int min_grid_size;
	int block_size;

	calculate_grid(&min_grid_size, &block_size, max_output+1);

	size_t temp_size = interpreter_functor_8::get_temp_size(input_columns.size(),left_inputs.size(),final_output_positions.size());

	cuDF::Allocator::allocate((void **)&temp_space,temp_size, stream);

	int64_t * temp_valids_in_buffer, *temp_valids_out_buffer;
	size_t temp_valids_in_size = min_grid_size * block_size * input_columns.size() * sizeof(int64_t);
	size_t temp_valids_out_size = min_grid_size * block_size * final_output_positions.size() * sizeof(int64_t);

	cuDF::Allocator::allocate((void **)&temp_valids_in_buffer,temp_valids_in_size, stream);
	cuDF::Allocator::allocate((void **)&temp_valids_out_buffer,temp_valids_out_size, stream);

	interpreter_functor_8 op(input_columns,
			output_columns,
			left_inputs.size(),
			left_inputs,
			right_inputs,
			outputs,
			final_output_positions,
			operators,
			unary_operators,
			left_scalars,
			right_scalars
			,stream,
			temp_space,
			max_output,
			block_size);

	transformKernel<<<min_grid_size
					,block_size,
					//	transformKernel<<<1
					//	,1,
					shared_memory_per_thread * block_size,
					stream>>>(op, num_rows, temp_valids_in_buffer, temp_valids_out_buffer);


	// op.update_columns_null_count(output_columns);

	CheckCudaErrors(cudaStreamSynchronize(stream));

	cuDF::Allocator::deallocate(temp_space,stream);
	cuDF::Allocator::deallocate(temp_valids_in_buffer,stream);
	cuDF::Allocator::deallocate(temp_valids_out_buffer,stream);

	CheckCudaErrors(cudaGetLastError());

	CheckCudaErrors(cudaStreamDestroy(stream));
}
