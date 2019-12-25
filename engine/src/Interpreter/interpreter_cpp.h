/*
 * interpreter_cpp.h
 *
 *  Created on: Jan 12, 2019
 *      Author: felipe
 */

#ifndef INTERPRETER_CPP_H_
#define INTERPRETER_CPP_H_


#include "cudf/legacy/binaryop.hpp"
#include "interpreter_cpp.h"
#include <gdf_wrapper/gdf_wrapper.cuh>
#include <vector>

// We have templated cude that has to be in a
//.cuh but we need to be able to include this in cpp code that is not compiled with nvcc
// this wraps that
typedef short column_index_type;
static const short SCALAR_INDEX = -2;
static const short SCALAR_NULL_INDEX = -3;


void perform_operation(std::vector<gdf_column *> output_columns,
	std::vector<gdf_column *> input_columns,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,
	std::vector<column_index_type> & final_output_positions,
	std::vector<gdf_binary_operator_exp> & operators,
	std::vector<gdf_unary_operator> & unary_operators,


	std::vector<cudf::scalar*> & left_scalars,
	std::vector<cudf::scalar*> & right_scalars,
	std::vector<column_index_type> new_input_indices);


#endif /* INTERPRETER_CPP_H_ */
