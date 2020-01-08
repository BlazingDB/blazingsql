/*
 * interpreter_cpp.h
 *
 *  Created on: Jan 12, 2019
 *      Author: felipe
 */

#ifndef INTERPRETER_CPP_H_
#define INTERPRETER_CPP_H_

#include <cudf/table/table_view.hpp>
#include <cudf/scalar/scalar.hpp>
#include <vector>

#include "gdf_wrapper/gdf_types.cuh"

// We have templated cude that has to be in a
//.cuh but we need to be able to include this in cpp code that is not compiled with nvcc
// this wraps that
typedef int16_t column_index_type;
static const short UNARY_INDEX = -1;
static const short SCALAR_INDEX = -2;
static const short SCALAR_NULL_INDEX = -3;

void perform_operation(cudf::mutable_table_view & output_table,
	const cudf::table_view & table,
	const std::vector<column_index_type> & left_inputs,
	const std::vector<column_index_type> & right_inputs,
	const std::vector<column_index_type> & outputs,
	const std::vector<column_index_type> & final_output_positions,
	const std::vector<gdf_binary_operator_exp> & operators,
	const std::vector<gdf_unary_operator> & unary_operators,
	const std::vector<std::unique_ptr<cudf::scalar>> & left_scalars,
	const std::vector<std::unique_ptr<cudf::scalar>> & right_scalars);

#endif /* INTERPRETER_CPP_H_ */
