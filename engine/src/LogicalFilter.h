/*
 * LogicalFilter.h
 *
 *  Created on: Jul 20, 2018
 *      Author: felipe
 */

#ifndef LOGICALFILTER_H_
#define LOGICALFILTER_H_

#include <vector>
#include "gdf_wrapper/gdf_wrapper.cuh"
#include <string>
#include "DataFrame.h"
#include "Utils.cuh"
#include "cudf/legacy/binaryop.hpp"

typedef short column_index_type;

void evaluate_expression(
		blazing_frame& inputs,
		const std::string& expression,
		gdf_column_cpp& output);


void add_expression_to_plan(	blazing_frame & inputs,
		std::vector<gdf_column *>& input_columns,
		std::string expression,
		column_index_type expression_position,
		column_index_type num_outputs,
		column_index_type num_inputs,
		std::vector<column_index_type> & left_inputs,
		std::vector<column_index_type> & right_inputs,
		std::vector<column_index_type> & outputs,

		std::vector<gdf_binary_operator_exp> & operators,
		std::vector<gdf_unary_operator> & unary_operators,


		std::vector<gdf_scalar> & left_scalars,
		std::vector<gdf_scalar> & right_scalars,
		std::vector<column_index_type> & new_input_indices,
		
		std::vector<column_index_type> & final_output_positions,
		gdf_column * output_column = nullptr);

#endif /* LOGICALFILTER_H_ */
