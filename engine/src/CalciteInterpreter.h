
#ifndef CALCITEINTERPRETER_H_
#define CALCITEINTERPRETER_H_

#include <iostream>
#include <vector>
#include <string>
#include "DataFrame.h"
#include "Types.h"
#include "LogicalFilter.h"
#include "io/DataLoader.h"
#include "cudf/legacy/binaryop.hpp"

#include <blazingdb/manager/Context.h>
using blazingdb::manager::Context;

struct project_plan_params{
  size_t num_expressions_out;
  std::vector<gdf_column *> output_columns;
  std::vector<gdf_column *> input_columns;
  std::vector<column_index_type> left_inputs;
  std::vector<column_index_type> right_inputs;
  std::vector<column_index_type> outputs;
  std::vector<column_index_type> final_output_positions;
  std::vector<gdf_binary_operator_exp> operators;
  std::vector<gdf_unary_operator> unary_operators;
  std::vector<gdf_scalar> left_scalars;
  std::vector<gdf_scalar> right_scalars;
  std::vector<column_index_type> new_column_indices;
  std::vector<gdf_column_cpp> columns;
  gdf_error error;
};


blazing_frame evaluate_split_query(
		std::vector<std::vector<gdf_column_cpp> > input_tables,
		std::vector<std::string> table_names,
		std::vector<std::vector<std::string>> column_names,
		std::vector<std::string> query,
		Context * queryContext,
		int call_depth = 0);

std::string get_named_expression(std::string query_part, std::string expression_name);

void execute_project_plan(blazing_frame & input, std::string query_part);

project_plan_params parse_project_plan(blazing_frame& input, std::string query_part);

void process_project(blazing_frame & input, std::string query_part);

blazing_frame evaluate_query(
		std::vector<ral::io::data_loader > input_loaders,
		std::vector<ral::io::Schema> schemas,
		std::vector<std::string> table_names,
		std::string logicalPlan,
		connection_id_t connection,
        Context& queryContext
  );

void split_inequality_join_into_join_and_filter(const std::string & join_statement, 
 					std::string & new_join_statement, std::string & filter_statement);
           
#endif /* CALCITEINTERPRETER_H_ */
