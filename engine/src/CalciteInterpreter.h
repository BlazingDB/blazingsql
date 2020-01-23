
#ifndef CALCITEINTERPRETER_H_
#define CALCITEINTERPRETER_H_

#include "DataFrame.h"
#include "../Interpreter/interpreter_cpp.h"
#include "Types.h"
#include "cudf/legacy/binaryop.hpp"
#include "io/DataLoader.h"
#include <iostream>
#include <string>
#include <vector>

#include <blazingdb/manager/Context.h>
using blazingdb::manager::experimental::Context;

struct project_plan_params {
	size_t num_expressions_out;
	std::vector<cudf::column *> output_columns;
	std::vector<cudf::column *> input_columns;
	std::vector<interops::column_index_type> left_inputs;
	std::vector<interops::column_index_type> right_inputs;
	std::vector<interops::column_index_type> outputs;
	std::vector<interops::column_index_type> final_output_positions;
	std::vector<gdf_binary_operator_exp> operators;
	std::vector<gdf_unary_operator> unary_operators;
	// TODO percy cudf0.12 implement proper scalar support
	std::vector<cudf::scalar*> left_scalars;
	std::vector<cudf::scalar*> right_scalars;
	std::vector<interops::column_index_type> new_column_indices;
	std::vector<gdf_column_cpp> columns;
	gdf_error error;
};

ral::frame::TableViewPair evaluate_split_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::vector<std::string> query,
	Context * queryContext,
	int call_depth = 0);

void execute_project_plan(blazing_frame & input, std::string query_part);

project_plan_params parse_project_plan(blazing_frame & input, std::string query_part);

project_plan_params parse_project_plan(const ral::frame::BlazingTableView & table, std::string query_part);

void process_project(blazing_frame & input, std::string query_part);

std::unique_ptr<ral::frame::BlazingTable> evaluate_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::string logicalPlan,
	connection_id_t connection,
	Context & queryContext);

void split_inequality_join_into_join_and_filter(const std::string & join_statement, 
 					std::string & new_join_statement, std::string & filter_statement);

void getTableScanInfo(std::string & logicalPlan_in, 
						std::vector<std::string> & relational_algebra_steps_out,
						std::vector<std::string> & table_names_out,
						std::vector<std::vector<int>> & table_columns_out);

#endif /* CALCITEINTERPRETER_H_ */
