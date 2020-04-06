
#ifndef CALCITEINTERPRETER_H_
#define CALCITEINTERPRETER_H_

#include "Interpreter/interpreter_cpp.h"
#include "cudf/legacy/binaryop.hpp"
#include "io/DataLoader.h"
#include <iostream>
#include <string>
#include <vector>

#include <blazingdb/manager/Context.h>
using blazingdb::manager::experimental::Context;

std::unique_ptr<ral::frame::BlazingTable> evaluate_split_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::vector<std::string> query,
	Context * queryContext,
	int call_depth = 0);

std::unique_ptr<ral::frame::BlazingTable> evaluate_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::string logicalPlan,
	int64_t connection,
	Context & queryContext);

std::unique_ptr<ral::frame::BlazingTable> execute_plan(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::string logicalPlan,
	int64_t connection,
	Context & queryContext);


void split_inequality_join_into_join_and_filter(const std::string & join_statement, 
 					std::string & new_join_statement, std::string & filter_statement);

void getTableScanInfo(std::string & logicalPlan_in, 
						std::vector<std::string> & relational_algebra_steps_out,
						std::vector<std::string> & table_names_out,
						std::vector<std::vector<int>> & table_columns_out);

#endif /* CALCITEINTERPRETER_H_ */
