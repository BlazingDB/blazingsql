#include "CalciteInterpreter.h"

#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>

#include <algorithm>
#include <regex>
#include <set>
#include <string>
#include <thread>

#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "ColumnManipulation.cuh"
#include "Interpreter/interpreter_cpp.h"
#include "JoinProcessor.h"
#include "ResultSetRepository.h"
#include "Traits/RuntimeTraits.h"
#include "Utils.cuh"
#include "communication/network/Server.h"
#include "config/GPUManager.cuh"
#include "cuDF/safe_nvcategory_gather.hpp"
#include "cudf/legacy/binaryop.hpp"
#include "io/DataLoader.h"
#include "legacy/groupby.hpp"
#include "legacy/reduction.hpp"
#include "legacy/stream_compaction.hpp"
#include "operators/GroupBy.h"
#include "operators/JoinOperator.h"
#include "operators/OrderBy.h"
#include "utilities/CommonOperations.h"
#include "utilities/RalColumn.h"
#include "utilities/StringUtils.h"
#include <cudf/filling.hpp>
#include <cudf/legacy/table.hpp>
#include <rmm/thrust_rmm_allocator.h>
#include "parser/expression_tree.hpp"
#include "Interpreter/interpreter_cpp.h"

#include <cudf/column/column_factories.hpp>

const std::string LOGICAL_JOIN_TEXT = "LogicalJoin";
const std::string LOGICAL_UNION_TEXT = "LogicalUnion";
const std::string LOGICAL_SCAN_TEXT = "LogicalTableScan";
const std::string BINDABLE_SCAN_TEXT = "BindableTableScan";
const std::string LOGICAL_AGGREGATE_TEXT = "LogicalAggregate";
const std::string LOGICAL_PROJECT_TEXT = "LogicalProject";
const std::string LOGICAL_SORT_TEXT = "LogicalSort";
const std::string LOGICAL_FILTER_TEXT = "LogicalFilter";
const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";

bool is_union(std::string query_part) { return (query_part.find(LOGICAL_UNION_TEXT) != std::string::npos); }

bool is_project(std::string query_part) { return (query_part.find(LOGICAL_PROJECT_TEXT) != std::string::npos); }

bool is_logical_scan(std::string query_part) { return (query_part.find(LOGICAL_SCAN_TEXT) != std::string::npos); }

bool is_bindable_scan(std::string query_part) { return (query_part.find(BINDABLE_SCAN_TEXT) != std::string::npos); }

bool is_filtered_bindable_scan(std::string query_part) {
	return is_bindable_scan(query_part) && (query_part.find("filters") != std::string::npos);
}

bool is_scan(std::string query_part) { return is_logical_scan(query_part) || is_bindable_scan(query_part); }


bool is_filter(std::string query_part) { return (query_part.find(LOGICAL_FILTER_TEXT) != std::string::npos); }

int count_string_occurrence(std::string haystack, std::string needle) {
	int position = haystack.find(needle, 0);
	int count = 0;
	while(position != std::string::npos) {
		count++;
		position = haystack.find(needle, position + needle.size());
	}

	return count;
}

bool is_double_input(std::string query_part) {
	if(ral::operators::is_join(query_part)) {
		return true;
	} else if(is_union(query_part)) {
		return true;
	} else {
		return false;
	}
}

// Input: [[hr, emps]] or [[emps]] Output: hr.emps or emps
std::string extract_table_name(std::string query_part) {
	size_t start = query_part.find("[[") + 2;
	size_t end = query_part.find("]]");
	std::string table_name_text = query_part.substr(start, end - start);
	std::vector<std::string> table_parts = StringUtil::split(table_name_text, ',');
	std::string table_name = "";
	for(int i = 0; i < table_parts.size(); i++) {
		if(table_parts[i][0] == ' ') {
			table_parts[i] = table_parts[i].substr(1, table_parts[i].size() - 1);
		}
		table_name += table_parts[i];
		if(i != table_parts.size() - 1) {
			table_name += ".";
		}
	}

	return table_name;
}

project_plan_params parse_project_plan(const ral::frame::BlazingTableView & table, std::string query_part) {
	using interops::column_index_type;

	gdf_error err = GDF_SUCCESS;

	size_t size = table.num_columns();

	// LogicalProject(x=[$0], y=[$1], z=[$2], e=[$3], join_x=[$4], y0=[$5], EXPR$6=[+($0, $5)])
	std::string combined_expression =
		query_part.substr(query_part.find("(") + 1, (query_part.rfind(")") - query_part.find("(")) - 1);

	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);

	// now we have a vector
	// x=[$0
	//std::vector<bool> input_used_in_output(input.get_width(), false);

	std::vector<gdf_column_cpp> columns(expressions.size());
	std::vector<std::string> names(expressions.size());


	std::vector<column_index_type> final_output_positions;
	std::vector<std::unique_ptr<cudf::column>> output_columns;
	std::vector<std::unique_ptr<cudf::column>> input_columns;

	// TODO: some of this code could be used to extract columns
	// that will be projected to make the csv and parquet readers
	// be able to ignore columns that are not
	// TODO percy cudf0.12 was invalid here, should we consider empty?
	cudf::type_id max_temp_type = cudf::type_id::EMPTY;
	std::vector<cudf::type_id> output_type_expressions(
		expressions.size());  // contains output types for columns that are expressions, if they are not expressions we
							  // skip over it

	size_t num_expressions_out = 0;
	//std::vector<bool> input_used_in_expression(input.get_size_column(), false);

	// for(int i = 0; i < expressions.size(); i++) {  // last not an expression
	// 	std::string expression = expressions[i].substr(
	// 		expressions[i].find("=[") + 2, (expressions[i].size() - expressions[i].find("=[")) - 3);

	// 	std::string name = expressions[i].substr(0, expressions[i].find("=["));

	// 	if(contains_evaluation(expression)) {
	// 		//output_type_expressions[i] = get_output_type_expression(&input, &max_temp_type, expression);

	// 		// todo put this into its own function
	// 		std::string clean_expression = clean_calcite_expression(expression);

	// 		std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	// 		fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(table.view(), tokens);
	// 		for(std::string token : tokens) {
	// 			if(!is_operator_token(token) && !is_literal(token)) {
	// 				size_t index = get_index(token);
	// 				input_used_in_expression[index] = true;
	// 			}
	// 		}
	// 		num_expressions_out++;
	// 	}
	// }

	// create allocations for output on seperate thread

	// std::vector<column_index_type> new_column_indices(input_used_in_expression.size());
	// for(int i = 0; i < input_used_in_expression.size(); i++) {
	// 	if(input_used_in_expression[i]) {
	// 		new_column_indices[i] = input_columns.size();
	// 		// TODO percy jp cudf0.12 project
	// 		//input_columns.push_back(table.view().column(i));
	// 	} else {
	// 		new_column_indices[i] = -1;  // won't be uesd anyway
	// 	}
	// }

	// TODO: this shit is all super hacky in here we should clean it up
	// std::vector<column_index_type> left_inputs;
	// std::vector<column_index_type> right_inputs;
	// std::vector<column_index_type> outputs;

	// std::vector<gdf_binary_operator_exp> operators;
	// std::vector<gdf_unary_operator> unary_operators;

	// std::vector<cudf::scalar*> left_scalars;
	// std::vector<cudf::scalar*> right_scalars;
	// size_t cur_expression_out = 0;
	// for(int i = 0; i < expressions.size(); i++) {  // last not an expression
	// 	std::string expression = expressions[i].substr(
	// 		expressions[i].find("=[") + 2, (expressions[i].size() - expressions[i].find("=[")) - 3);

	// 	std::string name = expressions[i].substr(0, expressions[i].find("=["));

	// 	if(contains_evaluation(expression)) {
	// 		final_output_positions.push_back(input_columns.size() + final_output_positions.size());

	// 		// TODO Percy Rommel Jean Pierre improve timestamp resolution
	// 		// assumes worst possible case allocation for output
	// 		// TODO: find a way to know what our output size will be

	// 		std::unique_ptr<cudf::column> output_temp;

	// 		switch (output_type_expressions[i]) {
	// 			case cudf::type_id::INT8:
	// 			case cudf::type_id::INT16:
	// 			case cudf::type_id::INT32:
	// 			case cudf::type_id::INT64:
	// 			case cudf::type_id::FLOAT32:
	// 			case cudf::type_id::FLOAT64: {
	// 				//output_temp = cudf::make_numeric_column(cudf::data_type(output_type_expressions[i]), row_size);
	// 				break;
	// 			}
	// 			// TODO percy cudf0.12 jp strings and dates cases cc rommel
	// 		}
			
	// 		output_columns.push_back(std::move(output_temp));

			// TODO percy jp cudf0.12 project
//			add_expression_to_plan(table,
//				input_columns,
//				expression,
//				cur_expression_out,
//				num_expressions_out,
//				input_columns.size(),
//				left_inputs,
//				right_inputs,
//				outputs,
//				operators,
//				unary_operators,
//				left_scalars,
//				right_scalars,
//				new_column_indices,
//				final_output_positions,
//				output.get_gdf_column());
//			cur_expression_out++;
//			columns[i] = output;
		// } else {
		// 	// TODO percy this code is duplicated inside get_index, refactor get_index
		// 	const std::string cleaned_expression = clean_calcite_expression(expression);
		// 	const bool is_literal_col = is_literal(cleaned_expression);

		// 	if(is_literal_col) {
		// 		int index = i;
		// 		cudf::type_id col_type = infer_dtype_from_literal(cleaned_expression);

		// 		output_type_expressions[i] = col_type;
		// 		std::vector< std::unique_ptr<cudf::column> > output_holder;

		// 		if(col_type == cudf::type_id::CATEGORY) {
		// 			//const std::string literal_expression = cleaned_expression.substr(1, cleaned_expression.size() - 2);
		// 			//NVCategory * new_category = repeated_string_category(literal_expression, row_size);
		// 			//output.create_gdf_column(new_category, row_size, name);
		// 		} else {
		// 			int column_width = ral::traits::get_dtype_size_in_bytes(col_type);
		// 			output.create_gdf_column(col_type, row_size, nullptr, column_width);
		// 			std::unique_ptr<cudf::scalar> literal_scalar = get_scalar_from_string(cleaned_expression);
		// 			output.set_name(name);
					
		// 			// TODO percy cudf0.12 port to cudf::column
		// 			//cudf::fill(output.get_gdf_column(), to_gdf_scalar(literal_scalar), 0, row_size);
		// 		}

				// TODO percy jp cudf0.12 project
				//output_columns.push_back(output.get_gdf_column());
				//input_used_in_output[index] = false;
				//columns[i] = output;
			// } else {
			// 	int index = get_index(expression);
				// TODO percy jp cudf0.12 project
//				gdf_column_cpp output = input.get_column(index);
//				output.set_name(name);
//				input_used_in_output[index] = true;
//				columns[i] = output;
	// 		}
	// 	}
	// }

	// free_gdf_column(&temp);
	// TODO percy jp cudf0.12 project
//	return project_plan_params{num_expressions_out,
//		output_columns,
//		input_columns,
//		left_inputs,
//		right_inputs,
//		outputs,
//		final_output_positions,
//		operators,
//		unary_operators,
//		left_scalars,
//		right_scalars,
//		new_column_indices,
//		columns,
//		err};
}

void execute_project_plan(blazing_frame & input, std::string query_part) {
	// project_plan_params params = parse_project_plan(input, query_part);

	// perform operations
	// if(params.num_expressions_out > 0) {
	// 	size_t size = params.input_columns[0]->size();

	// 	if(size > 0) {
			// perform_operation(params.output_columns,
			// 	params.input_columns,
			// 	params.left_inputs,
			// 	params.right_inputs,
			// 	params.outputs,
			// 	params.final_output_positions,
			// 	params.operators,
			// 	params.unary_operators,
			// 	params.left_scalars,
			// 	params.right_scalars);
	// 	}
	// }

	// input.clear();
	// input.add_table(params.columns);

	// for(size_t i = 0; i < input.get_width(); i++) {
		// TODO percy cudf0.12 port to cudf::column
		//input.get_column(i).update_null_count();
	// }
}

blazing_frame process_union(blazing_frame & left, blazing_frame & right, std::string query_part) {
	bool isUnionAll = (get_named_expression(query_part, "all") == "true");
	if(!isUnionAll) {
		throw std::runtime_error{"In process_union function: UNION is not supported, use UNION ALL"};
	}

	// Check same number of columns
	if(left.get_size_column(0) != right.get_size_column(0)) {
		throw std::runtime_error{
			"In process_union function: left frame and right frame have different number of columns"};
	}

	// Check columns have the same data type
	size_t ncols = left.get_size_column(0);
	for(size_t i = 0; i < ncols; i++) {
		if(left.get_column(i).get_gdf_column()->type().id() != right.get_column(i).get_gdf_column()->type().id()) {
			throw std::runtime_error{"In process_union function: left column and right column have different dtypes"};
		}
	}

	// Check to see if one of the tables is empty
	if(left.get_num_rows_in_table(0) == 0)
		return right;
	else if(right.get_num_rows_in_table(0) == 0)
		return left;

	std::vector<gdf_column_cpp> new_table = ral::utilities::concatTables({left.get_table(0), right.get_table(0)});

	blazing_frame result_frame;
	result_frame.add_table(new_table);

	return result_frame;
}

std::vector<int> get_group_columns(std::string query_part) {
	std::string temp_column_string = get_named_expression(query_part, "group");
	if(temp_column_string.size() <= 2) {
		return std::vector<int>();
	}
	// now you have somethig like {0, 1}
	temp_column_string = temp_column_string.substr(1, temp_column_string.length() - 2);
	std::vector<std::string> column_numbers_string = StringUtil::split(temp_column_string, ",");
	std::vector<int> group_columns_indices(column_numbers_string.size());
	for(int i = 0; i < column_numbers_string.size(); i++) {
		group_columns_indices[i] = std::stoull(column_numbers_string[i], 0);
	}
	return group_columns_indices;
}


/*
This function will take a join_statement and if it contains anything that is not an equijoin, it will try to break it up into an equijoin (new_join_statement) and a filter (filter_statement)
If its just an equijoin, then the new_join_statement will just be join_statement and filter_statement will be empty

Examples:
Basic case:
join_statement = LogicalJoin(condition=[=($3, $0)], joinType=[inner])
new_join_statement = LogicalJoin(condition=[=($3, $0)], joinType=[inner])
filter_statement = ""

Simple case:
join_statement = LogicalJoin(condition=[AND(=($3, $0), >($5, $2))], joinType=[inner])
new_join_statement = LogicalJoin(condition=[=($3, $0)], joinType=[inner])
filter_statement = LogicalFilter(condition=[>($5, $2)])

Complex case:
join_statement = LogicalJoin(condition=[AND(=($7, $0), OR(AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5)))], joinType=[inner])
new_join_statement = LogicalJoin(condition=[=($7, $0)], joinType=[inner])
filter_statement = LogicalFilter(condition=[OR(AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5))])

Error case:
join_statement = LogicalJoin(condition=[OR(=($7, $0), AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5))], joinType=[inner])
Should throw an error

Error case:
join_statement = LogicalJoin(condition=[AND(<($7, $0), >($7, $1)], joinType=[inner])
Should throw an error

*/
void split_inequality_join_into_join_and_filter(const std::string & join_statement, std::string & new_join_statement, std::string & filter_statement){
	new_join_statement = join_statement;
	filter_statement = "";

	std::string condition = get_named_expression(join_statement, "condition");
	std::string join_type = get_named_expression(join_statement, "joinType");

	ral::parser::parse_tree condition_tree;
	condition_tree.build(condition);
	std::string new_join_statement_expression, filter_statement_expression;
	condition_tree.split_inequality_join_into_join_and_filter(new_join_statement_expression, filter_statement_expression);

	new_join_statement = "LogicalJoin(condition=[" + new_join_statement_expression + "], joinType=[" + join_type + "])";
	if (filter_statement_expression != ""){
		filter_statement = "LogicalFilter(condition=[" + filter_statement_expression + "])";
	} else {
		filter_statement = "";
	}
}

// Returns the index from table if exists
size_t get_table_index(std::vector<std::string> table_names, std::string table_name) {
	if(StringUtil::beginsWith(table_name, "main.")) {
		table_name = table_name.substr(5);
	}

	auto it = std::find(table_names.begin(), table_names.end(), table_name);
	if(it != table_names.end()) {
		return std::distance(table_names.begin(), it);
	} else {
		throw std::invalid_argument("table name does not exists ==>" + table_name);
	}
}

// TODO: if a table needs to be used more than once you need to include it twice
// i know that kind of sucks, its for the 0 copy stuff, this can easily be remedied
// by changings scan to make copies
blazing_frame evaluate_split_query(std::vector<std::vector<gdf_column_cpp>> input_tables,
	std::vector<std::string> table_names,
	std::vector<std::vector<std::string>> column_names,
	std::vector<std::string> query,
	Context * queryContext,
	int call_depth) {
	assert(input_tables.size() == table_names.size());

	CodeTimer blazing_timer;
	blazing_timer.reset();

	if(query.size() == 1) {
		// process yourself and return

		if(is_scan(query[0])) {
			blazing_frame scan_frame;
			// EnumerableTableScan(table=[[hr, joiner]])
			scan_frame.add_table(input_tables[get_table_index(table_names, extract_table_name(query[0]))]);
			return scan_frame;
		} else {
			// i dont think there are any other type of end nodes at the moment
		}
	}

	if(is_double_input(query[0])) {
		int other_depth_one_start = 2;
		for(int i = 2; i < query.size(); i++) {
			int j = 0;
			while(query[i][j] == ' ') {
				j += 2;
			}
			int depth = (j / 2) - call_depth;
			if(depth == 1) {
				other_depth_one_start = i;
			}
		}
		// these shoudl be split up and run on different threads
		blazing_frame left_frame;
		left_frame = evaluate_split_query(input_tables,
			table_names,
			column_names,
			std::vector<std::string>(query.begin() + 1, query.begin() + other_depth_one_start),
			queryContext,
			call_depth + 1);

		blazing_frame right_frame;
		right_frame = evaluate_split_query(input_tables,
			table_names,
			column_names,
			std::vector<std::string>(query.begin() + other_depth_one_start, query.end()),
			queryContext,
			call_depth + 1);

		blazing_frame result_frame;

		if(ral::operators::is_join(query[0])) {
			// we know that left and right are dataframes we want to join together
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			int numLeft = left_frame.get_num_rows_in_table(0);
			int numRight = right_frame.get_num_rows_in_table(0);
			left_frame.add_table(right_frame.get_table(0));
			/// left_frame.consolidate_tables();
			result_frame = ral::operators::process_join(queryContext, left_frame, query[0]);
			std::string extraInfo =
				"left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_join",
				"num rows result",
				result_frame.get_num_rows_in_table(0),
				extraInfo));
			blazing_timer.reset();
			return result_frame;
		} else if(is_union(query[0])) {
			// TODO: append the frames to each other
			// return right_frame;//!!
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			int numLeft = left_frame.get_num_rows_in_table(0);
			int numRight = right_frame.get_num_rows_in_table(0);
			result_frame = process_union(left_frame, right_frame, query[0]);
			std::string extraInfo =
				"left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_union",
				"num rows result",
				result_frame.get_num_rows_in_table(0),
				extraInfo));
			blazing_timer.reset();
			return result_frame;
		} else {
			throw std::runtime_error{"In evaluate_split_query function: unsupported query operator"};
		}

	} else {
		// process child
		blazing_frame child_frame = evaluate_split_query(input_tables,
			table_names,
			column_names,
			std::vector<std::string>(query.begin() + 1, query.end()),
			queryContext,
			call_depth + 1);
		// process self
		if(is_project(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			execute_project_plan(child_frame, query[0]);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_project",
				"num rows",
				child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			return child_frame;
		} else if(ral::operators::is_aggregate(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// TODO percy cudf0.12 evaluate_query aggregate
			//ral::operators::process_aggregate(child_frame, query[0], queryContext);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_aggregate",
				"num rows",
				child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			return child_frame;
		} else if(ral::operators::is_sort(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			ral::operators::process_sort(child_frame, query[0], queryContext);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(
				*queryContext, "evaluate_split_query process_sort", "num rows", child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			return child_frame;
		} else if(is_filter(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// process_filter(queryContext, child_frame, query[0]);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_filter",
				"num rows",
				child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			return child_frame;
		} else {
			throw std::runtime_error{"In evaluate_split_query function: unsupported query operator"};
		}
		// return frame
	}
}

blazing_frame evaluate_split_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::vector<std::string> query,
	Context * queryContext,
	int call_depth = 0) {
	assert(input_loaders.size() == table_names.size());

	CodeTimer blazing_timer;
	blazing_timer.reset();

	if(query.size() == 1) {
		// process yourself and return

		if(is_scan(query[0])) {
			blazing_frame scan_frame;
			std::vector<gdf_column_cpp> input_table;

			size_t table_index = get_table_index(table_names, extract_table_name(query[0]));
			if(is_bindable_scan(query[0])) {
				blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
				std::string project_string = get_named_expression(query[0], "projects");
				std::vector<std::string> project_string_split =
					get_expressions_from_expression_list(project_string, true);

				std::string aliases_string = get_named_expression(query[0], "aliases");
				std::vector<std::string> aliases_string_split =
					get_expressions_from_expression_list(aliases_string, true);

				std::vector<size_t> projections;
				for(int i = 0; i < project_string_split.size(); i++) {
					projections.push_back(std::stoull(project_string_split[i]));
				}

				// This is for the count(*) case, we don't want to load all the columns
				if(projections.size() == 0 && aliases_string_split.size() == 1) {
					projections.push_back(0);
				}
				std::unique_ptr<ral::frame::BlazingTable> new_blaz_table = input_loaders[table_index].load_data(queryContext, projections, schemas[table_index]);

				// Setting the aliases only when is not an empty set
				for(size_t col_idx = 0; col_idx < aliases_string_split.size(); col_idx++) {
					// TODO: Rommel, this check is needed when for example the scan has not projects but there are extra
					// aliases
					if(col_idx < input_table.size()) {
						input_table[col_idx].set_name(aliases_string_split[col_idx]);
					}
				}
				int num_rows = new_blaz_table->num_rows();
				Library::Logging::Logger().logInfo(
					blazing_timer.logDuration(*queryContext, "evaluate_split_query load_data", "num rows", num_rows));
				blazing_timer.reset();

				if(is_filtered_bindable_scan(query[0])) {
					scan_frame.add_table(input_table);
					// process_filter(queryContext, scan_frame, query[0]);
					Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
						"evaluate_split_query process_filter",
						"num rows",
						scan_frame.get_num_rows_in_table(0)));
					blazing_timer.reset();
					queryContext->incrementQueryStep();
					return scan_frame;
				}
			} else {
				blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
				std::unique_ptr<ral::frame::BlazingTable> new_blaz_table = input_loaders[table_index].load_data(queryContext, {}, schemas[table_index]);
				int num_rows = new_blaz_table->num_rows();
				Library::Logging::Logger().logInfo(
					blazing_timer.logDuration(*queryContext, "evaluate_split_query load_data", "num rows", num_rows));
				blazing_timer.reset();
			}


			// EnumerableTableScan(table=[[hr, joiner]])
			scan_frame.add_table(input_table);
			queryContext->incrementQueryStep();
			return scan_frame;
		} else {
			// i dont think there are any other type of end nodes at the moment
		}
	}

	if(is_double_input(query[0])) {
		int other_depth_one_start = 2;
		for(int i = 2; i < query.size(); i++) {
			int j = 0;
			while(query[i][j] == ' ') {
				j += 2;
			}
			int depth = (j / 2) - call_depth;
			if(depth == 1) {
				other_depth_one_start = i;
			}
		}
		// these shoudl be split up and run on different threads
		blazing_frame left_frame;
		left_frame = evaluate_split_query(input_loaders,
			schemas,
			table_names,
			std::vector<std::string>(query.begin() + 1, query.begin() + other_depth_one_start),
			queryContext,
			call_depth + 1);

		blazing_frame right_frame;
		right_frame = evaluate_split_query(input_loaders,
			schemas,
			table_names,
			std::vector<std::string>(query.begin() + other_depth_one_start, query.end()),
			queryContext,
			call_depth + 1);

		blazing_frame result_frame;
		if(ral::operators::is_join(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// we know that left and right are dataframes we want to join together
			int numLeft = left_frame.get_num_rows_in_table(0);
			int numRight = right_frame.get_num_rows_in_table(0);
			left_frame.add_table(right_frame.get_table(0));
			///left_frame.consolidate_tables();
			std::string new_join_statement, filter_statement;
			split_inequality_join_into_join_and_filter(query[0], new_join_statement, filter_statement);
			result_frame = ral::operators::process_join(queryContext, left_frame, new_join_statement);
			std::string extraInfo = "left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext, "evaluate_split_query process_join", "num rows result", result_frame.get_num_rows_in_table(0), extraInfo));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			if (filter_statement != ""){
				// process_filter(queryContext, result_frame,filter_statement);
				Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext, "evaluate_split_query inequality join process_filter", "num rows", result_frame.get_num_rows_in_table(0)));
				blazing_timer.reset();
				queryContext->incrementQueryStep();
			}
			return result_frame;
		} else if(is_union(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// TODO: append the frames to each other
			// return right_frame;//!!
			int numLeft = left_frame.get_num_rows_in_table(0);
			int numRight = right_frame.get_num_rows_in_table(0);
			result_frame = process_union(left_frame, right_frame, query[0]);
			std::string extraInfo =
				"left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_union",
				"num rows result",
				result_frame.get_num_rows_in_table(0),
				extraInfo));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return result_frame;
		} else {
			throw std::runtime_error{"In evaluate_split_query function: unsupported query operator"};
		}

	} else {
		// process child
		blazing_frame child_frame = evaluate_split_query(input_loaders,
			schemas,
			table_names,
			std::vector<std::string>(query.begin() + 1, query.end()),
			queryContext,
			call_depth + 1);
		// process self
		if(is_project(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			execute_project_plan(child_frame, query[0]);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_project",
				"num rows",
				child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return child_frame;
		} else if(ral::operators::is_aggregate(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// TODO percy cudf0.12 evaluate_query aggregate
			//ral::operators::process_aggregate(child_frame, query[0], queryContext);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_aggregate",
				"num rows",
				child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return child_frame;
		} else if(ral::operators::is_sort(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			ral::operators::process_sort(child_frame, query[0], queryContext);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(
				*queryContext, "evaluate_split_query process_sort", "num rows", child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return child_frame;
		} else if(is_filter(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// process_filter(queryContext, child_frame, query[0]);
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_filter",
				"num rows",
				child_frame.get_num_rows_in_table(0)));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return child_frame;
		} else {
			throw std::runtime_error{"In evaluate_split_query function: unsupported query operator"};
		}
		// return frame
	}
}

query_token_t evaluate_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::string logicalPlan,
	connection_id_t connection,
	Context & queryContext,
	query_token_t token) {
	std::thread t([=] {
		CodeTimer blazing_timer;

		std::vector<std::string> splitted = StringUtil::split(logicalPlan, "\n");
		if(splitted[splitted.size() - 1].length() == 0) {
			splitted.erase(splitted.end() - 1);
		}

		try {
			Context context = queryContext;
			blazing_frame output_frame = evaluate_split_query(input_loaders, schemas, table_names, splitted, &context);

			// REMOVE any columns that were ipcd to put into the result set
			for(size_t index = 0; index < output_frame.get_size_column(); index++) {
				gdf_column_cpp output_column = output_frame.get_column(index);

				// TODO percy cudf0.12 port to cudf::column
				//if(output_column.is_ipc() || output_column.has_token()) {
				//	output_frame.set_column(index, output_column.clone(output_column.name()));
				//}
			}
			double duration = blazing_timer.getDuration();
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(queryContext, "Query Execution Done"));

			result_set_repository::get_instance().update_token(token, output_frame, duration);

			Library::Logging::Logger().logInfo(blazing_timer.logDuration(queryContext, "Query Done"));
		} catch(const std::exception & e) {
			std::cerr << "evaluate_split_query error => " << e.what() << '\n';
			try {
				result_set_repository::get_instance().update_token(token, blazing_frame{}, 0.0, e.what());
			} catch(const std::exception & e) {
				std::cerr << "error => " << e.what() << '\n';
			}
		}

		ral::communication::network::Server::getInstance().deregisterContext(queryContext.getContextToken());
	});

	//@todo: hablar con felipe sobre detach
	t.detach();

	return token;
}



blazing_frame evaluate_query(
		std::vector<ral::io::data_loader > input_loaders,
		std::vector<ral::io::Schema> schemas,
		std::vector<std::string> table_names,
		std::string logicalPlan,
		connection_id_t connection,
		Context& queryContext
		){

		CodeTimer blazing_timer;

		Library::Logging::Logger().logInfo(blazing_timer.logDuration(queryContext, "\"Query Start\n" + logicalPlan + "\""));

		std::vector<std::string> splitted = StringUtil::split(logicalPlan, "\n");
		if (splitted[splitted.size() - 1].length() == 0) {
			splitted.erase(splitted.end() -1);
		}

		try {
			blazing_frame output_frame = evaluate_split_query(input_loaders, schemas,table_names, splitted, &queryContext);
			output_frame.deduplicate();
			for (size_t i=0;i<output_frame.get_width();i++) {
				if (output_frame.get_column(i).get_gdf_column()->type().id() == cudf::type_id::STRING) {
					NVStrings * new_strings = nullptr;
					if (output_frame.get_column(i).get_gdf_column()->size() > 0) {
						// TODO percy cudf0.12 custrings this was not commented
//						NVCategory* new_category = static_cast<NVCategory *> (output_frame.get_column(i).dtype_info().category)->gather_and_remap( static_cast<int *>(output_frame.get_column(i).data()), output_frame.get_column(i).size());
//						new_strings = new_category->to_strings();
//						NVCategory::destroy(new_category);
					} else {
						new_strings = NVStrings::create_from_array(nullptr, 0);
					}

					gdf_column_cpp string_column;
					string_column.create_gdf_column(new_strings, output_frame.get_column(i).get_gdf_column()->size(), output_frame.get_column(i).name());

					output_frame.set_column(i, string_column);
				}

				// TODO percy cudf0.12 port to cudf::column
				//GDFRefCounter::getInstance()->deregister_column(output_frame.get_column(i).get_gdf_column());
			}

			double duration = blazing_timer.getDuration();
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(queryContext, "Query Execution Done"));

			return output_frame;
		} catch(const std::exception& e) {
			std::string err = "ERROR: in evaluate_split_query " + std::string(e.what());
			Library::Logging::Logger().logError(ral::utilities::buildLogString(std::to_string(queryContext.getContextToken()), std::to_string(queryContext.getQueryStep()), std::to_string(queryContext.getQuerySubstep()), err));
			throw;
		}
}


void getTableScanInfo(std::string & logicalPlan_in,
						std::vector<std::string> & relational_algebra_steps_out,
						std::vector<std::string> & table_names_out,
						std::vector<std::vector<int>> & table_columns_out){

	std::vector<std::string> splitted = StringUtil::split(logicalPlan_in, "\n");
	if (splitted[splitted.size() - 1].length() == 0) {
		splitted.erase(splitted.end() -1);
	}

	for (auto step : splitted){
		if (is_scan(step)) {
			relational_algebra_steps_out.push_back(step);

			std::string table_name = extract_table_name(step);
			if(StringUtil::beginsWith(table_name, "main.")) {
				table_name = table_name.substr(5);
			}
			table_names_out.push_back(table_name);

			if (is_bindable_scan(step)) {
				std::string projects = get_named_expression(step, "projects");
				std::vector<std::string> column_index_strings = get_expressions_from_expression_list(projects, true);
				std::vector<int> column_indeces;
				std::transform(column_index_strings.begin(), column_index_strings.end(), std::back_inserter(column_indeces), [](const std::string& str) { return std::stoi(str); });
				table_columns_out.push_back(column_indeces);
			}else if (is_scan(step)){
				table_columns_out.push_back({});
			}
		}
	}
}
