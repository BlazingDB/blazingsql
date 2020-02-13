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
#include "Interpreter/interpreter_cpp.h"
#include "Traits/RuntimeTraits.h"
#include "Utils.cuh"
#include "communication/network/Server.h"
#include "config/GPUManager.cuh"
#include "io/DataLoader.h"
#include "operators/GroupBy.h"
#include "operators/OrderBy.h"
#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"
#include <cudf/filling.hpp>
#include "parser/expression_tree.hpp"
#include "Interpreter/interpreter_cpp.h"

#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <cudf/column/column_factories.hpp>
#include "execution_graph/logic_controllers/LogicalProject.h"

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

bool is_sort(std::string query_part) { return (query_part.find(LOGICAL_SORT_TEXT) != std::string::npos); }

bool is_join(const std::string & query) { return (query.find(LOGICAL_JOIN_TEXT) != std::string::npos); }


bool is_double_input(std::string query_part) {
	if(is_join(query_part)) {
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

std::unique_ptr<ral::frame::BlazingTable> process_union(const ral::frame::BlazingTableView & left, const ral::frame::BlazingTableView & right, std::string query_part) {
	bool isUnionAll = (get_named_expression(query_part, "all") == "true");
	if(!isUnionAll) {
		throw std::runtime_error{"In process_union function: UNION is not supported, use UNION ALL"};
	}

	// Check same number of columns
	if(left.num_columns() != right.num_columns()) {
		throw std::runtime_error{
			"In process_union function: left frame and right frame have different number of columns"};
	}
	std::vector<ral::frame::BlazingTableView> tables{left, right};
	return ral::utilities::experimental::concatTables(tables);
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


ral::frame::TableViewPair evaluate_split_query(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::vector<std::string> query,
	Context * queryContext,
	int call_depth) {

	// MAIN SPLIT

	assert(input_loaders.size() == table_names.size());

	CodeTimer blazing_timer;
	blazing_timer.reset();

	if(query.size() == 1) {
		// process yourself and return

		if(is_scan(query[0])) {
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
				std::unique_ptr<ral::frame::BlazingTable> input_table;
				ral::frame::BlazingTableView input_table_view;
				std::tie(input_table, input_table_view) = input_loaders[table_index].load_data(queryContext, projections, schemas[table_index]);

				std::vector<std::string> col_names = input_table_view.names();

				// Setting the aliases only when is not an empty set
				for(size_t col_idx = 0; col_idx < aliases_string_split.size(); col_idx++) {
					// TODO: Rommel, this check is needed when for example the scan has not projects but there are extra
					// aliases
					if(col_idx < input_table_view.num_columns()) {
						col_names[col_idx] = aliases_string_split[col_idx];
					}
				}
				if(input_table){ // the BlazingTable is not guaranteed to have something
					input_table->setNames(col_names);
				}
				input_table_view.setNames(col_names);

				int num_rows = input_table_view.num_rows();
				Library::Logging::Logger().logInfo(
					blazing_timer.logDuration(*queryContext, "evaluate_split_query load_data", "num rows", num_rows));
				blazing_timer.reset();

				if(is_filtered_bindable_scan(query[0])) {
					const std::string query_part = query[0];
					input_table = ral::processor::process_filter(input_table_view, query_part, queryContext);
					input_table_view = input_table->toBlazingTableView();

					Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
						"evaluate_split_query process_filter",
						"num rows",
						input_table_view.num_rows()));

					blazing_timer.reset();
					queryContext->incrementQueryStep();
					return std::make_pair(std::move(input_table), input_table_view);
				} else {
					queryContext->incrementQueryStep();
					return std::make_pair(std::move(input_table), input_table_view);
				}
			} else {
				blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
				ral::frame::TableViewPair input_table_pair = input_loaders[table_index].load_data(queryContext, {}, schemas[table_index]);

				queryContext->incrementQueryStep();
				int num_rows = input_table_pair.second.num_rows();
				Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext, "evaluate_split_query load_data", "num rows", num_rows));
				blazing_timer.reset();
				return input_table_pair;
			}
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
		ral::frame::TableViewPair left_frame_pair = evaluate_split_query(input_loaders,
			schemas,
			table_names,
			std::vector<std::string>(query.begin() + 1, query.begin() + other_depth_one_start),
			queryContext,
			call_depth + 1);

		ral::frame::TableViewPair right_frame_pair = evaluate_split_query(input_loaders,
			schemas,
			table_names,
			std::vector<std::string>(query.begin() + other_depth_one_start, query.end()),
			queryContext,
			call_depth + 1);

		if(is_join(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
			// we know that left and right are dataframes we want to join together
			int numLeft = left_frame_pair.second.num_rows();
			int numRight = right_frame_pair.second.num_rows();

			std::string new_join_statement, filter_statement;
			StringUtil::findAndReplaceAll(query[0], "IS NOT DISTINCT FROM", "=");
			split_inequality_join_into_join_and_filter(query[0], new_join_statement, filter_statement);

			//result_frame = ral::operators::process_join(queryContext, left_frame, new_join_statement);
			std::unique_ptr<ral::frame::BlazingTable> result_frame = ral::processor::process_logical_join(queryContext, left_frame_pair.second, right_frame_pair.second, new_join_statement);
			std::string extraInfo = "left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);

			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext, "evaluate_split_query process_join", "num rows result", result_frame->num_rows(), extraInfo));
			blazing_timer.reset();
			queryContext->incrementQueryStep();
			if (filter_statement != ""){
				result_frame = ral::processor::process_filter(result_frame->toBlazingTableView(), filter_statement, queryContext);
				//process_filter(queryContext, result_frame,filter_statement);

				Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext, "evaluate_split_query inequality join process_filter", "num rows", result_frame->num_rows()));

				blazing_timer.reset();
				queryContext->incrementQueryStep();
			}
			ral::frame::BlazingTableView result_frame_view = result_frame->toBlazingTableView();
			return std::make_pair(std::move(result_frame), result_frame_view);
		} else if(is_union(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query

			// return right_frame;//!!
			int numLeft = left_frame_pair.second.num_rows();
			int numRight = right_frame_pair.second.num_rows();

			ral::frame::TableViewPair result_pair;
			if (numLeft == 0){
				result_pair = std::move(right_frame_pair);
			} else if (numRight == 0) {
				result_pair = std::move(left_frame_pair);
			} else {
				std::unique_ptr<ral::frame::BlazingTable> result_frame = process_union(left_frame_pair.second, right_frame_pair.second, query[0]);
				ral::frame::BlazingTableView result_frame_view = result_frame->toBlazingTableView();
				result_pair = std::make_pair(std::move(result_frame), result_frame_view);
			}
			std::string extraInfo =	"left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
				Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
						"evaluate_split_query process_union", "num rows result", result_pair.second.num_rows(), extraInfo));
				blazing_timer.reset();
				queryContext->incrementQueryStep();

			return result_pair;
		} else {
			throw std::runtime_error{"In evaluate_split_query function: unsupported query operator"};
		}

	} else {
		// process child
		std::unique_ptr<ral::frame::BlazingTable> child_frame;
		ral::frame::BlazingTableView child_frame_view;
		std::tie(child_frame, child_frame_view) = evaluate_split_query(input_loaders,
			schemas,
			table_names,
			std::vector<std::string>(query.begin() + 1, query.end()),
			queryContext,
			call_depth + 1);
		// process self
		if(is_project(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query

			if (child_frame_view.num_columns()) {
				child_frame = ral::processor::process_project(child_frame_view, query[0], queryContext);
				child_frame_view = child_frame->toBlazingTableView();

				Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
					"evaluate_split_query process_project",
					"num rows",
					child_frame->num_rows()));
			}

			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return std::make_pair(std::move(child_frame), child_frame_view);

		} else if(ral::operators::experimental::is_aggregate(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query

			child_frame = ral::operators::experimental::process_aggregate(child_frame_view, query[0], queryContext);
			child_frame_view = child_frame->toBlazingTableView();

			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_aggregate",
				"num rows",
				child_frame->num_rows()));

			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return std::make_pair(std::move(child_frame), child_frame_view);
		} else if(is_sort(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query

			child_frame = ral::operators::experimental::process_sort(child_frame_view, query[0], queryContext);
			child_frame_view = child_frame->toBlazingTableView();
			//ral::operators::process_sort(child_frame, query[0], queryContext);

			// TODO percy cudf0.12 log logs
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(
				*queryContext, "evaluate_split_query process_sort", "num rows", child_frame->num_rows()));

			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return std::make_pair(std::move(child_frame), child_frame_view);
		} else if(is_filter(query[0])) {
			blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query

			child_frame = ral::processor::process_filter(child_frame->toBlazingTableView(), query[0], queryContext);
			child_frame_view = child_frame->toBlazingTableView();
			//process_filter(queryContext, child_frame, query[0]);

			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*queryContext,
				"evaluate_split_query process_filter",
				"num rows",
				child_frame->num_rows()));

			blazing_timer.reset();
			queryContext->incrementQueryStep();
			return std::make_pair(std::move(child_frame), child_frame_view);
		} else {
			throw std::runtime_error{"In evaluate_split_query function: unsupported query operator"};
		}
		// return frame
	}
}

std::unique_ptr<ral::frame::BlazingTable> evaluate_query(
		std::vector<ral::io::data_loader > input_loaders,
		std::vector<ral::io::Schema> schemas,
		std::vector<std::string> table_names,
		std::string logicalPlan,
		int64_t connection,
		Context& queryContext
		){

		CodeTimer blazing_timer;

		Library::Logging::Logger().logInfo(blazing_timer.logDuration(queryContext, "\"Query Start\n" + logicalPlan + "\""));

		std::vector<std::string> splitted = StringUtil::split(logicalPlan, "\n");
		if (splitted[splitted.size() - 1].length() == 0) {
			splitted.erase(splitted.end() -1);
		}

		try {
			std::unique_ptr<ral::frame::BlazingTable> output_frame;
			ral::frame::BlazingTableView output_frame_view;
			std::tie(output_frame, output_frame_view) = evaluate_split_query(input_loaders, schemas,table_names, splitted, &queryContext);

			if (output_frame == nullptr){
				output_frame = output_frame_view.clone();
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
