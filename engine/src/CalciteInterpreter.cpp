#include "CalciteInterpreter.h"

#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>

#include <regex>

#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "communication/network/Server.h"
#include "operators/OrderBy.h"
#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"
#include "parser/expression_tree.hpp"
#include "utilities/DebuggingUtils.h"

#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"
#include "execution_graph/logic_controllers/PhysicalPlanGenerator.h"

#include <spdlog/spdlog.h>

using namespace fmt::literals;


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

std::unique_ptr<ral::frame::BlazingTable> execute_plan(std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::string logicalPlan,
	int64_t connection,
	Context & queryContext)  {

	CodeTimer blazing_timer;
	auto logger = spdlog::get("batch_logger");
	logger->debug("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=queryContext.getContextToken(),
									"step"_a=queryContext.getQueryStep(),
									"substep"_a=queryContext.getQuerySubstep(),
									"info"_a="execute_plan start",
									"duration"_a="");

	try {
		assert(input_loaders.size() == table_names.size());

		std::unique_ptr<ral::frame::BlazingTable> output_frame; 
		ral::batch::tree_processor tree{
			.root = {},
			.context = queryContext.clone(),
			.input_loaders = input_loaders,
			.schemas = schemas,
			.table_names = table_names,
			.transform_operators_bigger_than_gpu = true
		};
		ral::batch::OutputKernel output;

		auto query_graph = tree.build_batch_graph(logicalPlan);
		
		logger->info("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=queryContext.getContextToken(),
									"step"_a=queryContext.getQueryStep(),
									"substep"_a=queryContext.getQuerySubstep(),
									"info"_a="\"Query Start\n{}\""_format(tree.to_string()),
									"duration"_a="");
		
		std::map<std::string, std::string> config_options = queryContext.getConfigOptions();
		// Lets build a string with all the configuration parameters set.
		std::string config_info = "";
		std::map<std::string, std::string>::iterator it = config_options.begin(); 
		while (it != config_options.end())
		{
			config_info += it->first + ": " + it->second + "; ";
			it++;
		}
		logger->info("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=queryContext.getContextToken(),
									"step"_a=queryContext.getQueryStep(),
									"substep"_a=queryContext.getQuerySubstep(),
									"info"_a="\"Config Options: {}\""_format(config_info),
									"duration"_a="");
		
		if (query_graph->num_nodes() > 0) {
			*query_graph += link(query_graph->get_last_kernel(), output, ral::cache::cache_settings{.type = ral::cache::CacheType::CONCATENATING});
			// query_graph.show();
			query_graph->execute();
			output_frame = output.release();
		}

		logger->info("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=queryContext.getContextToken(),
									"step"_a=queryContext.getQueryStep(),
									"substep"_a=queryContext.getQuerySubstep(),
									"info"_a="Query Execution Done",
									"duration"_a=blazing_timer.elapsed_time());

		assert(output_frame != nullptr);

		logger->flush();

		return output_frame;
	} catch(const std::exception& e) {
		logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=queryContext.getContextToken(),
									"step"_a=queryContext.getQueryStep(),
									"substep"_a=queryContext.getQuerySubstep(),
									"info"_a="In execute_plan. What: {}"_format(e.what()),
									"duration"_a="");
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
