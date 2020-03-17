#include "GroupBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "../Interpreter/interpreter_cpp.h"
#include "Traits/RuntimeTraits.h"
#include "communication/CommunicationData.h"
#include "distribution/primitives.h"
#include "utilities/CommonOperations.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <numeric>
#include <regex>
#include <tuple>

#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/groupby.hpp>
#include <cudf/reduction.hpp>
#include <cudf/replace.hpp>
#include <cudf/detail/aggregation/aggregation.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>

namespace ral {
namespace operators {

namespace experimental {


std::vector<int> get_group_columns(std::string query_part) {
	std::string temp_column_string = get_named_expression(query_part, "group");
	if(temp_column_string.size() <= 2) {
		return std::vector<int>();
	}

	// Now we have somethig like {0, 1}
	temp_column_string = temp_column_string.substr(1, temp_column_string.length() - 2);
	std::vector<std::string> column_numbers_string = StringUtil::split(temp_column_string, ",");
	std::vector<int> group_column_indices(column_numbers_string.size());
	for(int i = 0; i < column_numbers_string.size(); i++) {
		group_column_indices[i] = std::stoull(column_numbers_string[i], 0);
	}
	return group_column_indices;
}

typedef blazingdb::manager::experimental::Context Context;
typedef ral::communication::experimental::CommunicationData CommunicationData;
using namespace ral::distribution::experimental;

std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table,
															const std::string& query_part, Context * context) {

		// Get groups
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd - 1);

	std::vector<int> group_column_indices = get_group_columns(combined_expression);

	// Get aggregations
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);
	for(std::string expr : expressions) {
		std::string expression = std::regex_replace(expr, std::regex("^ +| +$|( ) +"), "$1");
		if(expression.find("group=") == std::string::npos) {
			aggregation_expressions.push_back(expression);

			// if the aggregation has an alias, lets capture it here, otherwise we'll figure out what to call the
			// aggregation based on its input
			if(expression.find("EXPR$") == 0)
				aggregation_column_assigned_aliases.push_back("");
			else
				aggregation_column_assigned_aliases.push_back(expression.substr(0, expression.find("=[")));
		}
	}
	if(aggregation_expressions.size() == 0) {
		return groupby_without_aggregations(context, table, group_column_indices);
	} else if (group_column_indices.size() == 0) {
		return aggregations_without_groupby(context, table, aggregation_expressions, aggregation_column_assigned_aliases);
	} else {
		return aggregations_with_groupby(context, table, aggregation_expressions, aggregation_column_assigned_aliases, group_column_indices);
	}
}


std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices) {

	static CodeTimer timer;
	timer.reset();
	if(context->getTotalNodes() <= 1) {
		return compute_groupby_without_aggregations(table, group_column_indices);
	} else {

		size_t total_rows_table = table.view().num_rows();

		ral::frame::BlazingTableView groupbyColumns(table.view().select(group_column_indices), table.names());

		std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamplesFromRatio(
																	groupbyColumns, 0.1);

		Library::Logging::Logger().logInfo(timer.logDuration(*context, "distributed groupby_without_aggregations part 1 generateSample"));

		std::unique_ptr<ral::frame::BlazingTable> grouped_table;
		std::thread groupbyThread{[](Context * context,
								const ral::frame::BlazingTableView & table,
								const std::vector<int> & group_column_indices,
								std::unique_ptr<ral::frame::BlazingTable> & grouped_table) {
								static CodeTimer timer2;
								grouped_table = compute_groupby_without_aggregations(table, group_column_indices);
								Library::Logging::Logger().logInfo(
								 	   timer2.logDuration(*context, "distributed groupby_without_aggregations part 2 async compute_groupby_without_aggregations"));
								timer2.reset();
							},
		context,
		std::ref(table),
		std::ref(group_column_indices),
		std::ref(grouped_table)};

		// std::unique_ptr<ral::frame::BlazingTable> grouped_table = compute_groupby_without_aggregations(table, group_column_indices);

		std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
		if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
			context->incrementQuerySubstep();
			std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > samples_pair = collectSamples(context);

			std::vector<ral::frame::BlazingTableView> samples;
			for (int i = 0; i < samples_pair.first.size(); i++){
				samples.push_back(samples_pair.first[i].second->toBlazingTableView());
			}
			samples.push_back(selfSamples->toBlazingTableView());

			std::vector<size_t> total_rows_tables = samples_pair.second;
			total_rows_tables.push_back(total_rows_table);

			partitionPlan = generatePartitionPlansGroupBy(context, samples);

			context->incrementQuerySubstep();
			distributePartitionPlan(context, partitionPlan->toBlazingTableView());

			Library::Logging::Logger().logInfo(timer.logDuration(
			 	*context, "distributed groupby_without_aggregations part 2 collectSamples generatePartitionPlans distributePartitionPlan"));
		} else {
			context->incrementQuerySubstep();
			sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

			context->incrementQuerySubstep();
			partitionPlan = getPartitionPlan(context);

			Library::Logging::Logger().logInfo(
			 	timer.logDuration(*context, "distributed groupby_without_aggregations part 2 sendSamplesToMaster getPartitionPlan"));

		}

		// Wait for sortThread
		groupbyThread.join();
		timer.reset();  // lets do the reset here, since  part 2 async is capting the time

		if(partitionPlan->view().num_rows() == 0) {
			return std::unique_ptr<ral::frame::BlazingTable>();
		}

		// need to sort the data before its partitioned
		std::vector<cudf::order> column_order(group_column_indices.size(), cudf::order::ASCENDING);
		std::vector<cudf::null_order> null_orders(group_column_indices.size(), cudf::null_order::AFTER);
		CudfTableView groupbyColumnsForSort = grouped_table->view().select(group_column_indices);
		std::unique_ptr<cudf::column> sorted_order_col = cudf::experimental::sorted_order( groupbyColumnsForSort, column_order, null_orders );
		std::unique_ptr<cudf::experimental::table> gathered = cudf::experimental::gather( grouped_table->view(), sorted_order_col->view() );

		ral::frame::BlazingTableView gathered_table(gathered->view(), grouped_table->names());
		grouped_table = nullptr; // lets free grouped_table. We dont need it anymore.

		std::vector<int8_t> sortOrderTypes;
		std::vector<NodeColumnView> partitions = partitionData(
								context, gathered_table, partitionPlan->toBlazingTableView(), group_column_indices, sortOrderTypes);

		context->incrementQuerySubstep();
		distributePartitions(context, partitions);
		std::vector<NodeColumn> collected_partitions = collectPartitions(context);

		std::vector<ral::frame::BlazingTableView> partitions_to_merge;
		for (int i = 0; i < collected_partitions.size(); i++){
			partitions_to_merge.emplace_back(collected_partitions[i].second->toBlazingTableView());
		}
		for (auto partition : partitions){
			if (partition.first == CommunicationData::getInstance().getSelfNode()){
				partitions_to_merge.emplace_back(partition.second);
				break;
			}
		}

		return groupByWithoutAggregationsMerger(partitions_to_merge, group_column_indices);
	}
}

std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices) {

	std::unique_ptr<cudf::experimental::table> output = cudf::experimental::drop_duplicates(table.view(),
		group_column_indices,
		cudf::experimental::duplicate_keep_option::KEEP_FIRST);

	return std::make_unique<ral::frame::BlazingTable>( std::move(output), table.names() );
}


std::unique_ptr<ral::frame::BlazingTable> aggregations_without_groupby(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_expressions,
		const std::vector<std::string> & aggregation_column_assigned_aliases){

	static CodeTimer timer;
	timer.reset();

	std::vector<std::string> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation_string(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}

	std::unique_ptr<ral::frame::BlazingTable> results = compute_aggregations_without_groupby(table, aggregation_types, aggregation_input_expressions,
					aggregation_column_assigned_aliases);

	if(context->getTotalNodes() <= 1) {  // if single node, we can just return
		return results;
	}

	Library::Logging::Logger().logInfo(
	 	timer.logDuration(*context, "aggregations_without_groupby part 1 compute_aggregations"));
	timer.reset();

	if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		context->incrementQuerySubstep();
		std::vector<NodeColumn> collected_partitions = collectPartitions(context);

		std::vector<ral::frame::BlazingTableView> partitions_to_merge;
		for (int i = 0; i < collected_partitions.size(); i++){
			partitions_to_merge.emplace_back(collected_partitions[i].second->toBlazingTableView());
		}
		partitions_to_merge.emplace_back(results->toBlazingTableView());

		std::unique_ptr<BlazingTable> concatenated_aggregations = ral::utilities::experimental::concatTables(partitions_to_merge);

		std::vector<std::string> mod_aggregation_types = aggregation_types;
		std::vector<std::string> mod_aggregation_input_expressions = aggregation_input_expressions;
		for (int i = 0; i < mod_aggregation_types.size(); i++){
			if (mod_aggregation_types[i] == "COUNT"){
				mod_aggregation_types[i] = "SUM"; // if we have a COUNT, we want to SUM the output of the counts from other nodes
			}
			mod_aggregation_input_expressions[i] = std::to_string(i); // we just want to aggregate the input columns, so we are setting the indices here
		}

		std::unique_ptr<ral::frame::BlazingTable> merged_results = compute_aggregations_without_groupby(concatenated_aggregations->toBlazingTableView(),
				mod_aggregation_types, mod_aggregation_input_expressions, concatenated_aggregations->names());

		Library::Logging::Logger().logInfo(timer.logDuration(
		 	*context, "aggregations_without_groupby part 2 collectPartitions and merged"));
		timer.reset();
		return merged_results;
	} else {

		std::vector<NodeColumnView> selfPartition;
		selfPartition.emplace_back(context->getMasterNode(), results->toBlazingTableView());

		context->incrementQuerySubstep();
		ral::distribution::experimental::distributePartitions(context, selfPartition);

		Library::Logging::Logger().logInfo(
		 	timer.logDuration(*context, "aggregations_without_groupby part 2 distributePartitions"));
		timer.reset();

		return std::make_unique<ral::frame::BlazingTable>(cudf::experimental::empty_like(results->view()), results->names());
	}
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_types,
		const std::vector<std::string> & aggregation_input_expressions, const std::vector<std::string> & aggregation_column_assigned_aliases){

	std::vector<std::unique_ptr<cudf::scalar>> reductions;
	std::vector<std::string> agg_output_column_names;
	for (int i = 0; i < aggregation_types.size(); i++){
		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == "COUNT") { // this is a COUNT(*)
			std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
			auto numeric_s = static_cast< cudf::experimental::scalar_type_t<int64_t>* >(scalar.get());
			numeric_s->set_value((int64_t)(table.view().num_rows()));
			reductions.emplace_back(std::move(scalar));
		} else {
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> aggregation_input_scope_holder;
			CudfColumnView aggregation_input;
			if(contains_evaluation(aggregation_input_expressions[i])) {
				aggregation_input_scope_holder = ral::processor::evaluate_expressions(table.toBlazingColumns(), {aggregation_input_expressions[i]});
				aggregation_input = aggregation_input_scope_holder[0]->view();
			} else {
				aggregation_input = table.view().column(get_index(aggregation_input_expressions[i]));
			}

			if( aggregation_types[i] == "COUNT") {
				std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
				auto numeric_s = static_cast< cudf::experimental::scalar_type_t<int64_t>* >(scalar.get());
				numeric_s->set_value((int64_t)(aggregation_input.size() - aggregation_input.null_count()));
				reductions.emplace_back(std::move(scalar));
			} else {
				std::unique_ptr<cudf::experimental::aggregation> agg =
					std::make_unique<cudf::experimental::aggregation>(convertAggregationCudf(get_aggregation_operation(aggregation_types[i])));
				cudf::type_id output_type = get_aggregation_output_type(aggregation_input.type().id(), aggregation_types[i]);
				std::unique_ptr<cudf::scalar> reduction_out = cudf::experimental::reduce(aggregation_input, agg, cudf::data_type(output_type));
				if (aggregation_types[i] == "$SUM0" && !reduction_out->is_valid()){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
					std::unique_ptr<cudf::scalar> zero_scalar = get_scalar_from_string("0", reduction_out->type().id()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
					reductions.emplace_back(std::move(zero_scalar));
				} else {					
					reductions.emplace_back(std::move(reduction_out));
				}
			}
		}

		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
		if(aggregation_column_assigned_aliases[i] == "") {
			if(aggregation_input_expressions[i] == "" && aggregation_types[i] == "COUNT") { // this is a COUNT(*)
				agg_output_column_names.push_back(aggregation_types[i] + "(*)");
			} else {
				agg_output_column_names.push_back(aggregation_types[i] + "(" + table.names().at(get_index(aggregation_input_expressions[i])) + ")");
			}
		} else {
			agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
		}
	}
	// convert scalars into columns
	std::vector<std::unique_ptr<cudf::column>> output_columns;
	for (int i = 0; i < reductions.size(); i++){
		std::unique_ptr<cudf::column> temp = cudf::make_column_from_scalar(*(reductions[i]), 1);
		output_columns.emplace_back(std::move(temp));
	}
	return std::make_unique<ral::frame::BlazingTable>(std::move(std::make_unique<CudfTable>(std::move(output_columns))), agg_output_column_names);
}


std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_expressions,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices) {

	std::vector<AggregateKind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}

	static CodeTimer timer;
	timer.reset();
	if(context->getTotalNodes() <= 1) {
		return compute_aggregations_with_groupby(table, aggregation_types, aggregation_input_expressions,
					aggregation_column_assigned_aliases, group_column_indices);
	} else {
		size_t total_rows_table = table.view().num_rows();

		ral::frame::BlazingTableView groupbyColumns(table.view().select(group_column_indices), table.names());

		std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamplesFromRatio(
																	groupbyColumns, 0.1);

		Library::Logging::Logger().logInfo(timer.logDuration(*context, "distributed aggregations_with_groupby part 1 generateSample"));

		std::unique_ptr<ral::frame::BlazingTable> grouped_table;
		std::thread groupbyThread{[](Context * context,
								const ral::frame::BlazingTableView & table,
								const std::vector<AggregateKind> & aggregation_types,
								const std::vector<std::string> & aggregation_input_expressions,
								const std::vector<std::string> & aggregation_column_assigned_aliases,
								const std::vector<int> & group_column_indices,
								std::unique_ptr<ral::frame::BlazingTable> & grouped_table) {
								static CodeTimer timer2;
								grouped_table = compute_aggregations_with_groupby(table, aggregation_types, aggregation_input_expressions,
													aggregation_column_assigned_aliases, group_column_indices);
								Library::Logging::Logger().logInfo(
								 	   timer2.logDuration(*context, "distributed aggregations_with_groupby part 2 async compute_aggregations_with_groupby"));
								timer2.reset();
							},
		context,
		std::ref(table),
		std::ref(aggregation_types),
		std::ref(aggregation_input_expressions),
		std::ref(aggregation_column_assigned_aliases),
		std::ref(group_column_indices),
		std::ref(grouped_table)};

		// std::unique_ptr<ral::frame::BlazingTable> grouped_table = compute_aggregations_with_groupby(table, aggregation_types, aggregation_input_expressions,
													// aggregation_column_assigned_aliases, group_column_indices);

		std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
		if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
			context->incrementQuerySubstep();
			std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > samples_pair = collectSamples(context);

			std::vector<ral::frame::BlazingTableView> samples;
			for (int i = 0; i < samples_pair.first.size(); i++){
				samples.push_back(samples_pair.first[i].second->toBlazingTableView());
			}
			samples.push_back(selfSamples->toBlazingTableView());

			std::vector<size_t> total_rows_tables = samples_pair.second;
			total_rows_tables.push_back(total_rows_table);

			partitionPlan = generatePartitionPlansGroupBy(context, samples);

			context->incrementQuerySubstep();
			distributePartitionPlan(context, partitionPlan->toBlazingTableView());

			Library::Logging::Logger().logInfo(timer.logDuration(
			 	*context, "distributed aggregations_with_groupby part 3 collectSamples generatePartitionPlans distributePartitionPlan"));
		} else {
			context->incrementQuerySubstep();
			sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

			context->incrementQuerySubstep();
			partitionPlan = getPartitionPlan(context);

			Library::Logging::Logger().logInfo(
			 	timer.logDuration(*context, "distributed aggregations_with_groupby part 3 sendSamplesToMaster getPartitionPlan"));

		}

		// Wait for sortThread
		groupbyThread.join();
		timer.reset();  // lets do the reset here, since  part 2 async is capting the time

		if(partitionPlan->num_rows() == 0) {
			return std::make_unique<ral::frame::BlazingTable>(cudf::experimental::empty_like(grouped_table->view()), grouped_table->names());
		}

		// need to sort the data before its partitioned
		std::vector<cudf::order> column_order(group_column_indices.size(), cudf::order::ASCENDING);
		std::vector<cudf::null_order> null_orders(group_column_indices.size(), cudf::null_order::AFTER);
		CudfTableView groupbyColumnsForSort = grouped_table->view().select(group_column_indices);
		std::unique_ptr<cudf::column> sorted_order_col = cudf::experimental::sorted_order( groupbyColumnsForSort, column_order, null_orders );
		std::unique_ptr<cudf::experimental::table> gathered = cudf::experimental::gather( grouped_table->view(), sorted_order_col->view() );

		ral::frame::BlazingTableView gathered_table(gathered->view(), grouped_table->names());
		grouped_table = nullptr; // lets free grouped_table. We dont need it anymore.

		std::vector<int8_t> sortOrderTypes;
		std::vector<NodeColumnView> partitions = partitionData(
								context, gathered_table, partitionPlan->toBlazingTableView(), group_column_indices, sortOrderTypes);
		context->incrementQuerySubstep();
		distributePartitions(context, partitions);
		std::vector<NodeColumn> collected_partitions = collectPartitions(context);

		std::vector<ral::frame::BlazingTableView> partitions_to_merge;
		for (int i = 0; i < collected_partitions.size(); i++){
			partitions_to_merge.emplace_back(collected_partitions[i].second->toBlazingTableView());
		}
		for (auto partition : partitions){
			if (partition.first == CommunicationData::getInstance().getSelfNode()){
				partitions_to_merge.emplace_back(partition.second);
				break;
			}
		}

		Library::Logging::Logger().logInfo(
			timer.logDuration(*context, "distributed aggregations_with_groupby part 4 collected partitions"));

		std::unique_ptr<BlazingTable> concatenated_aggregations = ral::utilities::experimental::concatTables(partitions_to_merge);

		std::vector<AggregateKind> mod_aggregation_types = aggregation_types;
		std::vector<std::string> mod_aggregation_input_expressions(aggregation_input_expressions.size());
		std::vector<std::string> mod_aggregation_column_assigned_aliases(mod_aggregation_types.size());
		std::vector<int> mod_group_column_indices(group_column_indices.size());
		std::iota(mod_group_column_indices.begin(), mod_group_column_indices.end(), 0);
		for (int i = 0; i < mod_aggregation_types.size(); i++){
			if (mod_aggregation_types[i] == AggregateKind::COUNT_VALID || mod_aggregation_types[i] == AggregateKind::COUNT_ALL){
				mod_aggregation_types[i] = AggregateKind::SUM; // if we have a COUNT, we want to SUM the output of the counts from other nodes
			}
			mod_aggregation_input_expressions[i] = std::to_string(i + mod_group_column_indices.size()); // we just want to aggregate the input columns, so we are setting the indices here
			mod_aggregation_column_assigned_aliases[i] = concatenated_aggregations->names()[i + mod_group_column_indices.size()];
		}

		std::unique_ptr<ral::frame::BlazingTable> merged_results = compute_aggregations_with_groupby(concatenated_aggregations->toBlazingTableView(),
				mod_aggregation_types, mod_aggregation_input_expressions, mod_aggregation_column_assigned_aliases, mod_group_column_indices);

		Library::Logging::Logger().logInfo(
		 	timer.logDuration(*context, "distributed_aggregations_with_groupby part 5 concat and merge"));
		timer.reset();
		return merged_results;
	}
}

	cudf::experimental::aggregation::Kind convertAggregationCudf(AggregateKind input){
		if(input == AggregateKind::SUM){
			return cudf::experimental::aggregation::Kind::SUM;
		}else if(input == AggregateKind::MEAN){
			return cudf::experimental::aggregation::Kind::MEAN;
		}else if(input == AggregateKind::MIN){
			return cudf::experimental::aggregation::Kind::MIN;
		}else if(input == AggregateKind::MAX){
			return cudf::experimental::aggregation::Kind::MAX;
		}else if(input == AggregateKind::COUNT_VALID){
			return cudf::experimental::aggregation::Kind::COUNT_VALID;
		}else if(input == AggregateKind::COUNT_ALL){
			return cudf::experimental::aggregation::Kind::COUNT_ALL;
		}else if(input == AggregateKind::SUM0){
			return cudf::experimental::aggregation::Kind::SUM;
		} 
		throw std::runtime_error(
			"In convertAggregationCudf function: AggregateKind type not supported");
	}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_input_expressions, const std::vector<std::string> & aggregation_column_assigned_aliases,
		const std::vector<int> & group_column_indices) {

	// lets get the unique expressions. This is how many aggregation requests we will need
	std::vector<std::string> unique_expressions = aggregation_input_expressions;
	std::sort( unique_expressions.begin(), unique_expressions.end() );
	auto it = std::unique( unique_expressions.begin(), unique_expressions.end() );
	unique_expressions.resize( std::distance(unique_expressions.begin(),it) );

	// We will iterate over the unique expressions and create an aggregation request for each one.
	// We do it this way, because you could have something like min(colA), max(colA), sum(colA).
	// These three aggregations would all be in one request because they have the same input
	std::vector< std::unique_ptr<ral::frame::BlazingColumn> > aggregation_inputs_scope_holder;
	std::vector<cudf::experimental::groupby::aggregation_request> requests;
	std::vector<int> agg_out_indices;
	std::vector<std::string> agg_output_column_names;
	for (size_t u = 0; u < unique_expressions.size(); u++){
		std::string expression = unique_expressions[u];

		CudfColumnView aggregation_input; // this is the input from which we will crete the aggregation request
		std::vector<std::unique_ptr<cudf::experimental::aggregation>> agg_ops_for_request;
		for (size_t i = 0; i < aggregation_input_expressions.size(); i++){
			if (expression == aggregation_input_expressions[i]){

				int column_index = -1;
				// need to calculate or determine the aggregation input only once
				if (aggregation_input.size() == 0) {
					if(expression == "" && aggregation_types[i] == AggregateKind::COUNT_ALL ) { // this is COUNT(*). Lets just pick the first column
						aggregation_input = table.view().column(0);
					} else if(contains_evaluation(expression)) {
						std::vector< std::unique_ptr<ral::frame::BlazingColumn> > computed_columns = ral::processor::evaluate_expressions(table.toBlazingColumns(), {expression});
						aggregation_inputs_scope_holder.insert(aggregation_inputs_scope_holder.end(), std::make_move_iterator(computed_columns.begin()), std::make_move_iterator(computed_columns.end()));
						aggregation_input = aggregation_inputs_scope_holder.back()->view();
					} else {
						column_index = get_index(expression);
						aggregation_input = table.view().column(column_index);
					}					
				}
				agg_ops_for_request.push_back(std::make_unique<cudf::experimental::aggregation>(convertAggregationCudf(aggregation_types[i])));
				agg_out_indices.push_back(i);  // this is to know what is the desired order of aggregations output

				// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
				if(aggregation_column_assigned_aliases[i] == "") {
					if(aggregation_types[i] == AggregateKind::COUNT_ALL) {  // COUNT(*) case
						agg_output_column_names.push_back("COUNT(*)");
					} else {
						if (column_index == -1){
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + expression + ")");
						} else {
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table.names().at(column_index) + ")");
						}
					}
				} else {
					agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
				}
			}
		}
			requests.push_back(cudf::experimental::groupby::aggregation_request {.values = aggregation_input, .aggregations = std::move(agg_ops_for_request)});
	}

	CudfTableView keys = table.view().select(group_column_indices);
	cudf::experimental::groupby::groupby group_by_obj(keys, cudf::include_nulls::YES);
	std::pair<std::unique_ptr<cudf::experimental::table>, std::vector<cudf::experimental::groupby::aggregation_result>> result = group_by_obj.aggregate( requests );

	// output table is grouped columns and then aggregated columns
	std::vector< std::unique_ptr<cudf::column> > output_columns = result.first->release();
	output_columns.resize(agg_out_indices.size() + group_column_indices.size());

	// lets collect all the aggregated results from the results structure and then add them to output_columns
	std::vector< std::unique_ptr<cudf::column> > agg_cols_out;
	for (int i = 0; i < result.second.size(); i++){
		for (int j = 0; j < result.second[i].results.size(); j++){
			agg_cols_out.emplace_back(std::move(result.second[i].results[j]));
		}
	}
	for (int i = 0; i < agg_out_indices.size(); i++){
		if (aggregation_types[agg_out_indices[i]] == AggregateKind::SUM0 && agg_cols_out[i]->null_count() > 0){
			std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string("0", agg_cols_out[i]->type().id()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
			std::unique_ptr<cudf::column> temp = cudf::experimental::replace_nulls(agg_cols_out[i]->view(), *scalar );
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(temp);
		} else {
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(agg_cols_out[i]);
		}
	}
	std::unique_ptr<CudfTable> output_table = std::make_unique<CudfTable>(std::move(output_columns));

	// lets put together the output names
	std::vector<std::string> output_names;
	for (int i = 0; i < group_column_indices.size(); i++){
		output_names.push_back(table.names()[group_column_indices[i]]);
	}
	output_names.resize(agg_out_indices.size() + group_column_indices.size());
	for (int i = 0; i < agg_out_indices.size(); i++){
		output_names[agg_out_indices[i] + group_column_indices.size()] = agg_output_column_names[i];
	}

	return std::make_unique<BlazingTable>(std::move(output_table), output_names);
}


// Bigger than GPU functions
auto get_group_vars(const std::string & query_part) {
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd - 1);

	std::vector<int> group_column_indices = get_group_columns(combined_expression);

	// Get aggregations
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);
	for(std::string expr : expressions) {
		std::string expression = std::regex_replace(expr, std::regex("^ +| +$|( ) +"), "$1");
		if(expression.find("group=") == std::string::npos) {
			aggregation_expressions.push_back(expression);

			// if the aggregation has an alias, lets capture it here, otherwise we'll figure out what to call the
			// aggregation based on its input
			if(expression.find("EXPR$") == 0)
				aggregation_column_assigned_aliases.push_back("");
			else
				aggregation_column_assigned_aliases.push_back(expression.substr(0, expression.find("=[")));
		}
	}

	return std::make_tuple(group_column_indices, aggregation_expressions, aggregation_column_assigned_aliases);
}

bool is_aggregations_without_groupby(const std::string& query_part) {
	std::vector<int> group_column_indices;
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::tie(group_column_indices, aggregation_expressions, aggregation_column_assigned_aliases) = get_group_vars(query_part);
	
	return group_column_indices.size() == 0;
}

std::vector<int> get_group_column_indices(const std::string& query_part) {
	std::vector<int> group_column_indices;
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::tie(group_column_indices, aggregation_expressions, aggregation_column_assigned_aliases) = get_group_vars(query_part);
	
	return group_column_indices;
}

auto groupby_without_aggregations_and_sample(Context * context,
																							const ral::frame::BlazingTableView & table,
																							const std::vector<int> & group_column_indices) {
	std::unique_ptr<ral::frame::BlazingTable> grouped_table = compute_groupby_without_aggregations(table, group_column_indices);
	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;

	if(context->getTotalNodes() > 1) {
		size_t total_rows_table = table.view().num_rows();
		
		ral::frame::BlazingTableView groupbyColumns(table.view().select(group_column_indices), table.names());
		std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamplesFromRatio(
																															groupbyColumns, 0.1);	

		if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
			context->incrementQuerySubstep();
			std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > samples_pair = collectSamples(context);

			std::vector<ral::frame::BlazingTableView> samples;
			for (int i = 0; i < samples_pair.first.size(); i++){
				samples.push_back(samples_pair.first[i].second->toBlazingTableView());
			}
			samples.push_back(selfSamples->toBlazingTableView());

			std::vector<size_t> total_rows_tables = samples_pair.second;
			total_rows_tables.push_back(total_rows_table);

			partitionPlan = generatePartitionPlansGroupBy(context, samples);

			context->incrementQuerySubstep();
			distributePartitionPlan(context, partitionPlan->toBlazingTableView());
		} else {
			context->incrementQuerySubstep();
			sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

			context->incrementQuerySubstep();
			partitionPlan = getPartitionPlan(context);
		}
	}

	return std::make_pair(std::move(grouped_table), std::move(partitionPlan));
}

auto aggregations_without_groupby_and_sample(Context * context,
																						const ral::frame::BlazingTableView & table,
																						const std::vector<std::string> & aggregation_expressions,
																						const std::vector<std::string> & aggregation_column_assigned_aliases){

	std::vector<std::string> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation_string(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}

	std::unique_ptr<ral::frame::BlazingTable> results = compute_aggregations_without_groupby(table,
																																													aggregation_types,
																																													aggregation_input_expressions,
																																													aggregation_column_assigned_aliases);

	// aggregations_without_groupby does not need a partitionPlan, just return and empty table
	std::unique_ptr<ral::frame::BlazingTable> partitionPlan = std::make_unique<ral::frame::BlazingTable>(
			std::vector<std::unique_ptr<BlazingColumn>>{}, std::vector<std::string>{});

	if(context->getTotalNodes() > 1) {
		if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
			context->incrementQuerySubstep();
			std::vector<NodeColumn> collected_partitions = collectPartitions(context);

			std::vector<ral::frame::BlazingTableView> partitions_to_merge;
			for (int i = 0; i < collected_partitions.size(); i++){
				partitions_to_merge.emplace_back(collected_partitions[i].second->toBlazingTableView());
			}
			partitions_to_merge.emplace_back(results->toBlazingTableView());

			std::unique_ptr<BlazingTable> concatenated_aggregations = ral::utilities::experimental::concatTables(partitions_to_merge);

			std::vector<std::string> mod_aggregation_types = aggregation_types;
			std::vector<std::string> mod_aggregation_input_expressions = aggregation_input_expressions;
			for (int i = 0; i < mod_aggregation_types.size(); i++){
				if (mod_aggregation_types[i] == "COUNT"){
					mod_aggregation_types[i] = "SUM"; // if we have a COUNT, we want to SUM the output of the counts from other nodes
				}
				mod_aggregation_input_expressions[i] = std::to_string(i); // we just want to aggregate the input columns, so we are setting the indices here
			}

			results = compute_aggregations_without_groupby(concatenated_aggregations->toBlazingTableView(),
																										mod_aggregation_types,
																										mod_aggregation_input_expressions,
																										concatenated_aggregations->names());
		} else {
			std::vector<NodeColumnView> selfPartition;
			selfPartition.emplace_back(context->getMasterNode(), results->toBlazingTableView());

			context->incrementQuerySubstep();
			ral::distribution::experimental::distributePartitions(context, selfPartition);

			results = std::make_unique<ral::frame::BlazingTable>(cudf::experimental::empty_like(results->view()), results->names());
		}
	}

	return std::make_pair(std::move(results), std::move(partitionPlan));
}

auto aggregations_with_groupby_and_sample(Context * context,
																					const ral::frame::BlazingTableView & table,
																					const std::vector<std::string> & aggregation_expressions,
																					const std::vector<std::string> & aggregation_column_assigned_aliases,
																					const std::vector<int> & group_column_indices) {
	std::vector<AggregateKind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}

	std::unique_ptr<ral::frame::BlazingTable> grouped_table = compute_aggregations_with_groupby(table,
																																															aggregation_types,
																																															aggregation_input_expressions,
																																															aggregation_column_assigned_aliases,
																																															group_column_indices);
	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;

	if(context->getTotalNodes() > 1) {
		size_t total_rows_table = table.view().num_rows();

		ral::frame::BlazingTableView groupbyColumns(table.view().select(group_column_indices), table.names());
		std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamplesFromRatio(
																															groupbyColumns, 0.1);

		if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
			context->incrementQuerySubstep();
			std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > samples_pair = collectSamples(context);

			std::vector<ral::frame::BlazingTableView> samples;
			for (int i = 0; i < samples_pair.first.size(); i++){
				samples.push_back(samples_pair.first[i].second->toBlazingTableView());
			}
			samples.push_back(selfSamples->toBlazingTableView());

			std::vector<size_t> total_rows_tables = samples_pair.second;
			total_rows_tables.push_back(total_rows_table);

			partitionPlan = generatePartitionPlansGroupBy(context, samples);

			context->incrementQuerySubstep();
			distributePartitionPlan(context, partitionPlan->toBlazingTableView());
		} else {
			context->incrementQuerySubstep();
			sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

			context->incrementQuerySubstep();
			partitionPlan = getPartitionPlan(context);
		}
	}

	return std::make_pair(std::move(grouped_table), std::move(partitionPlan));
}

std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable>>
group_and_sample(const ral::frame::BlazingTableView & table, const std::string& query_part, Context * context) {
	std::vector<int> group_column_indices;
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::tie(group_column_indices, aggregation_expressions, aggregation_column_assigned_aliases) = get_group_vars(query_part);

	if(aggregation_expressions.size() == 0) {
		return groupby_without_aggregations_and_sample(context, table, group_column_indices);
	} else if (group_column_indices.size() == 0) {
		return aggregations_without_groupby_and_sample(context, table, aggregation_expressions, aggregation_column_assigned_aliases);
	} else {
		return aggregations_with_groupby_and_sample(context, table, aggregation_expressions, aggregation_column_assigned_aliases, group_column_indices);
	}
}

auto partition_groupby_aggregations(Context * context,
																		const ral::frame::BlazingTableView & grouped_table,
																		const ral::frame::BlazingTableView & partitionPlan,
																		const std::vector<int> & group_column_indices) {
	if(partitionPlan.num_rows() == 0) {
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge;
		partitions_to_merge.push_back(grouped_table.clone());
		return partitions_to_merge;
	}
			
	// need to sort the data before its partitioned
	std::vector<cudf::order> column_order(group_column_indices.size(), cudf::order::ASCENDING);
	std::vector<cudf::null_order> null_orders(group_column_indices.size(), cudf::null_order::AFTER);
	CudfTableView groupbyColumnsForSort = grouped_table.view().select(group_column_indices);
	std::unique_ptr<cudf::column> sorted_order_col = cudf::experimental::sorted_order( groupbyColumnsForSort, column_order, null_orders );
	std::unique_ptr<cudf::experimental::table> gathered = cudf::experimental::gather( grouped_table.view(), sorted_order_col->view() );

	ral::frame::BlazingTableView gathered_table(gathered->view(), grouped_table.names());

	std::vector<int8_t> sortOrderTypes;
	std::vector<NodeColumnView> partitions = partitionData(
							context, gathered_table, partitionPlan, group_column_indices, sortOrderTypes);

	context->incrementQuerySubstep();
	distributePartitions(context, partitions);
	std::vector<NodeColumn> collected_partitions = collectPartitions(context);

	std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge;
	for (int i = 0; i < collected_partitions.size(); i++){
		partitions_to_merge.emplace_back(std::move(collected_partitions[i].second));
	}
	for (auto partition : partitions){
		if (partition.first == CommunicationData::getInstance().getSelfNode()){
			std::unique_ptr<ral::frame::BlazingTable> table = partition.second.clone();
			partitions_to_merge.push_back(std::move(table));
			break;
		}
	}

	return partitions_to_merge;
}

std::vector<std::unique_ptr<ral::frame::BlazingTable>>
partition_group(const ral::frame::BlazingTableView & partitionPlan,
								const ral::frame::BlazingTableView & grouped_table,
								const std::string & query_part,
								Context * context) {
	std::vector<int> group_column_indices;
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::tie(group_column_indices, aggregation_expressions, aggregation_column_assigned_aliases) = get_group_vars(query_part);

	if (group_column_indices.size() == 0) {
		// should not reach here
		// aggregations_without_groupby does not need partitions
		assert(false);
	}

	return partition_groupby_aggregations(context, grouped_table, partitionPlan, group_column_indices);
}

auto merge_groupby_without_aggregations(Context * context,
																				const std::vector<ral::frame::BlazingTableView> & partitions_to_merge,
																				const std::vector<int> & group_column_indices) {
	return groupByWithoutAggregationsMerger(partitions_to_merge, group_column_indices);
}


auto merge_aggregations_with_groupby(Context * context,
																			const std::vector<ral::frame::BlazingTableView> & partitions_to_merge,
																			const std::vector<std::string> & aggregation_expressions,
																			const std::vector<int> & group_column_indices) {
	std::vector<AggregateKind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}

	std::unique_ptr<BlazingTable> concatenated_aggregations = ral::utilities::experimental::concatTables(partitions_to_merge);

	std::vector<AggregateKind> mod_aggregation_types = aggregation_types;
	std::vector<std::string> mod_aggregation_input_expressions(aggregation_input_expressions.size());
	std::vector<std::string> mod_aggregation_column_assigned_aliases(aggregation_types.size());
	std::vector<int> mod_group_column_indices(group_column_indices.size());
	std::iota(mod_group_column_indices.begin(), mod_group_column_indices.end(), 0);
	for (int i = 0; i < aggregation_types.size(); i++){
		if (aggregation_types[i] == AggregateKind::COUNT_VALID || aggregation_types[i] == AggregateKind::COUNT_ALL){
			aggregation_types[i] = AggregateKind::SUM; // if we have a COUNT, we want to SUM the output of the counts from other nodes
		}
		mod_aggregation_input_expressions[i] = std::to_string(i + mod_group_column_indices.size()); // we just want to aggregate the input columns, so we are setting the indices here
		mod_aggregation_column_assigned_aliases[i] = concatenated_aggregations->names()[i + mod_group_column_indices.size()];
	}

	std::unique_ptr<ral::frame::BlazingTable> merged_results = compute_aggregations_with_groupby(concatenated_aggregations->toBlazingTableView(),
			aggregation_types, mod_aggregation_input_expressions, mod_aggregation_column_assigned_aliases, mod_group_column_indices);

	return merged_results;
}

std::unique_ptr<ral::frame::BlazingTable>
merge_group(const std::vector<ral::frame::BlazingTableView> & partitions_to_merge,
						const std::string & query_part,
						Context * context) {
	std::vector<int> group_column_indices;
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::tie(group_column_indices, aggregation_expressions, aggregation_column_assigned_aliases) = get_group_vars(query_part);

	if (group_column_indices.size() == 0) {
		// should not reach here
		// aggregations_without_groupby does not need merge
		assert(false);
	}

	if(aggregation_expressions.size() == 0) {
		return merge_groupby_without_aggregations(context, partitions_to_merge, group_column_indices);
	} else {
		return merge_aggregations_with_groupby(context, partitions_to_merge, aggregation_expressions, group_column_indices);
	}

}

}  // namespace experimental
}  // namespace operators
}  // namespace ral
