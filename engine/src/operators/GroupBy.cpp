#include "GroupBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "ColumnManipulation.cuh"
#include "GDFColumn.cuh"
#include "../Interpreter/interpreter_cpp.h"
#include "Traits/RuntimeTraits.h"
#include "communication/CommunicationData.h"
#include "config/GPUManager.cuh"
#include "distribution/primitives.h"
#include "utilities/CommonOperations.h"
#include "utilities/RalColumn.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <numeric>
#include <regex>
#include <tuple>

#include "cudf/legacy/groupby.hpp"
#include "cudf/legacy/reduction.hpp"
#include <cudf/legacy/table.hpp>
#include <rmm/thrust_rmm_allocator.h>

#include "cuDF/safe_nvcategory_gather.hpp"

#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/groupby.hpp>
#include <cudf/reduction.hpp>
#include <cudf/detail/aggregation/aggregation.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>

namespace ral {
namespace operators {

namespace {
using blazingdb::manager::experimental::Context;
}  // namespace

const std::string LOGICAL_AGGREGATE_TEXT = "LogicalAggregate";

bool is_aggregate(std::string query_part) { return (query_part.find(LOGICAL_AGGREGATE_TEXT) != std::string::npos); }

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

cudf::reduction::operators gdf_agg_op_to_reduction_operators(const gdf_agg_op agg_op) {
	switch(agg_op) {
	case GDF_SUM: return cudf::reduction::operators::SUM;
	case GDF_MIN: return cudf::reduction::operators::MIN;
	case GDF_MAX: return cudf::reduction::operators::MAX;
	default: throw std::runtime_error("In gdf_agg_op_to_reduction_operators: Unexpected gdf_agg_op");
	}
}

cudf::groupby::operators gdf_agg_op_to_groupby_operators(const gdf_agg_op agg_op) {
	switch(agg_op) {
	case GDF_SUM: return cudf::groupby::SUM;
	case GDF_MIN: return cudf::groupby::MIN;
	case GDF_MAX: return cudf::groupby::MAX;
	case GDF_COUNT: return cudf::groupby::COUNT;
	case GDF_AVG: return cudf::groupby::MEAN;
	case GDF_COUNT_DISTINCT: throw std::runtime_error("COUNT DISTINCT currently not supported with a group by");
	default: throw std::runtime_error("Groupby Operator currently not supported with a group by");
	}
}


void distributed_groupby_without_aggregations(
	Context & queryContext, blazing_frame & input, std::vector<int> & group_column_indices) {
	using ral::communication::experimental::CommunicationData;
	static CodeTimer timer;
	timer.reset();

	std::vector<gdf_column_cpp> group_columns(group_column_indices.size());
	for(size_t i = 0; i < group_column_indices.size(); i++) {
		group_columns[i] = input.get_column(group_column_indices[i]);
	}
	std::vector<gdf_column_cpp> data_cols_in(input.get_width());
	for(int i = 0; i < input.get_width(); i++) {
		data_cols_in[i] = input.get_column(i);
	}

	size_t rowSize = input.get_num_rows_in_table(0);

	std::vector<gdf_column_cpp> selfSamples = ral::distribution::sampling::generateSample(group_columns, 0.1);
	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_groupby_without_aggregations part 0 generateSample"));
	timer.reset();

	auto groupByTask = std::async(
		std::launch::async,
		[](Context & queryContext, std::vector<gdf_column_cpp> & input, const std::vector<int> & group_column_indices) {
			static CodeTimer timer2;
			// TODO percy william alex port distribution
//			std::vector<gdf_column_cpp> result = groupby_without_aggregations(input, group_column_indices);
//			Library::Logging::Logger().logInfo(timer.logDuration(
//				queryContext, "distributed_groupby_without_aggregations part 1 async groupby_without_aggregations"));
//			timer.reset();
//			return result;
		},
		std::ref(queryContext),
		std::ref(data_cols_in),
		std::ref(group_column_indices));

	std::vector<gdf_column_cpp> partitionPlan;
	if(queryContext.isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//std::vector<ral::distribution::NodeSamples> samples = ral::distribution::collectSamples(queryContext);
		//samples.emplace_back(rowSize, CommunicationData::getInstance().getSelfNode(), selfSamples);
		//partitionPlan = ral::distribution::generatePartitionPlansGroupBy(queryContext, samples);

		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//ral::distribution::experimental::distributePartitionPlan(queryContext, partitionPlan);

		Library::Logging::Logger().logInfo(timer.logDuration(queryContext,
			"distributed_groupby_without_aggregations part 1 collectSamples generatePartitionPlansGroupBy "
			"distributePartitionPlan"));
		timer.reset();
	} else {
		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//ral::distribution::experimental::sendSamplesToMaster(queryContext, selfSamples, rowSize);

		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//partitionPlan = ral::distribution::experimental::getPartitionPlan(queryContext);

		Library::Logging::Logger().logInfo(timer.logDuration(
			queryContext, "distributed_groupby_without_aggregations part 1 sendSamplesToMaster getPartitionPlan"));
		timer.reset();
	}
	
	// TODO percy william alex port distribution
//	// Wait for groupByThread
//	std::vector<gdf_column_cpp> groupedTable = groupByTask.get();
//	queryContext.incrementQuerySubstep();

//	if(partitionPlan[0].get_gdf_column()->size() == 0) {
//		return;
//	}

//	std::vector<ral::distribution::NodeColumns> partitions =
//		ral::distribution::partitionData(queryContext, groupedTable, group_column_indices, partitionPlan, false);
//	Library::Logging::Logger().logInfo(
//		timer.logDuration(queryContext, "distributed_groupby_without_aggregations part 2 partitionData"));
//	timer.reset();

//	queryContext.incrementQuerySubstep();
//	ral::distribution::distributePartitions(queryContext, partitions);
//	std::vector<ral::distribution::NodeColumns> partitionsToMerge = ral::distribution::collectPartitions(queryContext);

//	auto it = std::find_if(partitions.begin(), partitions.end(), [&](ral::distribution::NodeColumns & el) {
//		return el.getNode() == CommunicationData::getInstance().getSelfNode();
//	});
//	// Could "it" iterator be partitions.end()?
//	partitionsToMerge.push_back(*it);

//	Library::Logging::Logger().logInfo(timer.logDuration(
//		queryContext, "distributed_groupby_without_aggregations part 3 distributePartitions collectPartitions"));
//	timer.reset();

//	ral::distribution::groupByWithoutAggregationsMerger(partitionsToMerge, group_column_indices, input);

//	Library::Logging::Logger().logInfo(timer.logDuration(
//		queryContext, "distributed_groupby_without_aggregations part 4 groupByWithoutAggregationsMerger"));
//	timer.reset();
}

void aggregations_with_groupby(std::vector<CudfColumnView> const & group_by_columns,
   std::vector<cudf::column_view> const & aggregation_inputs,
   std::vector<cudf::experimental::aggregation::Kind> const & agg_ops,
   std::vector< std::unique_ptr<cudf::column> > & group_by_output_columns,
   std::vector< std::unique_ptr<cudf::column> > & aggregation_output_columns) {

   std::vector<cudf::experimental::groupby::aggregation_request> requests;
   requests.reserve(agg_ops.size());

   for(size_t i = 0 ; i < agg_ops.size() ; ++i){
	   requests.push_back(cudf::experimental::groupby::aggregation_request {.values = aggregation_inputs[i]});
	   requests[i].aggregations.reserve(agg_ops.size());

	   requests[i].aggregations.push_back(std::make_unique<cudf::experimental::aggregation>(agg_ops[i]));

	   cudf::table_view keys {{group_by_columns}};
	   cudf::experimental::groupby::groupby obj(keys);
	   
	   std::pair<std::unique_ptr<cudf::experimental::table>, std::vector<cudf::experimental::groupby::aggregation_result>> output = obj.aggregate( requests );

	   if (output.first->num_columns()>0) {
		   std::vector< std::unique_ptr<cudf::column> > gby_cols = output.first->release();
		   group_by_output_columns.push_back(std::move(gby_cols[0]));
		   aggregation_output_columns.push_back(std::move(output.second[0].results[0]));
	   }
   }
}

void aggregations_without_groupby(const std::vector<cudf::experimental::aggregation::Kind> & agg_ops,
	std::vector<CudfColumnView> & aggregation_inputs,
	std::vector< std::unique_ptr<cudf::column> > & output_columns,
	const std::vector<cudf::type_id> & output_types,
	const std::vector<std::string> & output_column_names) {
	for(size_t i = 0; i < agg_ops.size(); i++) {
		switch(agg_ops[i]) {
		case cudf::experimental::aggregation::Kind::SUM:
		case cudf::experimental::aggregation::Kind::MIN:
		case cudf::experimental::aggregation::Kind::MAX:
			if(aggregation_inputs[i].size() == 0) {
				// Set output_column data to invalid
				std::unique_ptr<cudf::scalar> null_value;
				
				// TODO percy cudf0.12 implement proper scalar support
				//null_value.is_valid = false;
				//null_value.dtype = to_gdf_type(output_types[i]);
				//output_columns[i].create_gdf_column(null_value, output_column_names[i]);
				
				break;
			} else {
				
				// TODO percy cudf0.12 implement proper scalar support
				//cudf::reduction::operators reduction_op = gdf_agg_op_to_reduction_operators(agg_ops[i]);
				//std::unique_ptr<cudf::scalar> reduction_out =
				//	cudf::reduce(aggregation_inputs[i].get_gdf_column(), reduction_op, to_gdf_type(output_types[i]));
				//output_columns[i].create_gdf_column(reduction_out, output_column_names[i]);
				
				break;
			}
		case cudf::experimental::aggregation::Kind::MEAN:
			if(aggregation_inputs[i].size() == 0 ||
				(aggregation_inputs[i].size() == aggregation_inputs[i].null_count())) {
				// Set output_column data to invalid
				
				// TODO percy cudf0.12 implement proper scalar support
				//std::unique_ptr<cudf::scalar> null_value;
				//null_value.is_valid = false;
				//null_value.dtype = to_gdf_type(output_types[i]);
				//output_columns[i].create_gdf_column(null_value, output_column_names[i]);
				
				break;
			} else {
				cudf::type_id sum_output_type = get_aggregation_output_type(aggregation_inputs[i].type().id(), cudf::experimental::aggregation::Kind::SUM, false);
				
				// TODO percy cudf0.12 implement proper scalar support
				//std::unique_ptr<cudf::scalar> avg_sum_scalar = cudf::reduce(
				//	aggregation_inputs[i].get_gdf_column(), cudf::reduction::operators::SUM, to_gdf_type(sum_output_type));
				
				long avg_count = aggregation_inputs[i].size() - aggregation_inputs[i].null_count();

				assert(output_types[i] == GDF_FLOAT64);
				assert(sum_output_type == GDF_INT64 || sum_output_type == GDF_FLOAT64);

				// TODO percy cudf0.12 implement proper scalar support
				//std::unique_ptr<cudf::scalar> avg_scalar;
				//avg_scalar.dtype = GDF_FLOAT64;
				//avg_scalar.is_valid = true;
				//if(avg_sum_scalar.dtype == GDF_INT64)
				//	avg_scalar.data.fp64 = (double) avg_sum_scalar.data.si64 / (double) avg_count;
				//else
				//	avg_scalar.data.fp64 = (double) avg_sum_scalar.data.fp64 / (double) avg_count;
				//output_columns[i].create_gdf_column(avg_scalar, output_column_names[i]);
				
				break;
			}
		case cudf::experimental::aggregation::Kind::COUNT: {
			// TODO percy cudf0.12 implement proper scalar support
			//std::unique_ptr<cudf::scalar> reduction_out;
			//reduction_out.dtype = GDF_INT64;
			//reduction_out.is_valid = true;
			//reduction_out.data.si64 =
			//	aggregation_inputs[i].get_gdf_column()->size - aggregation_inputs[i].get_gdf_column()->null_count;
			//output_columns[i].create_gdf_column(reduction_out, output_column_names[i]);
			
			break;
		}
		// TODO percy cudf0.12 aggregation COUNT_DISTINCT cases
//		case GDF_COUNT_DISTINCT: {
//			throw std::runtime_error("COUNT DISTINCT currently not supported without a group by");
//		}
		}
	}
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations(const ral::frame::BlazingTableView & table,
	std::vector<int> & group_column_indices,
	std::vector<cudf::experimental::aggregation::Kind> const & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	size_t row_size = table.view().column(0).size();

	std::vector<CudfColumnView> group_by_columns(group_column_indices.size());
	for(size_t i = 0; i < group_column_indices.size(); i++) {
		group_by_columns[i] = table.view().column(group_column_indices[i]);
	}

	std::vector< CudfColumnView > aggregation_inputs(aggregation_types.size());
	std::vector< std::unique_ptr<cudf::column> > aggregation_inputs_scope_holder(aggregation_types.size());
	std::vector<cudf::type_id> output_types(aggregation_types.size());
	std::vector<std::string> output_column_names(aggregation_types.size());

	for(size_t i = 0; i < aggregation_types.size(); i++) {
		std::string expression = aggregation_input_expressions[i];

		// this means we have a COUNT(*). So lets create a simple column with no nulls
		if(expression == "" && aggregation_types[i] == cudf::experimental::aggregation::Kind::COUNT ) {
			std::unique_ptr<cudf::column> temp = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT8), row_size);
			std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT8));
			auto numeric_s = static_cast< cudf::experimental::scalar_type_t<int8_t>* >(scalar.get());
			numeric_s->set_value(0);
			cudf::experimental::fill(temp->mutable_view(), 0, temp->size(), *scalar);
			aggregation_inputs_scope_holder[i] = std::move(temp);
			aggregation_inputs[i] = aggregation_inputs_scope_holder[i]->view();
		} else {
			if(contains_evaluation(expression)) {
				// we dont knwo what the size of this input will be so allcoate max size
				// TODO de donde saco el nombre de la columna aqui???
				cudf::type_id unused;
				cudf::type_id agg_input_type = get_output_type_expression(table, unused, expression);
				
				std::unique_ptr<cudf::column> temp;
				
				switch (agg_input_type) {
					case cudf::type_id::INT8:
					case cudf::type_id::INT16:
					case cudf::type_id::INT32:
					case cudf::type_id::INT64:
					case cudf::type_id::FLOAT32:
					case cudf::type_id::FLOAT64: {
						temp = cudf::make_numeric_column(cudf::data_type(agg_input_type), row_size);
						break;
					}
					// TODO percy cudf0.12 jp strings and dates cases
				}
				
				aggregation_inputs_scope_holder[i] = std::move(temp);
				aggregation_inputs[i] = aggregation_inputs_scope_holder[i]->view();

				// TODO percy rommel jp port evaluate expression
				//evaluate_expression(table, expression, aggregation_inputs[i]);
			} else {
				aggregation_inputs[i] = table.view().column(get_index(expression));
			}
		}

		output_types[i] = get_aggregation_output_type(aggregation_inputs[i].type().id(), aggregation_types[i], group_column_indices.size() != 0);

		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
		if(aggregation_column_assigned_aliases[i] == "") {
			if(expression == "" && aggregation_types[i] == cudf::experimental::aggregation::Kind::COUNT) {  // COUNT(*) case
				output_column_names[i] = aggregator_to_string(aggregation_types[i]) + "(*)";
			} else {
				output_column_names[i] =
					aggregator_to_string(aggregation_types[i]) + "(" + table.names().at(i) + ")";
			}
		} else {
			output_column_names[i] = aggregation_column_assigned_aliases[i];
		}
	}

	std::vector< std::unique_ptr<cudf::column> > group_by_output_columns;
	std::vector< std::unique_ptr<cudf::column> > output_columns_aggregations(aggregation_types.size());
	if(group_column_indices.size() == 0) {
		aggregations_without_groupby(aggregation_types, aggregation_inputs, output_columns_aggregations, output_types, output_column_names);
	} else {
		aggregations_with_groupby(group_by_columns,
			aggregation_inputs,
			aggregation_types,
			group_by_output_columns,
			output_columns_aggregations);
	}

	// output table is grouped columns and then aggregated columns
	group_by_output_columns.insert(group_by_output_columns.end(),
		std::make_move_iterator(output_columns_aggregations.begin()),
		std::make_move_iterator(output_columns_aggregations.end()));

	std::unique_ptr<CudfTable> ret_tb = std::make_unique<CudfTable>(std::move(group_by_output_columns));
	std::unique_ptr<ral::frame::BlazingTable> ret = std::make_unique<ral::frame::BlazingTable>(std::move(ret_tb), output_column_names);

	return ret;	
}


void aggregationsMerger(std::vector<ral::distribution::NodeColumns> & aggregations,
	const std::vector<int> & groupColIndices,
	const std::vector<gdf_agg_op> & aggregationTypes,
	blazing_frame & output) {
	// Concat
	size_t totalConcatsOperations = groupColIndices.size() + aggregationTypes.size();
	std::vector<std::vector<gdf_column_cpp>> tablesToConcat(aggregations.size());
	for(size_t i = 0; i < aggregations.size(); i++) {
		tablesToConcat[i] = aggregations[i].getColumns();
	}
	std::vector<gdf_column_cpp> concatAggregations = ral::utilities::concatTables(tablesToConcat);

	// Do aggregations
	std::vector<gdf_column_cpp> groupByColumns(groupColIndices.size());
	for(size_t i = 0; i < groupColIndices.size(); i++) {
		groupByColumns[i] = concatAggregations[groupColIndices[i]];
	}

	// when we are merging COUNT aggregations, we want to SUM them, not use COUNT
	std::vector<gdf_agg_op> modAggregationTypes(aggregationTypes.size());
	for(size_t i = 0; i < aggregationTypes.size(); i++) {
		modAggregationTypes[i] = aggregationTypes[i] == GDF_COUNT ? GDF_SUM : aggregationTypes[i];
	}

	std::vector<gdf_column_cpp> aggregation_inputs(modAggregationTypes.size());
	std::vector<cudf::type_id> aggregation_dtypes(modAggregationTypes.size());
	std::vector<std::string> aggregation_names(modAggregationTypes.size());
	for(size_t i = 0; i < modAggregationTypes.size(); i++) {
		aggregation_inputs[i] = concatAggregations[groupColIndices.size() + i];
		aggregation_dtypes[i] = aggregation_inputs[i].get_gdf_column()->type().id();
		aggregation_names[i] = aggregation_inputs[i].name();
	}

	std::vector<gdf_column_cpp> group_by_output_columns;
	std::vector<gdf_column_cpp> output_columns_aggregations(modAggregationTypes.size());
	
	// TODO percy william alex port distribution
//	if(groupColIndices.size() == 0) {
//		aggregations_without_groupby(modAggregationTypes,
//			aggregation_inputs,
//			output_columns_aggregations,
//			aggregation_dtypes,
//			aggregation_names);
//	} else {
//		aggregations_with_groupby(groupByColumns,
//			aggregation_inputs,
//			modAggregationTypes,
//			group_by_output_columns,
//			output_columns_aggregations,
//			aggregation_names);
//	}

	std::vector<gdf_column_cpp> outputTable(group_by_output_columns);
	outputTable.insert(outputTable.end(),
		std::make_move_iterator(output_columns_aggregations.begin()),
		std::make_move_iterator(output_columns_aggregations.end()));

	output.clear();
	output.add_table(outputTable);
}


std::unique_ptr<ral::frame::BlazingTable> single_node_aggregations(const ral::frame::BlazingTableView & table,
	std::vector<int> & group_column_indices,
	std::vector<cudf::experimental::aggregation::Kind> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	std::unique_ptr<ral::frame::BlazingTable> aggregatedTable = compute_aggregations(table,
		group_column_indices,
		aggregation_types,
		aggregation_input_expressions,
		aggregation_column_assigned_aliases);

	return aggregatedTable;
}

void distributed_aggregations_with_groupby(Context & queryContext,
	blazing_frame & input,
	std::vector<int> & group_column_indices,
	std::vector<gdf_agg_op> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	using ral::communication::experimental::CommunicationData;
	static CodeTimer timer;
	timer.reset();

	if(std::find(aggregation_types.begin(), aggregation_types.end(), GDF_AVG) != aggregation_types.end()) {
		throw std::runtime_error{
			"In distributed_aggregations_with_groupby function: AVG is currently not supported in distributed mode"};
	}

	std::vector<gdf_column_cpp> group_columns(group_column_indices.size());
	for(size_t i = 0; i < group_column_indices.size(); i++) {
		group_columns[i] = input.get_column(group_column_indices[i]);
	}

	size_t rowSize = input.get_num_rows_in_table(0);

	std::vector<gdf_column_cpp> selfSamples = ral::distribution::sampling::generateSample(group_columns, 0.1);
	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_aggregations_with_groupby part 0 generateSample"));
	timer.reset();

	auto aggregationTask = std::async(
		std::launch::async,
		[](Context & queryContext,
			blazing_frame & input,
			std::vector<int> & group_column_indices,
			std::vector<gdf_agg_op> & aggregation_types,
			std::vector<std::string> & aggregation_input_expressions,
			std::vector<std::string> & aggregation_column_assigned_aliases) {
			static CodeTimer timer2;
			
			// TODO percy william alex port distribution
//			std::vector<gdf_column_cpp> result = compute_aggregations(input,
//				group_column_indices,
//				aggregation_types,
//				aggregation_input_expressions,
//				aggregation_column_assigned_aliases);
//			Library::Logging::Logger().logInfo(
//				timer.logDuration(queryContext, "distributed_aggregations_with_groupby async compute_aggregations"));
//			timer.reset();
//			return result;
		},
		std::ref(queryContext),
		std::ref(input),
		std::ref(group_column_indices),
		std::ref(aggregation_types),
		std::ref(aggregation_input_expressions),
		std::ref(aggregation_column_assigned_aliases));

	std::vector<gdf_column_cpp> partitionPlan;
	if(queryContext.isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//std::vector<ral::distribution::NodeSamples> samples = ral::distribution::collectSamples(queryContext);
		//samples.emplace_back(rowSize, CommunicationData::getInstance().getSelfNode(), selfSamples);
		//partitionPlan = ral::distribution::generatePartitionPlansGroupBy(queryContext, samples);

		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//ral::distribution::experimental::distributePartitionPlan(queryContext, partitionPlan);

		Library::Logging::Logger().logInfo(timer.logDuration(queryContext,
			"distributed_aggregations_with_groupby part 1 collectSamples generatePartitionPlansGroupBy "
			"distributePartitionPlan"));
		timer.reset();
	} else {
		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//ral::distribution::sendSamplesToMaster(queryContext, selfSamples, rowSize);

		queryContext.incrementQuerySubstep();
		
		// TODO percy william felipe port distribution cudf0.12
		//partitionPlan = ral::distribution::experimental::getPartitionPlan(queryContext);
		
		Library::Logging::Logger().logInfo(timer.logDuration(
			queryContext, "distributed_aggregations_with_groupby part 1 sendSamplesToMaster getPartitionPlan"));
		timer.reset();
	}

	// TODO percy william alex port distribution
//	// Wait for aggregationThread
//	std::vector<gdf_column_cpp> aggregatedTable = aggregationTask.get();

//	if(partitionPlan[0].get_gdf_column()->size() == 0) {
//		return;
//	}

//	std::vector<int> groupColumnIndices(group_column_indices.size());
//	std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);

//	std::vector<ral::distribution::NodeColumns> partitions =
//		ral::distribution::partitionData(queryContext, aggregatedTable, groupColumnIndices, partitionPlan, false);
//	Library::Logging::Logger().logInfo(
//		timer.logDuration(queryContext, "distributed_aggregations_with_groupby part 2 partitionData"));
//	timer.reset();

//	queryContext.incrementQuerySubstep();
//	ral::distribution::distributePartitions(queryContext, partitions);
//	std::vector<ral::distribution::NodeColumns> partitionsToMerge = ral::distribution::collectPartitions(queryContext);

//	auto it = std::find_if(partitions.begin(), partitions.end(), [&](ral::distribution::NodeColumns & el) {
//		return el.getNode() == CommunicationData::getInstance().getSelfNode();
//	});
//	// Could "it" iterator be partitions.end()?
//	partitionsToMerge.push_back(*it);

//	Library::Logging::Logger().logInfo(timer.logDuration(
//		queryContext, "distributed_aggregations_with_groupby part 3 distributePartitions collectPartitions"));
//	timer.reset();

//	aggregationsMerger(partitionsToMerge, groupColumnIndices, aggregation_types, input);

//	Library::Logging::Logger().logInfo(
//		timer.logDuration(queryContext, "distributed_aggregations_with_groupby part 4 aggregationsMerger"));
//	timer.reset();
}

void distributed_aggregations_without_groupby(Context & queryContext,
	blazing_frame & input,
	std::vector<int> & group_column_indices,
	std::vector<gdf_agg_op> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	using ral::communication::experimental::CommunicationData;
	static CodeTimer timer;
	timer.reset();

	// TODO percy william alex port distribution
//	std::vector<gdf_column_cpp> aggregatedTable = compute_aggregations(input,
//		group_column_indices,
//		aggregation_types,
//		aggregation_input_expressions,
//		aggregation_column_assigned_aliases);

//	Library::Logging::Logger().logInfo(
//		timer.logDuration(queryContext, "distributed_aggregations_without_groupby part 1 compute_aggregations"));
//	timer.reset();

//	if(queryContext.isMasterNode(CommunicationData::getInstance().getSelfNode())) {
//		queryContext.incrementQuerySubstep();
//		std::vector<ral::distribution::NodeColumns> partitionsToMerge =
//			ral::distribution::collectPartitions(queryContext);
//		partitionsToMerge.emplace_back(CommunicationData::getInstance().getSelfNode(), aggregatedTable);

//		std::vector<int> groupColumnIndices(group_column_indices.size());
//		std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);
//		aggregationsMerger(partitionsToMerge, groupColumnIndices, aggregation_types, input);
//		Library::Logging::Logger().logInfo(timer.logDuration(
//			queryContext, "distributed_aggregations_without_groupby part 2 collectPartitions aggregationsMerger"));
//		timer.reset();
//	} else {
//		std::vector<gdf_column_cpp> empty_output_table(aggregatedTable.size());
//		for(size_t i = 0; i < empty_output_table.size(); i++) {
//			empty_output_table[i].create_empty(aggregatedTable[i].get_gdf_column()->type().id(), aggregatedTable[i].name());
//		}

//		input.clear();
//		input.add_table(empty_output_table);

//		std::vector<ral::distribution::NodeColumns> selfPartition;
//		selfPartition.emplace_back(queryContext.getMasterNode(), aggregatedTable);

//		queryContext.incrementQuerySubstep();
//		ral::distribution::distributePartitions(queryContext, selfPartition);

//		Library::Logging::Logger().logInfo(
//			timer.logDuration(queryContext, "distributed_aggregations_without_groupby part 2 distributePartitions"));
//		timer.reset();
//	}
}

std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table,
																std::string query_part, Context * queryContext) {

	/*
	 * 			String sql = "select sum(e), sum(z), x, y from hr.emps group by x , y";
	 * 			generates the following calcite relational algebra
	 * 			LogicalProject(EXPR$0=[$2], EXPR$1=[$3], x=[$0], y=[$1])
	 * 	  	  		LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)], EXPR$1=[SUM($3)])
	 *   				LogicalProject(x=[$0], y=[$1], e=[$3], z=[$2])
	 *     					EnumerableTableScan(table=[[hr, emps]])
	 *
	 * 			As you can see the project following aggregate expects the columns to be grouped by to appear BEFORE the
	 * expressions
	 */
	// Get groups
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd - 1);

	std::vector<int> group_column_indices = get_group_columns(combined_expression);

	// Get aggregations
	std::vector<cudf::experimental::aggregation::Kind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);
	for(std::string expr : expressions) {
		std::string expression = std::regex_replace(expr, std::regex("^ +| +$|( ) +"), "$1");
		if(expression.find("group=") == std::string::npos) {
			cudf::experimental::aggregation::Kind operation = get_aggregation_operation_for_groupby(expression);
			aggregation_types.push_back(operation);
			aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));

			// if the aggregation has an alias, lets capture it here, otherwise we'll figure out what to call the
			// aggregation based on its input
			if(expression.find("EXPR$") == 0)
				aggregation_column_assigned_aliases.push_back("");
			else
				aggregation_column_assigned_aliases.push_back(expression.substr(0, expression.find("=[")));
		}
	}

	std::unique_ptr<ral::frame::BlazingTable> ret;
	
	if(aggregation_types.size() == 0) {
		if(!queryContext || queryContext->getTotalNodes() <= 1) {
			ret = groupby_without_aggregations(table, group_column_indices);
		} else {
			// TODO percy william alex port distribution
			//distributed_groupby_without_aggregations(*queryContext, table, group_column_indices);
		}
	} else {
		if(!queryContext || queryContext->getTotalNodes() <= 1) {
			ret = single_node_aggregations(table,
				group_column_indices,
				aggregation_types,
				aggregation_input_expressions,
				aggregation_column_assigned_aliases);
		} else {
			// TODO percy william alex port distribution
//			if(group_column_indices.size() == 0) {
//				distributed_aggregations_without_groupby(*queryContext,
//					input,
//					group_column_indices,
//					aggregation_types,
//					aggregation_input_expressions,
//					aggregation_column_assigned_aliases);
//			} else {
//				distributed_aggregations_with_groupby(*queryContext,
//					table,
//					group_column_indices,
//					aggregation_types,
//					aggregation_input_expressions,
//					aggregation_column_assigned_aliases);
//			}
		}
	}
	
	return ret;
}

//TODO rommel percy
std::unique_ptr<ral::frame::BlazingTable> groupby_without_aggregations(
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices) {

	std::unique_ptr<cudf::experimental::table> output = cudf::experimental::drop_duplicates(table.view(),
		group_column_indices,
		cudf::experimental::duplicate_keep_option::KEEP_FIRST);

	return std::make_unique<ral::frame::BlazingTable>( std::move(output), table.names() );
}

}  // namespace operators
}  // namespace ral


namespace ral {
namespace operators {
namespace experimental {

typedef blazingdb::manager::experimental::Context Context;
typedef ral::communication::experimental::CommunicationData CommunicationData;
using namespace ral::distribution::experimental;

std::unique_ptr<ral::frame::BlazingTable> process_aggregate(const ral::frame::BlazingTableView & table,
															std::string query_part, Context * context) {

		// Get groups
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
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
		
		std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamples(
																	groupbyColumns, 0.1);

		// WSM TODO cudf0.12 waiting on logger
		// Library::Logging::Logger().logInfo(timer.logDuration(context, "distributed groupby_without_aggregations part 1 generateSample"));

		std::unique_ptr<ral::frame::BlazingTable> grouped_table;
		std::thread groupbyThread{[](Context * context,
								const ral::frame::BlazingTableView & table,
								const std::vector<int> & group_column_indices,
								std::unique_ptr<ral::frame::BlazingTable> & grouped_table) {
								static CodeTimer timer2;
								grouped_table = compute_groupby_without_aggregations(table, group_column_indices);
								// WSM TODO cudf0.12 waiting on logger
								//    Library::Logging::Logger().logInfo(
								// 	   timer2.logDuration(context, "distributed groupby_without_aggregations part 2 async compute_groupby_without_aggregations"));
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

			// WSM TODO cudf0.12 waiting on logger
			// Library::Logging::Logger().logInfo(timer.logDuration(
			// 	context, "distributed groupby_without_aggregations part 2 collectSamples generatePartitionPlans distributePartitionPlan"));
		} else {
			context->incrementQuerySubstep();
			sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

			context->incrementQuerySubstep();
			partitionPlan = getPartitionPlan(context);

			// WSM TODO cudf0.12 waiting on logger
			// Library::Logging::Logger().logInfo(
			// 	timer.logDuration(context, "distributed groupby_without_aggregations part 2 sendSamplesToMaster getPartitionPlan"));
			
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

	// WSM TODO cudf0.12
	// Library::Logging::Logger().logInfo(
	// 	timer.logDuration(queryContext, "aggregations_without_groupby part 1 compute_aggregations"));
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

		// WSM TODO cudf0.12
		// Library::Logging::Logger().logInfo(timer.logDuration(
		// 	queryContext, "aggregations_without_groupby part 2 collectPartitions and merged"));
		timer.reset();
		return merged_results;
	} else {
		
		std::vector<NodeColumnView> selfPartition;
		selfPartition.emplace_back(context->getMasterNode(), results->toBlazingTableView());

		context->incrementQuerySubstep();
		ral::distribution::experimental::distributePartitions(context, selfPartition);

		// WSM TODO cudf0.12
		// Library::Logging::Logger().logInfo(
		// 	timer.logDuration(queryContext, "aggregations_without_groupby part 2 distributePartitions"));
		timer.reset();
		return std::unique_ptr<ral::frame::BlazingTable>();
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
			std::unique_ptr<cudf::column> aggregation_input_scope_holder;
			CudfColumnView aggregation_input; 
			if(contains_evaluation(aggregation_input_expressions[i])) {
				// WSM TODO cudf0.12 need evaluate_expression
				// put output into aggregation_input and add to aggregation_input_scope_holder
			} else {
				aggregation_input = table.view().column(get_index(aggregation_input_expressions[i]));
			}
			if( aggregation_types[i] == "COUNT") { 
				std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
				auto numeric_s = static_cast< cudf::experimental::scalar_type_t<int64_t>* >(scalar.get());
				numeric_s->set_value((int64_t)(aggregation_input.size() - aggregation_input.null_count()));
				reductions.emplace_back(std::move(scalar));
			} else {
				cudf::type_id output_type = get_aggregation_output_type(aggregation_input.type().id(), aggregation_types[i]);
				reductions.emplace_back(cudf::experimental::reduce(aggregation_input,
    					get_aggregation_operation_for_reduce(aggregation_types[i]), cudf::data_type(output_type)));
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
		output_columns.emplace_back(cudf::make_numeric_column(reductions[i]->type(), 1));
		cudf::experimental::fill(output_columns.back()->mutable_view(), 0, 1, *(reductions[i]));
	}
	return std::make_unique<ral::frame::BlazingTable>(std::move(std::make_unique<CudfTable>(std::move(output_columns))), agg_output_column_names);			
}


std::unique_ptr<ral::frame::BlazingTable> aggregations_with_groupby(Context * context,
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_expressions,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices) {

	std::vector<cudf::experimental::aggregation::Kind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation_for_groupby(expression));
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
		
		std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamples(
																	groupbyColumns, 0.1);

		// WSM TODO cudf0.12 waiting on logger
		// Library::Logging::Logger().logInfo(timer.logDuration(context, "distributed aggregations_with_groupby part 1 generateSample"));

		std::unique_ptr<ral::frame::BlazingTable> grouped_table;
		std::thread groupbyThread{[](Context * context,
								const ral::frame::BlazingTableView & table,
								const std::vector<cudf::experimental::aggregation::Kind> & aggregation_types, 
								const std::vector<std::string> & aggregation_input_expressions,
								const std::vector<std::string> & aggregation_column_assigned_aliases, 
								const std::vector<int> & group_column_indices,
								std::unique_ptr<ral::frame::BlazingTable> & grouped_table) {
								static CodeTimer timer2;
								grouped_table = compute_aggregations_with_groupby(table, aggregation_types, aggregation_input_expressions, 
													aggregation_column_assigned_aliases, group_column_indices);
								// WSM TODO cudf0.12 waiting on logger
								//    Library::Logging::Logger().logInfo(
								// 	   timer2.logDuration(context, "distributed aggregations_with_groupby part 2 async compute_aggregations_with_groupby"));
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

			// WSM TODO cudf0.12 waiting on logger
			// Library::Logging::Logger().logInfo(timer.logDuration(
			// 	context, "distributed aggregations_with_groupby part 3 collectSamples generatePartitionPlans distributePartitionPlan"));
		} else {
			context->incrementQuerySubstep();
			sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

			context->incrementQuerySubstep();
			partitionPlan = getPartitionPlan(context);

			// WSM TODO cudf0.12 waiting on logger
			// Library::Logging::Logger().logInfo(
			// 	timer.logDuration(context, "distributed aggregations_with_groupby part 3 sendSamplesToMaster getPartitionPlan"));
			
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

		// WSM TODO cudf0.12 waiting on logger
			// Library::Logging::Logger().logInfo(
			// 	timer.logDuration(context, "distributed aggregations_with_groupby part 4 collected partitions"));

		std::unique_ptr<BlazingTable> concatenated_aggregations = ral::utilities::experimental::concatTables(partitions_to_merge);
	
		std::vector<cudf::experimental::aggregation::Kind> mod_aggregation_types = aggregation_types;
		std::vector<std::string> mod_aggregation_input_expressions(aggregation_input_expressions.size());
		std::vector<std::string> mod_aggregation_column_assigned_aliases(mod_aggregation_types.size());  
		std::vector<int> mod_group_column_indices(group_column_indices.size());  
		std::iota(mod_group_column_indices.begin(), mod_group_column_indices.end(), 0);		 
		for (int i = 0; i < mod_aggregation_types.size(); i++){
			if (mod_aggregation_types[i] == cudf::experimental::aggregation::Kind::COUNT){
				mod_aggregation_types[i] = cudf::experimental::aggregation::Kind::SUM; // if we have a COUNT, we want to SUM the output of the counts from other nodes
			}
			mod_aggregation_input_expressions[i] = std::to_string(i + mod_group_column_indices.size()); // we just want to aggregate the input columns, so we are setting the indices here
			mod_aggregation_column_assigned_aliases[i] = concatenated_aggregations->names()[i + mod_group_column_indices.size()];
		}
		
		std::unique_ptr<ral::frame::BlazingTable> merged_results = compute_aggregations_with_groupby(concatenated_aggregations->toBlazingTableView(), 
				mod_aggregation_types, mod_aggregation_input_expressions, mod_aggregation_column_assigned_aliases, mod_group_column_indices);

		// WSM TODO cudf0.12 waiting on logger
		// Library::Logging::Logger().logInfo(
		// timer.logDuration(queryContext, "distributed_aggregations_with_groupby part 5 concat and merge"));
		timer.reset();
		return merged_results;
	}
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<cudf::experimental::aggregation::Kind> & aggregation_types, 
		const std::vector<std::string> & aggregation_input_expressions, const std::vector<std::string> & aggregation_column_assigned_aliases, 
		const std::vector<int> & group_column_indices) {

	// lets get the unique expressions. This is how many aggregation requests we will need
	std::vector<std::string> unique_expressions = aggregation_input_expressions;
	sort( unique_expressions.begin(), unique_expressions.end() );
	unique_expressions.erase( unique( unique_expressions.begin(), unique_expressions.end() ), unique_expressions.end() );

	// We will iterate over the unique expressions and create an aggregation request for each one. 
	// We do it this way, because you could have something like min(colA), max(colA), sum(colA). 
	// These three aggregations would all be in one request because they have the same input
	std::vector< std::unique_ptr<cudf::column> > aggregation_inputs_scope_holder;
	std::vector<cudf::experimental::groupby::aggregation_request> requests;
	std::vector<int> agg_out_indices;
	std::vector<std::string> agg_output_column_names;
	for (size_t u = 0; u < unique_expressions.size(); u++){
		std::string expression = aggregation_input_expressions[u];

		CudfColumnView aggregation_input; // this is the input from which we will crete the aggregation request
		std::vector<std::unique_ptr<cudf::experimental::aggregation>> agg_ops_for_request;
		for (size_t i = 0; i < aggregation_input_expressions.size(); i++){
			if (expression == aggregation_input_expressions[i]){

				// need to calculate or determine the aggregation input only once
				if (aggregation_input.size() == 0){
					// this means we have a COUNT(*). So lets create a simple column with no nulls
					if(expression == "" && aggregation_types[i] == cudf::experimental::aggregation::Kind::COUNT ) {
						std::unique_ptr<cudf::column> temp = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT8), table.view().num_rows());
						std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT8));
						auto numeric_s = static_cast< cudf::experimental::scalar_type_t<int8_t>* >(scalar.get());
						numeric_s->set_value(0);
						cudf::experimental::fill(temp->mutable_view(), 0, temp->size(), *scalar);
						aggregation_inputs_scope_holder.emplace_back(std::move(temp));
						aggregation_input  = aggregation_inputs_scope_holder.back()->view();
					} else {
						if(contains_evaluation(expression)) {
							// WSM TODO cudf0.12 need evaluate_expression
							// put output into aggregation_input and add to aggregation_inputs_scope_holder
						} else {
							aggregation_input = table.view().column(get_index(expression));
						}
					}
				}
				agg_ops_for_request.push_back(std::make_unique<cudf::experimental::aggregation>(aggregation_types[i]));
				agg_out_indices.push_back(i);  // this is to know what is the desired order of aggregations output
				
				// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
				if(aggregation_column_assigned_aliases[i] == "") {
					if(expression == "" && aggregation_types[i] == cudf::experimental::aggregation::Kind::COUNT) {  // COUNT(*) case
						agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(*)");
					} else {
						agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table.names().at(i) + ")");
					}
				} else {
					agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
				}
			}
		}		
		requests.push_back(cudf::experimental::groupby::aggregation_request {.values = aggregation_input, .aggregations = std::move(agg_ops_for_request)});
	}

	CudfTableView keys = table.view().select(group_column_indices);
	cudf::experimental::groupby::groupby group_by_obj(keys);
	std::pair<std::unique_ptr<cudf::experimental::table>, std::vector<cudf::experimental::groupby::aggregation_result>> result = group_by_obj.aggregate( requests );

	// output table is grouped columns and then aggregated columns
	std::vector< std::unique_ptr<cudf::column> > output_columns = result.first->release();
	
	// lets collect all the aggregated results from the results structure and then add them to output_columns
	std::vector< std::unique_ptr<cudf::column> > agg_cols_out;
	for (int i = 0; i < result.second.size(); i++){
		for (int j = 0; j < result.second[i].results.size(); j++){
			agg_cols_out.emplace_back(std::move(result.second[i].results[j]));
		}
	}
	for (int i = 0; i < agg_out_indices.size(); i++){
		output_columns.emplace_back(std::move(agg_cols_out[agg_out_indices[i]]));
	}
	std::unique_ptr<CudfTable> output_table = std::make_unique<CudfTable>(std::move(output_columns));

	// lets put together the output names
	std::vector<std::string> output_names;
	for (int i = 0; i < group_column_indices.size(); i++){
		output_names.push_back(table.names()[group_column_indices[i]]);
	}
	for (int i = 0; i < agg_out_indices.size(); i++){
		output_names.emplace_back(std::move(agg_output_column_names[agg_out_indices[i]]));
	}

	return std::make_unique<BlazingTable>(std::move(output_table), output_names);
}

}  // namespace experimental
}  // namespace operators
}  // namespace ral