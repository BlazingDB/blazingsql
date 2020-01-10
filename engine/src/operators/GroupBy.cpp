#include "GroupBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "ColumnManipulation.cuh"
#include "GDFColumn.cuh"
#include "LogicalFilter.h"
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

#include <cudf/groupby.hpp>
#include <cudf/stream_compaction.hpp>

namespace ral {
namespace operators {

namespace {
using blazingdb::manager::Context;
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

//ToDo Rommel
std::vector<gdf_column_cpp> groupby_without_aggregations(
	std::vector<gdf_column_cpp> & input, const std::vector<int> & group_column_indices) {
	cudf::size_type num_group_columns = group_column_indices.size();

	gdf_context ctxt;
	ctxt.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  //  Nulls are are treated as largest
	ctxt.flag_groupby_include_nulls = 1;  // Nulls are treated as values in group by keys where NULL == NULL (SQL style)

	cudf::table group_by_data_in_table = ral::utilities::create_table(input);
	cudf::table group_by_columns_out_table;

	// We want the index_col_ptr be on the heap because index_col will call delete when it goes out of scope
	// TODO percy cudf0.12 port to cudf::column
	cudf::column * index_col_ptr = new cudf::column();
	
	// TODO percy cudf0.12 port to cudf::column
//	std::tie(group_by_columns_out_table, *index_col_ptr) = gdf_group_by_without_aggregations(
//		group_by_data_in_table, num_group_columns, group_column_indices.data(), &ctxt);
	
	gdf_column_cpp index_col;
	
	// TODO percy cudf0.12 port to cudf::column
	//index_col.create_gdf_column(index_col_ptr);

	ral::init_string_category_if_null(group_by_columns_out_table);

	std::vector<gdf_column_cpp> output_columns_group(group_by_columns_out_table.num_columns());
	for(int i = 0; i < output_columns_group.size(); i++) {
		auto * grouped_col = group_by_columns_out_table.get_column(i);
		grouped_col->col_name =
			nullptr;  // need to do this because gdf_group_by_without_aggregations is not setting the name properly
		
		// TODO percy cudf0.12 port to cudf::column
		//output_columns_group[i].create_gdf_column(grouped_col);
	}

	std::vector<gdf_column_cpp> grouped_output(num_group_columns);
	for(int i = 0; i < num_group_columns; i++) {
		// TODO percy cudf0.12 port to cudf::column
//		if(input[i].valid()) {
//			grouped_output[i].create_gdf_column(input[i].dtype(),
//				index_col_ptr->size,
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input[i].dtype()),
//				input[i].name());
//		}
//		else {
//			grouped_output[i].create_gdf_column(input[i].dtype(),
//				index_col_ptr->size,
//				nullptr,
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input[i].dtype()),
//				input[i].name());
//		}
		materialize_column(output_columns_group[i].get_gdf_column(), grouped_output[i].get_gdf_column(), index_col_ptr);
	}
	return grouped_output;
}

void single_node_groupby_without_aggregations(blazing_frame & input, std::vector<int> & group_column_indices) {
	std::vector<gdf_column_cpp> data_cols_in(input.get_width());
	for(int i = 0; i < input.get_width(); i++) {
		data_cols_in[i] = input.get_column(i);
	}
	std::vector<gdf_column_cpp> grouped_table = groupby_without_aggregations(data_cols_in, group_column_indices);

	input.clear();
	input.add_table(grouped_table);
}

void distributed_groupby_without_aggregations(
	Context & queryContext, blazing_frame & input, std::vector<int> & group_column_indices) {
	using ral::communication::CommunicationData;
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
			std::vector<gdf_column_cpp> result = groupby_without_aggregations(input, group_column_indices);
			Library::Logging::Logger().logInfo(timer.logDuration(
				queryContext, "distributed_groupby_without_aggregations part 1 async groupby_without_aggregations"));
			timer.reset();
			return result;
		},
		std::ref(queryContext),
		std::ref(data_cols_in),
		std::ref(group_column_indices));

	std::vector<gdf_column_cpp> partitionPlan;
	if(queryContext.isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		queryContext.incrementQuerySubstep();
		std::vector<ral::distribution::NodeSamples> samples = ral::distribution::collectSamples(queryContext);
		samples.emplace_back(rowSize, CommunicationData::getInstance().getSelfNode(), selfSamples);

		partitionPlan = ral::distribution::generatePartitionPlansGroupBy(queryContext, samples);

		queryContext.incrementQuerySubstep();
		ral::distribution::distributePartitionPlan(queryContext, partitionPlan);

		Library::Logging::Logger().logInfo(timer.logDuration(queryContext,
			"distributed_groupby_without_aggregations part 1 collectSamples generatePartitionPlansGroupBy "
			"distributePartitionPlan"));
		timer.reset();
	} else {
		queryContext.incrementQuerySubstep();
		ral::distribution::sendSamplesToMaster(queryContext, selfSamples, rowSize);

		queryContext.incrementQuerySubstep();
		partitionPlan = ral::distribution::getPartitionPlan(queryContext);

		Library::Logging::Logger().logInfo(timer.logDuration(
			queryContext, "distributed_groupby_without_aggregations part 1 sendSamplesToMaster getPartitionPlan"));
		timer.reset();
	}
	// Wait for groupByThread
	std::vector<gdf_column_cpp> groupedTable = groupByTask.get();
	queryContext.incrementQuerySubstep();

	if(partitionPlan[0].get_gdf_column()->size() == 0) {
		return;
	}

	std::vector<ral::distribution::NodeColumns> partitions =
		ral::distribution::partitionData(queryContext, groupedTable, group_column_indices, partitionPlan, false);
	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_groupby_without_aggregations part 2 partitionData"));
	timer.reset();

	queryContext.incrementQuerySubstep();
	ral::distribution::distributePartitions(queryContext, partitions);
	std::vector<ral::distribution::NodeColumns> partitionsToMerge = ral::distribution::collectPartitions(queryContext);

	auto it = std::find_if(partitions.begin(), partitions.end(), [&](ral::distribution::NodeColumns & el) {
		return el.getNode() == CommunicationData::getInstance().getSelfNode();
	});
	// Could "it" iterator be partitions.end()?
	partitionsToMerge.push_back(*it);

	Library::Logging::Logger().logInfo(timer.logDuration(
		queryContext, "distributed_groupby_without_aggregations part 3 distributePartitions collectPartitions"));
	timer.reset();

	ral::distribution::groupByWithoutAggregationsMerger(partitionsToMerge, group_column_indices, input);

	Library::Logging::Logger().logInfo(timer.logDuration(
		queryContext, "distributed_groupby_without_aggregations part 4 groupByWithoutAggregationsMerger"));
	timer.reset();
}

void aggregations_with_groupby(std::vector<gdf_column_cpp> & group_by_columns,
	std::vector<gdf_column_cpp> & aggregation_inputs,
	const std::vector<gdf_agg_op> & agg_ops,
	std::vector<gdf_column_cpp> & group_by_output_columns,
	std::vector<gdf_column_cpp> & aggrgation_output_columns,
	const std::vector<std::string> & output_column_names) {
	cudf::table keys = ral::utilities::create_table(group_by_columns);
	cudf::table values = ral::utilities::create_table(aggregation_inputs);

	std::vector<cudf::groupby::operators> ops(agg_ops.size());
	std::transform(agg_ops.begin(), agg_ops.end(), ops.begin(), [&](const gdf_agg_op & op) {
		return gdf_agg_op_to_groupby_operators(op);
	});

	cudf::groupby::hash::Options options(false);  // options define null behaviour to be SQL style

	cudf::table group_by_output_table;
	cudf::table aggrgation_output_table;
	std::tie(group_by_output_table, aggrgation_output_table) = cudf::groupby::hash::groupby(keys,
                                            values, ops, options);
	
	init_string_category_if_null(group_by_output_table);
    init_string_category_if_null(aggrgation_output_table);

	group_by_output_columns.resize(group_by_output_table.num_columns());
	for(size_t i = 0; i < group_by_output_columns.size(); i++) {
		// TODO percy cudf0.12 port to cudf::column
//		group_by_output_columns[i].create_gdf_column(group_by_output_table.get_column(i));
//		group_by_output_columns[i].set_name(group_by_columns[i].name());
	}

	aggrgation_output_columns.resize(aggrgation_output_table.num_columns());
	for(size_t i = 0; i < aggrgation_output_columns.size(); i++) {
		// TODO percy cudf0.12 port to cudf::column
//		aggrgation_output_columns[i].create_gdf_column(aggrgation_output_table.get_column(i));
//		aggrgation_output_columns[i].set_name(output_column_names[i]);
	}
}

void _new_aggregations_with_groupby(std::vector<CudfColumnView> & group_by_columns,
	std::vector<cudf::column_view> & aggregation_inputs,
	const std::vector<std::unique_ptr<cudf::experimental::aggregation>> & agg_ops,
	std::vector<cudf::column_view> & group_by_output_columns,
	std::vector<cudf::column_view> & aggrgation_output_columns,
	const std::vector<std::string> & output_column_names) {
	
	//cudf::experimental::groupby()
	
//	cudf::table keys = ral::utilities::create_table(group_by_columns);
//	cudf::table values = ral::utilities::create_table(aggregation_inputs);

//	std::vector<cudf::groupby::operators> ops(agg_ops.size());
//	std::transform(agg_ops.begin(), agg_ops.end(), ops.begin(), [&](const gdf_agg_op & op) {
//		return gdf_agg_op_to_groupby_operators(op);
//	});

//	cudf::groupby::hash::Options options(false);  // options define null behaviour to be SQL style

//	cudf::table group_by_output_table;
//	cudf::table aggrgation_output_table;
//	std::tie(group_by_output_table, aggrgation_output_table) = cudf::groupby::hash::groupby(keys,
//                                            values, ops, options);
	
//	init_string_category_if_null(group_by_output_table);
//    init_string_category_if_null(aggrgation_output_table);

//	group_by_output_columns.resize(group_by_output_table.num_columns());
//	for(size_t i = 0; i < group_by_output_columns.size(); i++) {
//		// TODO percy cudf0.12 port to cudf::column
////		group_by_output_columns[i].create_gdf_column(group_by_output_table.get_column(i));
////		group_by_output_columns[i].set_name(group_by_columns[i].name());
//	}

//	aggrgation_output_columns.resize(aggrgation_output_table.num_columns());
//	for(size_t i = 0; i < aggrgation_output_columns.size(); i++) {
//		// TODO percy cudf0.12 port to cudf::column
////		aggrgation_output_columns[i].create_gdf_column(aggrgation_output_table.get_column(i));
////		aggrgation_output_columns[i].set_name(output_column_names[i]);
//	}
	
}

void aggregations_without_groupby(const std::vector<gdf_agg_op> & agg_ops,
	std::vector<gdf_column_cpp> & aggregation_inputs,
	std::vector<gdf_column_cpp> & output_columns,
	const std::vector<cudf::type_id> & output_types,
	const std::vector<std::string> & output_column_names) {
	for(size_t i = 0; i < agg_ops.size(); i++) {
		switch(agg_ops[i]) {
		case GDF_SUM:
		case GDF_MIN:
		case GDF_MAX:
			if(aggregation_inputs[i].get_gdf_column()->size() == 0) {
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
		case GDF_AVG:
			if(aggregation_inputs[i].get_gdf_column()->size() == 0 ||
				(aggregation_inputs[i].get_gdf_column()->size() == aggregation_inputs[i].get_gdf_column()->null_count())) {
				// Set output_column data to invalid
				
				// TODO percy cudf0.12 implement proper scalar support
				//std::unique_ptr<cudf::scalar> null_value;
				//null_value.is_valid = false;
				//null_value.dtype = to_gdf_type(output_types[i]);
				//output_columns[i].create_gdf_column(null_value, output_column_names[i]);
				
				break;
			} else {
				cudf::type_id sum_output_type = get_aggregation_output_type(aggregation_inputs[i].get_gdf_column()->type().id(), GDF_SUM, false);
				
				// TODO percy cudf0.12 implement proper scalar support
				//std::unique_ptr<cudf::scalar> avg_sum_scalar = cudf::reduce(
				//	aggregation_inputs[i].get_gdf_column(), cudf::reduction::operators::SUM, to_gdf_type(sum_output_type));
				
				long avg_count =
					aggregation_inputs[i].get_gdf_column()->size() - aggregation_inputs[i].get_gdf_column()->null_count();

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
		case GDF_COUNT: {
			// TODO percy cudf0.12 implement proper scalar support
			//std::unique_ptr<cudf::scalar> reduction_out;
			//reduction_out.dtype = GDF_INT64;
			//reduction_out.is_valid = true;
			//reduction_out.data.si64 =
			//	aggregation_inputs[i].get_gdf_column()->size - aggregation_inputs[i].get_gdf_column()->null_count;
			//output_columns[i].create_gdf_column(reduction_out, output_column_names[i]);
			
			break;
		}
		case GDF_COUNT_DISTINCT: {
			throw std::runtime_error("COUNT DISTINCT currently not supported without a group by");
		}
		}
	}
}

std::vector<gdf_column_cpp> compute_aggregations(blazing_frame & input,
	std::vector<int> & group_column_indices,
	std::vector<gdf_agg_op> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	size_t row_size = input.get_num_rows_in_table(0);

	std::vector<gdf_column_cpp> group_by_columns(group_column_indices.size());
	for(size_t i = 0; i < group_column_indices.size(); i++) {
		group_by_columns[i] = input.get_column(group_column_indices[i]);
	}

	std::vector<gdf_column_cpp> aggregation_inputs(aggregation_types.size());
	std::vector<cudf::type_id> output_types(aggregation_types.size());
	std::vector<std::string> output_column_names(aggregation_types.size());

	for(size_t i = 0; i < aggregation_types.size(); i++) {
		std::string expression = aggregation_input_expressions[i];

		if(expression == "" &&
			aggregation_types[i] ==
				GDF_COUNT) {  // this means we have a COUNT(*). So lets create a simple column with no nulls
			std::vector<int8_t> temp(row_size, 0);
			aggregation_inputs[i].create_gdf_column(
				cudf::type_id::INT8, row_size, temp.data(), ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT8), "");
		} else {
			if(contains_evaluation(expression)) {
				// we dont knwo what the size of this input will be so allcoate max size
				// TODO de donde saco el nombre de la columna aqui???
				cudf::type_id unused;
				cudf::type_id agg_input_type = get_output_type_expression(&input, &unused, expression);

				aggregation_inputs[i].create_gdf_column(agg_input_type,
					row_size,
					nullptr,
					ral::traits::get_dtype_size_in_bytes(agg_input_type),
					"");
				evaluate_expression(input, expression, aggregation_inputs[i]);
			} else {
				aggregation_inputs[i] = input.get_column(get_index(expression));
			}
		}

		output_types[i] = get_aggregation_output_type(
			aggregation_inputs[i].get_gdf_column()->type().id(), aggregation_types[i], group_column_indices.size() != 0);

		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
		if(aggregation_column_assigned_aliases[i] == "") {
			if(expression == "" && aggregation_types[i] == GDF_COUNT) {  // COUNT(*) case
				output_column_names[i] = aggregator_to_string(aggregation_types[i]) + "(*)";
			} else {
				output_column_names[i] =
					aggregator_to_string(aggregation_types[i]) + "(" + aggregation_inputs[i].name() + ")";
			}
		} else {
			output_column_names[i] = aggregation_column_assigned_aliases[i];
		}
	}

	std::vector<gdf_column_cpp> group_by_output_columns;
	std::vector<gdf_column_cpp> output_columns_aggregations(aggregation_types.size());
	if(group_column_indices.size() == 0) {
		aggregations_without_groupby(
			aggregation_types, aggregation_inputs, output_columns_aggregations, output_types, output_column_names);
	} else {
		aggregations_with_groupby(group_by_columns,
			aggregation_inputs,
			aggregation_types,
			group_by_output_columns,
			output_columns_aggregations,
			output_column_names);
	}

	// output table is grouped columns and then aggregated columns
	group_by_output_columns.insert(group_by_output_columns.end(),
		std::make_move_iterator(output_columns_aggregations.begin()),
		std::make_move_iterator(output_columns_aggregations.end()));

	return group_by_output_columns;
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
	if(groupColIndices.size() == 0) {
		aggregations_without_groupby(modAggregationTypes,
			aggregation_inputs,
			output_columns_aggregations,
			aggregation_dtypes,
			aggregation_names);
	} else {
		aggregations_with_groupby(groupByColumns,
			aggregation_inputs,
			modAggregationTypes,
			group_by_output_columns,
			output_columns_aggregations,
			aggregation_names);
	}

	std::vector<gdf_column_cpp> outputTable(group_by_output_columns);
	outputTable.insert(outputTable.end(),
		std::make_move_iterator(output_columns_aggregations.begin()),
		std::make_move_iterator(output_columns_aggregations.end()));

	output.clear();
	output.add_table(outputTable);
}


void single_node_aggregations(blazing_frame & input,
	std::vector<int> & group_column_indices,
	std::vector<gdf_agg_op> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	std::vector<gdf_column_cpp> aggregatedTable = compute_aggregations(input,
		group_column_indices,
		aggregation_types,
		aggregation_input_expressions,
		aggregation_column_assigned_aliases);

	input.clear();
	input.add_table(aggregatedTable);
}

void distributed_aggregations_with_groupby(Context & queryContext,
	blazing_frame & input,
	std::vector<int> & group_column_indices,
	std::vector<gdf_agg_op> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	using ral::communication::CommunicationData;
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
			std::vector<gdf_column_cpp> result = compute_aggregations(input,
				group_column_indices,
				aggregation_types,
				aggregation_input_expressions,
				aggregation_column_assigned_aliases);
			Library::Logging::Logger().logInfo(
				timer.logDuration(queryContext, "distributed_aggregations_with_groupby async compute_aggregations"));
			timer.reset();
			return result;
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
		std::vector<ral::distribution::NodeSamples> samples = ral::distribution::collectSamples(queryContext);
		samples.emplace_back(rowSize, CommunicationData::getInstance().getSelfNode(), selfSamples);

		partitionPlan = ral::distribution::generatePartitionPlansGroupBy(queryContext, samples);

		queryContext.incrementQuerySubstep();
		ral::distribution::distributePartitionPlan(queryContext, partitionPlan);

		Library::Logging::Logger().logInfo(timer.logDuration(queryContext,
			"distributed_aggregations_with_groupby part 1 collectSamples generatePartitionPlansGroupBy "
			"distributePartitionPlan"));
		timer.reset();
	} else {
		queryContext.incrementQuerySubstep();
		ral::distribution::sendSamplesToMaster(queryContext, selfSamples, rowSize);

		queryContext.incrementQuerySubstep();
		partitionPlan = ral::distribution::getPartitionPlan(queryContext);
		Library::Logging::Logger().logInfo(timer.logDuration(
			queryContext, "distributed_aggregations_with_groupby part 1 sendSamplesToMaster getPartitionPlan"));
		timer.reset();
	}

	// Wait for aggregationThread
	std::vector<gdf_column_cpp> aggregatedTable = aggregationTask.get();

	if(partitionPlan[0].get_gdf_column()->size() == 0) {
		return;
	}

	std::vector<int> groupColumnIndices(group_column_indices.size());
	std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);

	std::vector<ral::distribution::NodeColumns> partitions =
		ral::distribution::partitionData(queryContext, aggregatedTable, groupColumnIndices, partitionPlan, false);
	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_aggregations_with_groupby part 2 partitionData"));
	timer.reset();

	queryContext.incrementQuerySubstep();
	ral::distribution::distributePartitions(queryContext, partitions);
	std::vector<ral::distribution::NodeColumns> partitionsToMerge = ral::distribution::collectPartitions(queryContext);

	auto it = std::find_if(partitions.begin(), partitions.end(), [&](ral::distribution::NodeColumns & el) {
		return el.getNode() == CommunicationData::getInstance().getSelfNode();
	});
	// Could "it" iterator be partitions.end()?
	partitionsToMerge.push_back(*it);

	Library::Logging::Logger().logInfo(timer.logDuration(
		queryContext, "distributed_aggregations_with_groupby part 3 distributePartitions collectPartitions"));
	timer.reset();

	aggregationsMerger(partitionsToMerge, groupColumnIndices, aggregation_types, input);

	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_aggregations_with_groupby part 4 aggregationsMerger"));
	timer.reset();
}

void distributed_aggregations_without_groupby(Context & queryContext,
	blazing_frame & input,
	std::vector<int> & group_column_indices,
	std::vector<gdf_agg_op> & aggregation_types,
	std::vector<std::string> & aggregation_input_expressions,
	std::vector<std::string> & aggregation_column_assigned_aliases) {
	using ral::communication::CommunicationData;
	static CodeTimer timer;
	timer.reset();

	std::vector<gdf_column_cpp> aggregatedTable = compute_aggregations(input,
		group_column_indices,
		aggregation_types,
		aggregation_input_expressions,
		aggregation_column_assigned_aliases);

	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_aggregations_without_groupby part 1 compute_aggregations"));
	timer.reset();

	if(queryContext.isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		queryContext.incrementQuerySubstep();
		std::vector<ral::distribution::NodeColumns> partitionsToMerge =
			ral::distribution::collectPartitions(queryContext);
		partitionsToMerge.emplace_back(CommunicationData::getInstance().getSelfNode(), aggregatedTable);

		std::vector<int> groupColumnIndices(group_column_indices.size());
		std::iota(groupColumnIndices.begin(), groupColumnIndices.end(), 0);
		aggregationsMerger(partitionsToMerge, groupColumnIndices, aggregation_types, input);
		Library::Logging::Logger().logInfo(timer.logDuration(
			queryContext, "distributed_aggregations_without_groupby part 2 collectPartitions aggregationsMerger"));
		timer.reset();
	} else {
		std::vector<gdf_column_cpp> empty_output_table(aggregatedTable.size());
		for(size_t i = 0; i < empty_output_table.size(); i++) {
			empty_output_table[i].create_empty(aggregatedTable[i].get_gdf_column()->type().id(), aggregatedTable[i].name());
		}

		input.clear();
		input.add_table(empty_output_table);

		std::vector<ral::distribution::NodeColumns> selfPartition;
		selfPartition.emplace_back(queryContext.getMasterNode(), aggregatedTable);

		queryContext.incrementQuerySubstep();
		ral::distribution::distributePartitions(queryContext, selfPartition);

		Library::Logging::Logger().logInfo(
			timer.logDuration(queryContext, "distributed_aggregations_without_groupby part 2 distributePartitions"));
		timer.reset();
	}
}

void process_aggregate(blazing_frame & input, std::string query_part, Context * queryContext) {
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
	std::vector<gdf_agg_op> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);
	for(std::string expr : expressions) {
		std::string expression = std::regex_replace(expr, std::regex("^ +| +$|( ) +"), "$1");
		if(expression.find("group=") == std::string::npos) {
			gdf_agg_op operation = get_aggregation_operation(expression);
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

	if(aggregation_types.size() == 0) {
		if(!queryContext || queryContext->getTotalNodes() <= 1) {
			single_node_groupby_without_aggregations(input, group_column_indices);
		} else {
			distributed_groupby_without_aggregations(*queryContext, input, group_column_indices);
		}
	} else {
		if(!queryContext || queryContext->getTotalNodes() <= 1) {
			single_node_aggregations(input,
				group_column_indices,
				aggregation_types,
				aggregation_input_expressions,
				aggregation_column_assigned_aliases);
		} else {
			if(group_column_indices.size() == 0) {
				distributed_aggregations_without_groupby(*queryContext,
					input,
					group_column_indices,
					aggregation_types,
					aggregation_input_expressions,
					aggregation_column_assigned_aliases);
			} else {
				distributed_aggregations_with_groupby(*queryContext,
					input,
					group_column_indices,
					aggregation_types,
					aggregation_input_expressions,
					aggregation_column_assigned_aliases);
			}
		}
	}
}

//ToDo Rommel
std::unique_ptr<ral::frame::BlazingTable> _new_groupby_without_aggregations(
	const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices) {


/**
 * @brief Create a new table without duplicate rows
 *
 * Given an `input` table_view, each row is copied to output table if the corresponding
 * row of `keys` columns is unique, where the definition of unique depends on the value of @p keep:
 * - KEEP_FIRST: only the first of a sequence of duplicate rows is copied
 * - KEEP_LAST: only the last of a sequence of duplicate rows is copied
 * - KEEP_NONE: only unique rows are kept
 *
 * @throws cudf::logic_error if The `input` row size mismatches with `keys`.
 *
 * @param[in] input           input table_view to copy only unique rows
 * @param[in] keys            vector of indices representing key columns from `input`
 * @param[in] keep            keep first entry, last entry, or no entries if duplicates found
 * @param[in] nulls_are_equal flag to denote nulls are equal if true,
 * nulls are not equal if false
 * @param[in] mr Optional, The resource to use for all allocations
 * @param[in] stream Optional CUDA stream on which to execute kernels
 *
 * @return unique_ptr<table> Table with unique rows as per specified `keep`.
 */
std::unique_ptr<cudf::experimental::table> output = cudf::experimental::drop_duplicates(table.view(),
                    group_column_indices,
                    cudf::experimental::duplicate_keep_option::KEEP_FIRST);


	// cudf::size_type num_group_columns = group_column_indices.size();

	// gdf_context ctxt;
	// ctxt.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  //  Nulls are are treated as largest
	// ctxt.flag_groupby_include_nulls = 1;  // Nulls are treated as values in group by keys where NULL == NULL (SQL style)

	// cudf::table group_by_data_in_table = ral::utilities::create_table(input);
	// cudf::table group_by_columns_out_table;

	// We want the index_col_ptr be on the heap because index_col will call delete when it goes out of scope
	// TODO percy cudf0.12 port to cudf::column
	// cudf::column * index_col_ptr = new cudf::column();
	
	// TODO percy cudf0.12 port to cudf::column
//	std::tie(group_by_columns_out_table, *index_col_ptr) = gdf_group_by_without_aggregations(
//		group_by_data_in_table, num_group_columns, group_column_indices.data(), &ctxt);
	
	// gdf_column_cpp index_col;
	
	// TODO percy cudf0.12 port to cudf::column
	//index_col.create_gdf_column(index_col_ptr);

	// ral::init_string_category_if_null(group_by_columns_out_table);

	// std::vector<gdf_column_cpp> output_columns_group(group_by_columns_out_table.num_columns());
	// for(int i = 0; i < output_columns_group.size(); i++) {
	// 	auto * grouped_col = group_by_columns_out_table.get_column(i);
	// 	grouped_col->col_name =
	// 		nullptr;  // need to do this because gdf_group_by_without_aggregations is not setting the name properly
		
		// TODO percy cudf0.12 port to cudf::column
		//output_columns_group[i].create_gdf_column(grouped_col);
	// }

	// std::vector<gdf_column_cpp> grouped_output(num_group_columns);
	// for(int i = 0; i < num_group_columns; i++) {
		// TODO percy cudf0.12 port to cudf::column
//		if(input[i].valid()) {
//			grouped_output[i].create_gdf_column(input[i].dtype(),
//				index_col_ptr->size,
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input[i].dtype()),
//				input[i].name());
//		}
//		else {
//			grouped_output[i].create_gdf_column(input[i].dtype(),
//				index_col_ptr->size,
//				nullptr,
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input[i].dtype()),
//				input[i].name());
//		}
		// materialize_column(output_columns_group[i].get_gdf_column(), grouped_output[i].get_gdf_column(), index_col_ptr);
	// }
	// return grouped_output;
}

}  // namespace operators
}  // namespace ral
