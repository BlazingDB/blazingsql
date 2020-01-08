#include "OrderBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "ColumnManipulation.cuh"
#include "GDFColumn.cuh"
#include "cudf.h"
#include "cudf/legacy/copying.hpp"
#include "Traits/RuntimeTraits.h"
#include "communication/CommunicationData.h"
#include "config/GPUManager.cuh"
#include "cuDF/safe_nvcategory_gather.hpp"
#include "distribution/primitives.h"
#include <algorithm>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>
#include <blazingdb/manager/Context.h>
#include <functional>
#include <iostream>
#include <numeric>
#include <thread>

namespace ral {
namespace operators {

const std::string LOGICAL_SORT_TEXT = "LogicalSort";
const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";

bool is_sort(std::string query_part) { return (query_part.find(LOGICAL_SORT_TEXT) != std::string::npos); }

int count_string_occurrence(std::string haystack, std::string needle) {
	int position = haystack.find(needle, 0);
	int count = 0;
	while(position != std::string::npos) {
		count++;
		position = haystack.find(needle, position + needle.size());
	}

	return count;
}

void limit_table(blazing_frame & input, gdf_size_type limitRows) {
	gdf_size_type rowSize = input.get_num_rows_in_table(0);

	if(limitRows < rowSize) {
		for(size_t i = 0; i < input.get_size_column(0); ++i) {
			auto & input_col = input.get_column(i);
			if(input_col.dtype() == GDF_STRING_CATEGORY) {
				ral::truncate_nvcategory(input_col.get_gdf_column(), limitRows);
			} else {
				gdf_column_cpp limitedCpp;
				gdf_column * sourceColumn = input_col.get_gdf_column();
				gdf_size_type width_per_value = ral::traits::get_dtype_size_in_bytes(sourceColumn);
				limitedCpp.create_gdf_column(sourceColumn->dtype,
					sourceColumn->dtype_info,
					limitRows,
					nullptr,
					nullptr,
					width_per_value,
					sourceColumn->col_name);

				gdf_column * limitedColumn = limitedCpp.get_gdf_column();
				cudf::copy_range(limitedColumn, *sourceColumn, 0, limitRows, 0);

				input.set_column(i, limitedCpp);
			}
		}
	}
}

void distributed_limit(Context & queryContext, blazing_frame & input, gdf_size_type limitRows) {
	using ral::communication::CommunicationData;
	using ral::distribution::NodeSamples;

	gdf_size_type rowSize = input.get_num_rows_in_table(0);

	queryContext.incrementQuerySubstep();
	ral::distribution::distributeRowSize(queryContext, rowSize);
	std::vector<gdf_size_type> nodesRowSize = ral::distribution::collectRowSize(queryContext);

	int self_node_idx = queryContext.getNodeIndex(CommunicationData::getInstance().getSelfNode());

	gdf_size_type prevTotalRows = std::accumulate(nodesRowSize.begin(), nodesRowSize.begin() + self_node_idx, 0);

	if(prevTotalRows + rowSize > limitRows) {
		limit_table(input, std::max(limitRows - prevTotalRows, 0));
	}
}

void sort(const Context & queryContext,
	blazing_frame & input,
	std::vector<gdf_column *> & rawCols,
	std::vector<int8_t> & sortOrderTypes,
	std::vector<gdf_column_cpp> & sortedTable) {
	static CodeTimer timer;
	timer.reset();

	gdf_column_cpp asc_desc_col;
	asc_desc_col.create_gdf_column(GDF_INT8,
		gdf_dtype_extra_info{TIME_UNIT_NONE, nullptr},
		sortOrderTypes.size(),
		sortOrderTypes.data(),
		ral::traits::get_dtype_size_in_bytes(GDF_INT8),
		"");

	gdf_column_cpp index_col;
	index_col.create_gdf_column(GDF_INT32,
		gdf_dtype_extra_info{TIME_UNIT_NONE, nullptr},
		input.get_num_rows_in_table(0),
		nullptr,
		ral::traits::get_dtype_size_in_bytes(GDF_INT32),
		"");

	gdf_context context;
	context.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  // Nulls are are treated as largest

	CUDF_CALL(gdf_order_by(rawCols.data(),
		(int8_t *) (asc_desc_col.get_gdf_column()->data),
		rawCols.size(),
		index_col.get_gdf_column(),
		&context));

	Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "sort part 1 gdf_order_by"));
	timer.reset();

	for(int i = 0; i < sortedTable.size(); i++) {
		materialize_column(
			input.get_column(i).get_gdf_column(), sortedTable[i].get_gdf_column(), index_col.get_gdf_column());
		sortedTable[i].update_null_count();
	}

	Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "sort part 2 materialize_column"));
	timer.reset();
}

void single_node_sort(const Context & queryContext,
	blazing_frame & input,
	std::vector<gdf_column *> & rawCols,
	std::vector<int8_t> & sortOrderTypes) {
	std::vector<gdf_column_cpp> sortedTable(input.get_size_column(0));
	for(int i = 0; i < sortedTable.size(); i++) {
		auto & input_col = input.get_column(i);
		if(input_col.valid())
			sortedTable[i].create_gdf_column(input_col.dtype(),
				input_col.dtype_info(),
				input_col.size(),
				nullptr,
				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
				input_col.name());
		else
			sortedTable[i].create_gdf_column(input_col.dtype(),
				input_col.dtype_info(),
				input_col.size(),
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
				input_col.name());
	}

	sort(queryContext, input, rawCols, sortOrderTypes, sortedTable);

	input.clear();
	input.add_table(sortedTable);
}

void distributed_sort(Context & queryContext,
	blazing_frame & input,
	std::vector<gdf_column_cpp> & cols,
	std::vector<gdf_column *> & rawCols,
	std::vector<int8_t> & sortOrderTypes,
	std::vector<int> & sortColIndices) {
	using ral::communication::CommunicationData;
	static CodeTimer timer;
	timer.reset();

	std::vector<gdf_column_cpp> sortedTable(input.get_size_column(0));
	for(int i = 0; i < sortedTable.size(); i++) {
		auto & input_col = input.get_column(i);
		if(input_col.valid())
			sortedTable[i].create_gdf_column(input_col.dtype(),
				input_col.dtype_info(),
				input_col.size(),
				nullptr,
				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
				input_col.name());
		else
			sortedTable[i].create_gdf_column(input_col.dtype(),
				input_col.dtype_info(),
				input_col.size(),
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
				input_col.name());
	}

	size_t rowSize = input.get_num_rows_in_table(0);


	std::vector<gdf_column_cpp> selfSamples = ral::distribution::sampling::generateSample(cols, 0.1);

	Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "distributed_sort part 1 generateSample"));
	timer.reset();

	std::thread sortThread{[](Context & queryContext,
							   blazing_frame & input,
							   std::vector<gdf_column *> & rawCols,
							   std::vector<int8_t> & sortOrderTypes,
							   std::vector<gdf_column_cpp> & sortedTable) {
							   static CodeTimer timer2;
							   sort(queryContext, input, rawCols, sortOrderTypes, sortedTable);
							   Library::Logging::Logger().logInfo(
								   timer2.logDuration(queryContext, "distributed_sort part 2 async sort"));
							   timer2.reset();
						   },
		std::ref(queryContext),
		std::ref(input),
		std::ref(rawCols),
		std::ref(sortOrderTypes),
		std::ref(sortedTable)};
	// sort(queryContext, input, rawCols, sortOrderTypes, sortedTable);

	std::vector<gdf_column_cpp> partitionPlan;
	if(queryContext.isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		queryContext.incrementQuerySubstep();
		std::vector<ral::distribution::NodeSamples> samples = ral::distribution::collectSamples(queryContext);
		samples.emplace_back(rowSize, CommunicationData::getInstance().getSelfNode(), selfSamples);

		partitionPlan = ral::distribution::generatePartitionPlans(queryContext, samples, sortOrderTypes);

		queryContext.incrementQuerySubstep();
		ral::distribution::distributePartitionPlan(queryContext, partitionPlan);

		Library::Logging::Logger().logInfo(timer.logDuration(
			queryContext, "distributed_sort part 2 collectSamples generatePartitionPlans distributePartitionPlan"));
	} else {
		queryContext.incrementQuerySubstep();
		ral::distribution::sendSamplesToMaster(queryContext, selfSamples, rowSize);

		queryContext.incrementQuerySubstep();
		partitionPlan = ral::distribution::getPartitionPlan(queryContext);

		Library::Logging::Logger().logInfo(
			timer.logDuration(queryContext, "distributed_sort part 2 sendSamplesToMaster getPartitionPlan"));
	}

	// Wait for sortThread
	sortThread.join();
	timer.reset();  // lets do the reset here, since  part 2 async is capting the time

	if(partitionPlan[0].size() == 0) {
		return;
	}

	std::vector<ral::distribution::NodeColumns> partitions = ral::distribution::partitionData(
		queryContext, sortedTable, sortColIndices, partitionPlan, true, sortOrderTypes);
	Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "distributed_sort part 3 partitionData"));
	timer.reset();

	queryContext.incrementQuerySubstep();
	ral::distribution::distributePartitions(queryContext, partitions);
	std::vector<ral::distribution::NodeColumns> partitionsToMerge = ral::distribution::collectPartitions(queryContext);

	Library::Logging::Logger().logInfo(
		timer.logDuration(queryContext, "distributed_sort part 4 distributePartitions collectPartitions"));
	timer.reset();

	auto it = std::find_if(partitions.begin(), partitions.end(), [&](ral::distribution::NodeColumns & el) {
		return el.getNode() == CommunicationData::getInstance().getSelfNode();
	});
	// Could "it" iterator be partitions.end()?
	partitionsToMerge.push_back(*it);

	ral::distribution::sortedMerger(partitionsToMerge, sortOrderTypes, sortColIndices, input);
	Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "distributed_sort part 5 sortedMerger"));
	timer.reset();
}

void process_sort(blazing_frame & input, std::string query_part, Context * queryContext) {
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd);

	std::string limitRowsStr = get_named_expression(combined_expression, "fetch");

	size_t num_sort_columns = count_string_occurrence(combined_expression, "sort");

	std::vector<gdf_column_cpp> cols(num_sort_columns);
	std::vector<gdf_column *> rawCols(num_sort_columns);
	std::vector<int8_t> sortOrderTypes(num_sort_columns);
	std::vector<int> sortColIndices(num_sort_columns);
	for(int i = 0; i < num_sort_columns; i++) {
		int sort_column_index = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		cols[i] = input.get_column(sort_column_index).clone();
		rawCols[i] = cols[i].get_gdf_column();
		sortOrderTypes[i] =
			(get_named_expression(combined_expression, "dir" + std::to_string(i)) == DESCENDING_ORDER_SORT_TEXT);
		sortColIndices[i] = sort_column_index;
	}

	if(!queryContext || queryContext->getTotalNodes() <= 1) {
		if(num_sort_columns > 0) {
			single_node_sort(*queryContext, input, rawCols, sortOrderTypes);
		}
		if(!limitRowsStr.empty()) {
			limit_table(input, std::stoi(limitRowsStr));
		}
	} else {
		if(num_sort_columns > 0) {
			distributed_sort(*queryContext, input, cols, rawCols, sortOrderTypes, sortColIndices);
		}
		if(!limitRowsStr.empty()) {
			distributed_limit(*queryContext, input, std::stoi(limitRowsStr));
		}
	}
}

}  // namespace operators
}  // namespace ral
