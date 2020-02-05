#include "OrderBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "ColumnManipulation.cuh"
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
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/strings/copying.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column_factories.hpp>

namespace ral {
namespace operators {
namespace experimental {

	typedef blazingdb::manager::experimental::Context Context;
	typedef blazingdb::transport::experimental::Node Node;
	typedef ral::communication::experimental::CommunicationData CommunicationData;
	using namespace ral::distribution::experimental;

const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";

std::unique_ptr<ral::frame::BlazingTable> process_sort(const ral::frame::BlazingTableView & table, const std::string & query_part, Context * context) {

	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd);

	std::string limitRowsStr = get_named_expression(combined_expression, "fetch");

	size_t num_sort_columns = count_string_occurrence(combined_expression, "sort");

	std::vector<int8_t> sortOrderTypes(num_sort_columns);
	std::vector<int> sortColIndices(num_sort_columns);
	for(int i = 0; i < num_sort_columns; i++) {
		sortColIndices[i] = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		sortOrderTypes[i] =
			(get_named_expression(combined_expression, "dir" + std::to_string(i)) == DESCENDING_ORDER_SORT_TEXT);
	}

	bool apply_limit = (limitRowsStr.empty() == false);
	cudf::size_type limitRows = apply_limit ? std::stoi(limitRowsStr) : -1;
	if(context->getTotalNodes() <= 1) {
		if (limitRows > 0)
			apply_limit = limitRows < table.num_rows();

		if(num_sort_columns > 0) {
			std::unique_ptr<ral::frame::BlazingTable> sorted_table = logicalSort(table, sortColIndices, sortOrderTypes);

			if(apply_limit) {
				return logicalLimit(sorted_table->toBlazingTableView(), limitRows);
			} else {
				return sorted_table;
			}
		} else if(apply_limit) {
			return logicalLimit(table, limitRows);
		} else { // this should never happen, you either have a sort or a filter,otherwise you should have never called this function
			return table.clone();
		}
	} else {
		if(num_sort_columns > 0) {
			std::unique_ptr<ral::frame::BlazingTable> sorted_table = distributed_sort(context, table, sortColIndices, sortOrderTypes);

			apply_limit = limitRows < sorted_table->num_rows() && limitRows!=-1;
			limitRows = determine_local_limit(context, sorted_table->num_rows(), limitRows);
			if(apply_limit) {
				if(limitRows == 0){
					return std::make_unique<BlazingTable>(cudf::experimental::empty_like(sorted_table->view()), table.names());
				}
				else{
					return logicalLimit(sorted_table->toBlazingTableView(), limitRows);
				}
			} else {
				return sorted_table;
			}
		} else if(apply_limit) {
			limitRows = determine_local_limit(context, table.num_rows(), limitRows);
			apply_limit = limitRows < table.num_rows();
			if(apply_limit) {
				return logicalLimit(table, limitRows);
			} else {
				return table.clone();
			}
		} else { // this should never happen, you either have a sort or a filter,otherwise you should have never called this function
			return table.clone();
		}
	}
}



std::unique_ptr<ral::frame::BlazingTable>  distributed_sort(Context * context,
	const ral::frame::BlazingTableView & table, const std::vector<int> & sortColIndices, const std::vector<int8_t> & sortOrderTypes){


	static CodeTimer timer;
	timer.reset();

	size_t total_rows_table = table.view().num_rows();

	ral::frame::BlazingTableView sortColumns(table.view().select(sortColIndices), table.names());

	std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamplesFromRatio(
																sortColumns, 0.1);

	Library::Logging::Logger().logInfo(timer.logDuration(*context, "distributed_sort part 1 generateSamplesFromRatio"));
	timer.reset();

	std::unique_ptr<ral::frame::BlazingTable> sortedTable;
	std::thread sortThread{[](Context * context,
							   const ral::frame::BlazingTableView & table,
							   const std::vector<int> & sortColIndices,
							   const std::vector<int8_t> & sortOrderTypes,
							   std::unique_ptr<ral::frame::BlazingTable> & sortedTable) {
							   static CodeTimer timer2;
							   sortedTable = logicalSort(table, sortColIndices, sortOrderTypes);
							    Library::Logging::Logger().logInfo(
								   timer2.logDuration(*context, "distributed_sort part 2 async sort"));
							   timer2.reset();
						   },
		context,
		std::ref(table),
		std::ref(sortColIndices),
		std::ref(sortOrderTypes),
		std::ref(sortedTable)};

	// std::unique_ptr<ral::frame::BlazingTable> sortedTable = logicalSort(table, sortColIndices, sortOrderTypes);

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

		partitionPlan = generatePartitionPlans(context, samples, total_rows_tables, sortOrderTypes);

		context->incrementQuerySubstep();
		distributePartitionPlan(context, partitionPlan->toBlazingTableView());

		Library::Logging::Logger().logInfo(timer.logDuration(
		 	*context, "distributed_sort part 2 collectSamples generatePartitionPlans distributePartitionPlan"));
	} else {
		context->incrementQuerySubstep();
		sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

		context->incrementQuerySubstep();
		partitionPlan = getPartitionPlan(context);

		Library::Logging::Logger().logInfo(
			timer.logDuration(*context, "distributed_sort part 2 sendSamplesToMaster getPartitionPlan"));
	}

	// Wait for sortThread
	sortThread.join();
	timer.reset();  // lets do the reset here, since  part 2 async is capting the time

	if(partitionPlan->view().num_rows() == 0) {
		return std::unique_ptr<ral::frame::BlazingTable>();
	}

	std::vector<NodeColumnView> partitions = partitionData(
							context, sortedTable->toBlazingTableView(), partitionPlan->toBlazingTableView(), sortColIndices, sortOrderTypes);

	Library::Logging::Logger().logInfo(timer.logDuration(*context, "distributed_sort part 3 partitionData"));
	timer.reset();

	context->incrementQuerySubstep();
	distributePartitions(context, partitions);
	std::vector<NodeColumn> collected_partitions = collectPartitions(context);

	Library::Logging::Logger().logInfo(
	 	timer.logDuration(*context, "distributed_sort part 4 distributePartitions collectPartitions"));
	timer.reset();

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

	std::unique_ptr<ral::frame::BlazingTable> merged = sortedMerger(partitions_to_merge, sortOrderTypes, sortColIndices);
	Library::Logging::Logger().logInfo(timer.logDuration(*context, "distributed_sort part 5 sortedMerger"));
	timer.reset();

	return merged;
}

// esto equivale mas o menos a single_node_sort pero con unos inputs un poco diferentes
std::unique_ptr<ral::frame::BlazingTable> logicalSort(
  const ral::frame::BlazingTableView & table, const std::vector<int> & sortColIndices, const std::vector<int8_t> & sortOrderTypes){

	std::vector<cudf::order> column_order;
	for(auto col_order : sortOrderTypes){
		if(col_order)
			column_order.push_back(cudf::order::DESCENDING);
		else
			column_order.push_back(cudf::order::ASCENDING);
	}

	CudfTableView sortColumns = table.view().select(sortColIndices);

	/*ToDo: Edit this according the Calcite output*/
	std::vector<cudf::null_order> null_orders(sortColIndices.size(), cudf::null_order::AFTER);

	std::unique_ptr<cudf::column> output = cudf::experimental::sorted_order( sortColumns, column_order, null_orders );

	std::unique_ptr<cudf::experimental::table> gathered = cudf::experimental::gather( table.view(), output->view() );

	return std::make_unique<ral::frame::BlazingTable>( std::move(gathered), table.names() );
  }


cudf::size_type determine_local_limit(Context * context,
	const cudf::size_type local_num_rows, cudf::size_type limit_rows){

	context->incrementQuerySubstep();
	ral::distribution::experimental::distributeNumRows(context, local_num_rows);

	std::vector<cudf::size_type> nodesRowSize = ral::distribution::experimental::collectNumRows(context);
	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	cudf::size_type prev_total_rows = std::accumulate(nodesRowSize.begin(), nodesRowSize.begin() + self_node_idx, 0);

	return std::max(limit_rows - prev_total_rows, 0);
}


// This function will return a new BlazingTable that only has limitRows rows.
// This function should only be called if a limit will actually be applied, otherwise it will just make a copy, which we would want to avoid
std::unique_ptr<ral::frame::BlazingTable> logicalLimit(
	const ral::frame::BlazingTableView & table, cudf::size_type limitRows) {
	cudf::size_type rowSize = table.view().num_rows();

	std::vector<std::unique_ptr<cudf::column>> output_cols;

	if(limitRows < rowSize) {
		for(size_t i = 0; i < table.view().num_columns(); ++i) {
			cudf::data_type columnType = table.view().column(i).type();
			cudf::type_id columnTypeId = columnType.id();

			if((cudf::CATEGORY == columnTypeId) || (cudf::EMPTY == columnTypeId) ||
				(cudf::NUM_TYPE_IDS == columnTypeId)) {
				throw std::runtime_error("Unsupported column type");
			}

			if(cudf::STRING == columnTypeId) {
				std::unique_ptr<cudf::column> output =
					cudf::strings::detail::slice(table.view().column(i), 0, limitRows);
				output_cols.push_back(std::move(output));
			} else {
				std::unique_ptr<cudf::column> mycolumn = cudf::make_fixed_width_column(columnType, limitRows);
				std::unique_ptr<cudf::column> output =
					cudf::experimental::copy_range(table.view().column(i), *mycolumn, 0, limitRows, 0);
				output_cols.push_back(std::move(output));
			}
		}
		return std::make_unique<ral::frame::BlazingTable>(
			std::make_unique<cudf::experimental::table>(std::move(output_cols)), table.names());
	} else {
		return table.clone();
	}
}

}  // namespace experimental
}  // namespace operators
}  // namespace ral
