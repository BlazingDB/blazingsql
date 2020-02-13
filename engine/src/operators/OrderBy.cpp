#include "OrderBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "cudf.h"
#include "Traits/RuntimeTraits.h"
#include "communication/CommunicationData.h"
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

using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Node;
using ral::communication::experimental::CommunicationData;
using namespace ral::distribution::experimental;

const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";

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

std::unique_ptr<cudf::experimental::table> logicalLimit(
	const cudf::table_view& table,
	cudf::size_type limitRows)
{
	assert(limitRows < table.num_rows());

	if (limitRows == 0) {
		return cudf::experimental::empty_like(table);
	}
	
	std::vector<std::unique_ptr<cudf::column>> output_cols;
	output_cols.reserve(table.num_columns());
	for(auto i = 0; i < table.num_columns(); ++i) {
		auto column = table.column(i);
		cudf::data_type columnType = column.type();

		std::unique_ptr<cudf::column> out_column;
		if(cudf::is_fixed_width(columnType)) {
			out_column = cudf::make_fixed_width_column(columnType, limitRows);
			cudf::mutable_column_view out_column_mutable_view = out_column->mutable_view();
			cudf::experimental::copy_range(column, out_column_mutable_view, 0, limitRows, 0);			
		} else {
			out_column = cudf::strings::detail::slice(column, 0, limitRows);
		}
		output_cols.push_back(std::move(out_column));
	}

	return std::make_unique<cudf::experimental::table>(std::move(output_cols));
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
		return std::make_unique<BlazingTable>(cudf::experimental::empty_like(table.view()), table.names());
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

cudf::size_type determine_local_limit(Context * context, cudf::size_type local_num_rows, cudf::size_type limit_rows){
	context->incrementQuerySubstep();
	ral::distribution::experimental::distributeNumRows(context, local_num_rows);

	std::vector<cudf::size_type> nodesRowSize = ral::distribution::experimental::collectNumRows(context);
	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	cudf::size_type prev_total_rows = std::accumulate(nodesRowSize.begin(), nodesRowSize.begin() + self_node_idx, 0);

	return std::min(std::max(limit_rows - prev_total_rows, 0), local_num_rows);
}

std::unique_ptr<ral::frame::BlazingTable> process_sort(const ral::frame::BlazingTableView & table, const std::string & query_part, Context * context) {

	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd);

	std::string limitRowsStr = get_named_expression(combined_expression, "fetch");
	cudf::size_type limitRows = !limitRowsStr.empty() ? std::stoi(limitRowsStr) : table.num_rows();

	size_t num_sort_columns = count_string_occurrence(combined_expression, "sort");

	std::vector<int8_t> sortOrderTypes(num_sort_columns);
	std::vector<int> sortColIndices(num_sort_columns);
	for(int i = 0; i < num_sort_columns; i++) {
		sortColIndices[i] = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		sortOrderTypes[i] =
			(get_named_expression(combined_expression, "dir" + std::to_string(i)) == DESCENDING_ORDER_SORT_TEXT);
	}

	std::unique_ptr<ral::frame::BlazingTable> out_blz_table;
	cudf::table_view table_view = table.view();
	if(context->getTotalNodes() <= 1) {
		if(num_sort_columns > 0) {
			out_blz_table = logicalSort(table, sortColIndices, sortOrderTypes);
			table_view = out_blz_table->view();
		}
		
		if(limitRows >= 0 && limitRows < table_view.num_rows()) {
			auto out_table = logicalLimit(table_view, limitRows);
			out_blz_table = std::make_unique<ral::frame::BlazingTable>( std::move(out_table), table.names() );
		}
	} else {
		if(num_sort_columns > 0) {
			out_blz_table = distributed_sort(context, table, sortColIndices, sortOrderTypes);
			table_view = out_blz_table->view();
		}

		limitRows = determine_local_limit(context, table_view.num_rows(), limitRows);
		if(limitRows >= 0 && limitRows < table_view.num_rows()) {
			auto out_table = logicalLimit(table_view, limitRows);
			out_blz_table = std::make_unique<ral::frame::BlazingTable>( std::move(out_table), table.names() );
		}
	}

	assert(!!out_blz_table);

	return out_blz_table;
}

}  // namespace experimental
}  // namespace operators
}  // namespace ral
