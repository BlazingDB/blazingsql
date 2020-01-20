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
#include <cudf/sorting.hpp>
#include <cudf/copying.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/column/column_factories.hpp>
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

void limit_table(blazing_frame & input, cudf::size_type limitRows) {
	cudf::size_type rowSize = input.get_num_rows_in_table(0);

	if(limitRows < rowSize) {
		for(size_t i = 0; i < input.get_size_column(0); ++i) {
			auto & input_col = input.get_column(i);
			if(input_col.get_gdf_column()->type().id() == cudf::type_id::STRING) {
				// TODO percy cudf0.12 custrings this was not commented
				//ral::truncate_nvcategory(input_col.get_gdf_column(), limitRows);
			} else {
				// TODO percy cudf0.12 port to cudf::column
//				gdf_column_cpp limitedCpp;
//				gdf_column * sourceColumn = input_col.get_gdf_column();
//				cudf::size_type width_per_value = ral::traits::get_dtype_size_in_bytes(sourceColumn);
//				limitedCpp.create_gdf_column(to_type_id(sourceColumn->dtype),
//					limitRows,
//					nullptr,
//					nullptr,
//					width_per_value,
//					sourceColumn->col_name);
//				gdf_column * limitedColumn = limitedCpp.get_gdf_column();
//				cudf::copy_range(limitedColumn, *sourceColumn, 0, limitRows, 0);
//				input.set_column(i, limitedCpp);
			}
		}
	}
}

void distributed_limit(Context & queryContext, blazing_frame & input, cudf::size_type limitRows) {
	using ral::communication::CommunicationData;
	using ral::distribution::NodeSamples;

	cudf::size_type rowSize = input.get_num_rows_in_table(0);

	queryContext.incrementQuerySubstep();
	ral::distribution::distributeRowSize(queryContext, rowSize);
	std::vector<cudf::size_type> nodesRowSize = ral::distribution::collectRowSize(queryContext);

	int self_node_idx = queryContext.getNodeIndex(CommunicationData::getInstance().getSelfNode());

	cudf::size_type prevTotalRows = std::accumulate(nodesRowSize.begin(), nodesRowSize.begin() + self_node_idx, 0);

	if(prevTotalRows + rowSize > limitRows) {
		limit_table(input, std::max(limitRows - prevTotalRows, 0));
	}
}

void sort(cudf::table_view & input,
	std::vector<cudf::column_view> & rawCols,
	std::vector<int8_t> & sortOrderTypes,
	std::vector<gdf_column_cpp> & sortedTable) {

	/*
	gdf_column_cpp asc_desc_col;
	asc_desc_col.create_gdf_column(cudf::type_id::INT8,
		sortOrderTypes.size(),
		sortOrderTypes.data(),
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT8),
		"");

	gdf_column_cpp index_col;
	index_col.create_gdf_column(cudf::type_id::INT32,
		input.get_num_rows_in_table(0),
		nullptr,
		ral::traits::get_dtype_size_in_bytes(cudf::type_id::INT32),
		"");

	gdf_context context;
	context.flag_null_sort_behavior = GDF_NULL_AS_LARGEST;  // Nulls are are treated as largest*/

	// TODO percy cudf0.12 port to cudf::column
//	CUDF_CALL(gdf_order_by(rawCols.data(),
//		(int8_t *) (asc_desc_col.get_gdf_column()->data),
//		rawCols.size(),
//		index_col.get_gdf_column(),
//		&context));

/*
	std::unique_ptr<column> sorted_order(
		table_view input, std::vector<order> const& column_order = {},
		std::vector<null_order> const& null_precedence = {},
		rmm::mr::device_memory_resource* mr = rmm::mr::get_default_resource());
*/

	/*for(int i = 0; i < sortedTable.size(); i++) {
		materialize_column(
			input.get_column(i).get_gdf_column(), sortedTable[i].get_gdf_column(), index_col.get_gdf_column());
		
		// TODO percy cudf0.12 port to cudf::column
		//sortedTable[i].update_null_count();
	}*/
}

void single_node_sort(const Context & queryContext,
	blazing_frame & input,
	std::vector<cudf::column_view> & rawCols,
	std::vector<int8_t> & sortOrderTypes) {
	std::vector<gdf_column_cpp> sortedTable(input.get_size_column(0));
	for(int i = 0; i < sortedTable.size(); i++) {
		auto & input_col = input.get_column(i);
		
		// TODO percy cudf0.12 port to cudf::column
//		if(input_col.valid()) {
//			sortedTable[i].create_gdf_column(input_col.dtype(),
//				input_col.size(),
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
//				input_col.name());
//		}
//		else {
//			sortedTable[i].create_gdf_column(input_col.dtype(),
//				input_col.size(),
//				nullptr,
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
//				input_col.name());
//		}
	}

	//sort(queryContext, input, rawCols, sortOrderTypes, sortedTable);

	input.clear();
	input.add_table(sortedTable);
}

void distributed_sort(Context & queryContext,
	blazing_frame & input,
	std::vector<gdf_column_cpp> & cols,
	std::vector<cudf::column *> & rawCols,
	std::vector<int8_t> & sortOrderTypes,
	std::vector<int> & sortColIndices) {
	using ral::communication::CommunicationData;
	static CodeTimer timer;
	timer.reset();

	std::vector<gdf_column_cpp> sortedTable(input.get_size_column(0));
	for(int i = 0; i < sortedTable.size(); i++) {
		auto & input_col = input.get_column(i);
		
		// TODO percy cudf0.12 port to cudf::column
//		if(input_col.valid()) {
//			sortedTable[i].create_gdf_column(input_col.dtype(),
//				input_col.size(),
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
//				input_col.name());
//		}
//		else {
//			sortedTable[i].create_gdf_column(input_col.dtype(),
//				input_col.size(),
//				nullptr,
//				nullptr,
//				ral::traits::get_dtype_size_in_bytes(input_col.dtype()),
//				input_col.name());
//		}
	}

	size_t rowSize = input.get_num_rows_in_table(0);


	std::vector<gdf_column_cpp> selfSamples = ral::distribution::sampling::generateSample(cols, 0.1);

	Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "distributed_sort part 1 generateSample"));
	timer.reset();

	/*std::thread sortThread{[](Context & queryContext,
							   blazing_frame & input,
							   std::vector<cudf::column *> & rawCols,
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
		std::ref(sortedTable)};*/
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
	//sortThread.join();
	timer.reset();  // lets do the reset here, since  part 2 async is capting the time

	if(partitionPlan[0].get_gdf_column()->size() == 0) {
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
	std::vector<cudf::column_view> rawCols(num_sort_columns);
	std::vector<int8_t> sortOrderTypes(num_sort_columns);
	std::vector<int> sortColIndices(num_sort_columns);
	for(int i = 0; i < num_sort_columns; i++) {
		int sort_column_index = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		cols[i] = input.get_column(sort_column_index).clone();
		//rawCols[i] = cols[i].get_gdf_column();
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
			// TODO percy cudf0.12 port to cudf::column
			//distributed_sort(*queryContext, input, cols, rawCols, sortOrderTypes, sortColIndices);
		}
		if(!limitRowsStr.empty()) {
			distributed_limit(*queryContext, input, std::stoi(limitRowsStr));
		}
	}
}



}  // namespace operators
}  // namespace ral

namespace ral {
namespace operators {
namespace experimental {

	typedef blazingdb::manager::experimental::Context Context;
	typedef blazingdb::transport::experimental::Node Node;
	typedef ral::communication::experimental::CommunicationData CommunicationData;
	using namespace ral::distribution::experimental;

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

	if(context->getTotalNodes() <= 1) {
		if(num_sort_columns > 0) {
			std::unique_ptr<ral::frame::BlazingTable> sorted_table = logicalSort(table, sortColIndices, sortOrderTypes);

			if(!limitRowsStr.empty()) {
				cudf::size_type limitRows = std::stoi(limitRowsStr);
				if (limitRows < sorted_table->view().num_rows()){ // lets do the limit only if it will actually do sometihng
					return logicalLimit(sorted_table->toBlazingTableView(), limitRows);
				} else {
					return sorted_table;
				}
			} else {
				return sorted_table;
			}
		} else if(!limitRowsStr.empty()) {
			cudf::size_type limitRows = std::stoi(limitRowsStr);
			if (limitRows < table.view().num_rows()){ // lets do the limit only if it will actually do sometihng
				return logicalLimit(table, limitRows);
			} else {
				// TODO: in the future we want to have a way to just return the input and not make a clone
				return table.clone();
			}
		} else { // this should never happen, you either have a sort or a filter,otherwise you should have never called this function
			return table.clone();
		}
	} else {
		if(num_sort_columns > 0) {
			return distributed_sort(context, table, sortColIndices, sortOrderTypes);
		}
		if(!limitRowsStr.empty()) {
			// distributed_limit(context, input, std::stoi(limitRowsStr));
		}
	}
}

std::unique_ptr<ral::frame::BlazingTable>  distributed_sort(Context * context,
	const ral::frame::BlazingTableView & table, const std::vector<int> & sortColIndices, const std::vector<int8_t> & sortOrderTypes){
	
	static CodeTimer timer;
	timer.reset();

	size_t total_rows_table = table.view().num_rows();

	ral::frame::BlazingTableView sortColumns(table.view().select(sortColIndices), table.names());
	
	std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::experimental::generateSamples(
																sortColumns, 0.1);

	// WSM TODO cudf0.12 waiting on logger
	// Library::Logging::Logger().logInfo(timer.logDuration(context, "distributed_sort part 1 generateSample"));
	timer.reset();

	std::unique_ptr<ral::frame::BlazingTable> sortedTable;
	std::thread sortThread{[](Context * context,
							   const ral::frame::BlazingTableView & table,
							   const std::vector<int> & sortColIndices,
							   const std::vector<int8_t> & sortOrderTypes,
							   std::unique_ptr<ral::frame::BlazingTable> & sortedTable) {
							   static CodeTimer timer2;
							   sortedTable = logicalSort(table, sortColIndices, sortOrderTypes);
							   // WSM TODO cudf0.12 waiting on logger
							//    Library::Logging::Logger().logInfo(
							// 	   timer2.logDuration(context, "distributed_sort part 2 async sort"));
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

		// WSM TODO cudf0.12 waiting on logger
		// Library::Logging::Logger().logInfo(timer.logDuration(
		// 	context, "distributed_sort part 2 collectSamples generatePartitionPlans distributePartitionPlan"));
	} else {
		context->incrementQuerySubstep();
		sendSamplesToMaster(context, selfSamples->toBlazingTableView(), total_rows_table);

		context->incrementQuerySubstep();
		partitionPlan = getPartitionPlan(context);

		// WSM TODO cudf0.12 waiting on logger
		// Library::Logging::Logger().logInfo(
		// 	timer.logDuration(context, "distributed_sort part 2 sendSamplesToMaster getPartitionPlan"));
	}

	// Wait for sortThread
	sortThread.join();
	timer.reset();  // lets do the reset here, since  part 2 async is capting the time

	if(partitionPlan->view().num_rows() == 0) {
		return std::unique_ptr<ral::frame::BlazingTable>();
	}

	std::vector<NodeColumnView> partitions = partitionData(
							context, sortedTable->toBlazingTableView(), partitionPlan->toBlazingTableView(), sortColIndices, sortOrderTypes);
	
	// WSM TODO cudf0.12 waiting on logger
	// Library::Logging::Logger().logInfo(timer.logDuration(context, "distributed_sort part 3 partitionData"));
	timer.reset();

	context->incrementQuerySubstep();
	distributePartitions(context, partitions);
	std::vector<NodeColumn> collected_partitions = collectPartitions(context);

	// WSM TODO cudf0.12 waiting on logger
	// Library::Logging::Logger().logInfo(
	// 	timer.logDuration(context, "distributed_sort part 4 distributePartitions collectPartitions"));
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
	// WSM TODO cudf0.12 waiting on logger
	// Library::Logging::Logger().logInfo(timer.logDuration(context, "distributed_sort part 5 sortedMerger"));
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


// This function will return a new BlazingTable that only has limitRows rows. 
// This function should only be called if a limit will actually be applied, otherwise it will just make a copy, which we would want to avoid
std::unique_ptr<ral::frame::BlazingTable> logicalLimit(
  const ral::frame::BlazingTableView & table, cudf::size_type limitRows){

	cudf::size_type rowSize = table.view().num_rows();

	std::vector<std::unique_ptr<cudf::column>> output_cols;

	if(limitRows < rowSize) {

		for(size_t i = 0; i < table.view().num_columns(); ++i) {
			std::unique_ptr<cudf::column> mycolumn = cudf::make_numeric_column( table.view().column(i).type(), limitRows);
			std::unique_ptr<cudf::column> output = cudf::experimental::copy_range(table.view().column(i), *mycolumn, 0, limitRows, 0);
			output_cols.push_back(std::move(output));
		}
		return std::make_unique<ral::frame::BlazingTable>( 
			std::make_unique<cudf::experimental::table>( std::move(output_cols) ), table.names() );
	} else {
		return table.clone();
	}	
  }

}  // namespace experimental
}  // namespace operators
}  // namespace ral
