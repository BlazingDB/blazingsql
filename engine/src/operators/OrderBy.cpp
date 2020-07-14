#include "OrderBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "communication/CommunicationData.h"
#include "distribution/primitives.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>
#include <cudf/strings/copying.hpp>
#include "parser/expression_utils.hpp"
#include "utilities/CommonOperations.h"

using namespace fmt::literals;

namespace ral {
namespace operators {

using blazingdb::manager::Context;
using blazingdb::transport::Node;
using ral::communication::CommunicationData;
using namespace ral::distribution;

const std::string ASCENDING_ORDER_SORT_TEXT = "ASC";
const std::string DESCENDING_ORDER_SORT_TEXT = "DESC";

/**---------------------------------------------------------------------------*
 * @brief Sorts the columns of the input table according the sortOrderTypes
 * and sortColIndices.
 *
 * @param[in] table             table whose rows need to be compared for ordering
 * @param[in] sortColIndices    The vector of selected column indices to perform
 *                              the sort.
 * @param[in] sortOrderTypes    The expected sort order for each column. Size
 *                              must be equal to `sortColIndices.size()` or empty.
 *
 * @returns A BlazingTable with rows sorted.
 *---------------------------------------------------------------------------**/
std::unique_ptr<ral::frame::BlazingTable> logicalSort(
  const ral::frame::BlazingTableView & table, const std::vector<int> & sortColIndices, const std::vector<cudf::order> & sortOrderTypes){

	CudfTableView sortColumns = table.view().select(sortColIndices);

	/*ToDo: Edit this according the Calcite output*/
	std::vector<cudf::null_order> null_orders(sortColIndices.size(), cudf::null_order::AFTER);

	std::unique_ptr<cudf::column> output = cudf::sorted_order( sortColumns, sortOrderTypes, null_orders );

	std::unique_ptr<cudf::table> gathered = cudf::gather( table.view(), output->view() );

	return std::make_unique<ral::frame::BlazingTable>( std::move(gathered), table.names() );
  }

std::unique_ptr<cudf::table> logicalLimit(const cudf::table_view& table, cudf::size_type limitRows) {
	assert(limitRows < table.num_rows());

	if (limitRows == 0) {
		return cudf::empty_like(table);
	}

	std::vector<std::unique_ptr<cudf::column>> output_cols;
	output_cols.reserve(table.num_columns());
	for(auto i = 0; i < table.num_columns(); ++i) {
		auto column = table.column(i);
		cudf::data_type columnType = column.type();

		std::unique_ptr<cudf::column> out_column;
		if(cudf::is_fixed_width(columnType)) {
			out_column = cudf::make_fixed_width_column(columnType, limitRows, column.has_nulls()?cudf::mask_state::UNINITIALIZED: cudf::mask_state::UNALLOCATED);
			cudf::mutable_column_view out_column_mutable_view = out_column->mutable_view();
			cudf::copy_range_in_place(column, out_column_mutable_view, 0, limitRows, 0);
		} else {
			out_column = cudf::strings::detail::slice(column, 0, limitRows);
		}
		output_cols.push_back(std::move(out_column));
	}

	return std::make_unique<cudf::table>(std::move(output_cols));
}

/**---------------------------------------------------------------------------*
 * @brief In a distributed context, this function determines what the limit would be
 * for this local node. It does this be distributing and collecting the total number of
 * rows in the table. Then knowing which node index this local node is, it can calculate
 * how many rows are ahead of the ones in this partition
 *
 * @param[in] contex
 * @param[in] local_num_rows    Number of rows of this partition
 * @param[in] limit_rows        Limit being applied to the whole query
 *
 * @returns The limit that would be applied to this partition
 *---------------------------------------------------------------------------**/
int64_t determine_local_limit(Context * context, int64_t local_num_rows, cudf::size_type limit_rows){
	// context->incrementQuerySubstep();
	// ral::distribution::distributeNumRows(context, local_num_rows);

	// std::vector<int64_t> nodesRowSize = ral::distribution::collectNumRows(context);
	// int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	// int64_t prev_total_rows = std::accumulate(nodesRowSize.begin(), nodesRowSize.begin() + self_node_idx, int64_t(0));

	// return std::min(std::max(limit_rows - prev_total_rows, int64_t{0}), local_num_rows);

	return 0;
}

std::tuple<std::vector<int>, std::vector<cudf::order>, cudf::size_type>
get_sort_vars(const std::string & query_part) {
	auto rangeStart = query_part.find("(");
	auto rangeEnd = query_part.rfind(")") - rangeStart - 1;
	std::string combined_expression = query_part.substr(rangeStart + 1, rangeEnd);

	int num_sort_columns = count_string_occurrence(combined_expression, "sort");

	std::vector<int> sortColIndices(num_sort_columns);
	std::vector<cudf::order> sortOrderTypes(num_sort_columns);
	for(auto i = 0; i < num_sort_columns; i++) {
		sortColIndices[i] = get_index(get_named_expression(combined_expression, "sort" + std::to_string(i)));
		sortOrderTypes[i] = (get_named_expression(combined_expression, "dir" + std::to_string(i)) == ASCENDING_ORDER_SORT_TEXT ? cudf::order::ASCENDING : cudf::order::DESCENDING);
	}

	std::string limitRowsStr = get_named_expression(combined_expression, "fetch");
	cudf::size_type limitRows = !limitRowsStr.empty() ? std::stoi(limitRowsStr) : -1;

	return std::make_tuple(sortColIndices, sortOrderTypes, limitRows);
}

bool has_limit_only(const std::string & query_part){
	std::vector<int> sortColIndices;
	std::tie(sortColIndices, std::ignore, std::ignore) = get_sort_vars(query_part);

	return sortColIndices.empty();
}

int64_t get_limit_rows_when_relational_alg_is_simple(const std::string & query_part){
	int64_t limitRows;
	std::tie(std::ignore, std::ignore, limitRows) = get_sort_vars(query_part);
	return limitRows;
}

std::pair<std::unique_ptr<ral::frame::BlazingTable>, int64_t>
limit_table(std::unique_ptr<ral::frame::BlazingTable> table, int64_t num_rows_limit) {

	cudf::size_type table_rows = table->num_rows();
	if (num_rows_limit <= 0) {
		return std::make_pair(std::make_unique<ral::frame::BlazingTable>(cudf::empty_like(table->view()), table->names()), 0);
	} else if (num_rows_limit >= table_rows)	{
		return std::make_pair(std::move(table), num_rows_limit - table_rows);
	} else {
		return std::make_pair(std::make_unique<ral::frame::BlazingTable>(logicalLimit(table->view(), num_rows_limit), table->names()), 0);
	}
}

std::unique_ptr<ral::frame::BlazingTable> sort(const ral::frame::BlazingTableView & table, const std::string & query_part){
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	return logicalSort(table, sortColIndices, sortOrderTypes);
}

std::unique_ptr<ral::frame::BlazingTable> sample(const ral::frame::BlazingTableView & table, const std::string & query_part){
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	auto tableNames = table.names();
	std::vector<std::string> sortColNames(sortColIndices.size());
  std::transform(sortColIndices.begin(), sortColIndices.end(), sortColNames.begin(), [&](auto index) { return tableNames[index]; });

	ral::frame::BlazingTableView sortColumns(table.view().select(sortColIndices), sortColNames);

	std::unique_ptr<ral::frame::BlazingTable> selfSamples = ral::distribution::sampling::generateSamplesFromRatio(sortColumns, 0.1);
	return selfSamples;
}


std::vector<cudf::table_view> partition_table(const ral::frame::BlazingTableView & partitionPlan, const ral::frame::BlazingTableView & sortedTable, const std::string & query_part) {
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	if(sortedTable.num_rows() == 0) {
		return {sortedTable.view()};
	}

	// TODO this is just a default setting. Will want to be able to properly set null_order
	std::vector<cudf::null_order> null_orders(sortOrderTypes.size(), cudf::null_order::AFTER);

	cudf::table_view columns_to_search = sortedTable.view().select(sortColIndices);
	auto pivot_indexes = cudf::upper_bound(columns_to_search, partitionPlan.view(),
												sortOrderTypes, null_orders);

	std::vector<cudf::size_type> split_indexes = ral::utilities::vector_to_column<cudf::size_type>(pivot_indexes->view());
	return cudf::split(sortedTable.view(), split_indexes);
}

std::unique_ptr<ral::frame::BlazingTable> generate_partition_plan(const std::vector<ral::frame::BlazingTableView> & samples,
	std::size_t table_num_rows, std::size_t avg_bytes_per_row, const std::string & query_part, Context * context){
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;

	std::size_t num_bytes_per_order_by_partition = 400000000;
	int max_num_order_by_partitions_per_node = 8;
	std::map<std::string, std::string> config_options = context->getConfigOptions();
	auto it = config_options.find("NUM_BYTES_PER_ORDER_BY_PARTITION");
	if (it != config_options.end()){
		num_bytes_per_order_by_partition = std::stoull(config_options["NUM_BYTES_PER_ORDER_BY_PARTITION"]);
	}
	it = config_options.find("MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE");
	if (it != config_options.end()){
		max_num_order_by_partitions_per_node = std::stoi(config_options["MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE"]);
	}

	int num_nodes = context->getTotalNodes();
	cudf::size_type total_num_partitions = (double)table_num_rows*(double)avg_bytes_per_row/(double)num_bytes_per_order_by_partition;
	total_num_partitions = total_num_partitions <= 0 ? 1 : total_num_partitions;
	// want to make the total_num_partitions to be a multiple of the number of nodes to evenly distribute
	total_num_partitions = ((total_num_partitions + num_nodes - 1) / num_nodes) * num_nodes;
	total_num_partitions = total_num_partitions > max_num_order_by_partitions_per_node * num_nodes ? max_num_order_by_partitions_per_node * num_nodes : total_num_partitions;

	std::string info = "table_num_rows: " + std::to_string(table_num_rows) + " avg_bytes_per_row: " + std::to_string(avg_bytes_per_row) +
							" total_num_partitions: " + std::to_string(total_num_partitions) +
							" NUM_BYTES_PER_ORDER_BY_PARTITION: " + std::to_string(num_bytes_per_order_by_partition) +
							" MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE: " + std::to_string(max_num_order_by_partitions_per_node);

	auto logger = spdlog::get("batch_logger");
	logger->debug("{query_id}|{step}|{substep}|{info}|||||",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="Determining Number of Order By Partitions " + info);

	
	if( ral::utilities::checkIfConcatenatingStringsWillOverflow(samples)) {
		logger->warn("{query_id}|{step}|{substep}|{info}",
						"query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
						"step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
						"substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
						"info"_a="In generatePartitionPlans Concatenating Strings will overflow strings length");
	}

	partitionPlan = generatePartitionPlans(total_num_partitions, samples, sortOrderTypes);
	context->incrementQuerySubstep();

	return partitionPlan;
}

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<ral::frame::BlazingTableView> partitions_to_merge, const std::string & query_part) {
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);
	return sortedMerger(partitions_to_merge, sortOrderTypes, sortColIndices);
}

}  // namespace operators
}  // namespace ral
