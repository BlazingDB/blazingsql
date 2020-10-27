#include "OrderBy.h"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "communication/CommunicationData.h"
#include "distribution/primitives.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/copying.hpp>
#include <cudf/sorting.hpp>
#include <cudf/search.hpp>
#include <random>
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
	} else if (limitRows < table.num_rows()) {
		std::vector<cudf::size_type> splits = {limitRows};
		std::vector<cudf::table_view> split_table = cudf::split(table, splits);
		return std::make_unique<cudf::table>(split_table[0]);
	} else {
		return std::make_unique<cudf::table>(table);
	}
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
	context->incrementQuerySubstep();
	ral::distribution::distributeNumRows(context, local_num_rows);

	std::vector<int64_t> nodesRowSize = ral::distribution::collectNumRows(context);
	int self_node_idx = context->getNodeIndex(CommunicationData::getInstance().getSelfNode());
	int64_t prev_total_rows = std::accumulate(nodesRowSize.begin(), nodesRowSize.begin() + self_node_idx, int64_t(0));

	return std::min(std::max(limit_rows - prev_total_rows, int64_t{0}), local_num_rows);
}

auto get_sort_vars(const std::string & query_part) {
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

int64_t get_local_limit(int64_t total_batch_rows, const std::string & query_part, Context * context){
	cudf::size_type limitRows;
	std::tie(std::ignore, std::ignore, limitRows) = get_sort_vars(query_part);

	if(context->getTotalNodes() > 1 && limitRows >= 0) {
		limitRows = determine_local_limit(context, total_batch_rows, limitRows);
	}

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

std::unique_ptr<ral::frame::BlazingTable> sample(const ral::frame::BlazingTableView & table, const std::string & query_part, Context * context, float samples_ratio){
	auto tableNames = table.names();

	// more homogeneus when we have many workers
	int num_nodes = context->getTotalNodes();
	int total_samples = std::ceil(table.num_rows() * samples_ratio);
	if (total_samples < num_nodes && num_nodes < table.num_rows()) {
		total_samples = num_nodes;
	}

	std::random_device rd;
	auto samples = cudf::sample(table.view(), total_samples, cudf::sample_with_replacement::FALSE, rd());

	return std::make_unique<ral::frame::BlazingTable>(std::move(samples), tableNames);
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

	// This mean master node does not have data, so we want to recompute the `avg_bytes_per_row` value
	// using the average of all the samples that contains data
	if (avg_bytes_per_row == 1 && samples.size() > 1 && table_num_rows > 0) {
		size_t local_average_by_sample = 0;
		int count_non_empty = 0;
		for (int i = 0; i < samples.size(); ++i) {
			if (samples[i].num_rows() > 0) {
				count_non_empty++;
				local_average_by_sample += samples[i].sizeInBytes() / samples[i].num_rows();
			}
		}
		avg_bytes_per_row = local_average_by_sample / count_non_empty;
	}

	// Now we just want those columns that sould be ordered
	auto tableNames = samples[0].names();
	std::vector<std::string> sortColNames(sortColIndices.size());
	std::transform(sortColIndices.begin(), sortColIndices.end(), sortColNames.begin(), [&](auto index) { return tableNames[index]; });

	std::vector<ral::frame::BlazingTableView> samples_only_with_sort_cols;
	for (int i = 0; i < samples.size(); i++){
		auto sample_view_with_sorted_columns = samples[i].view().select(sortColIndices);
		auto sample_blaz_view_with_sort_columns = std::make_unique<ral::frame::BlazingTableView>(std::move(sample_view_with_sorted_columns), sortColNames);
		samples_only_with_sort_cols.push_back(*sample_blaz_view_with_sort_columns);
    }

	cudf::size_type total_num_partitions = (double)table_num_rows * (double)avg_bytes_per_row / (double)num_bytes_per_order_by_partition;
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

	if( ral::utilities::checkIfConcatenatingStringsWillOverflow(samples_only_with_sort_cols)) {
		logger->warn("{query_id}|{step}|{substep}|{info}",
						"query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
						"step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
						"substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
						"info"_a="In generatePartitionPlans Concatenating Strings will overflow strings length");
	}
	partitionPlan = generatePartitionPlans(total_num_partitions, samples_only_with_sort_cols, sortOrderTypes);
	context->incrementQuerySubstep();

	return partitionPlan;
}

std::unique_ptr<ral::frame::BlazingTable> generate_distributed_partition_plan(const ral::frame::BlazingTableView & selfSamples,
	std::size_t table_num_rows, std::size_t avg_bytes_per_row, const std::string & query_part, Context * context){
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
	if(context->isMasterNode(CommunicationData::getInstance().getSelfNode())) {
		context->incrementQuerySubstep();
		std::pair<std::vector<NodeColumn>, std::vector<std::size_t> > samples_pair = collectSamples(context);
		std::vector<ral::frame::BlazingTableView> samples;
		for (int i = 0; i < samples_pair.first.size(); i++){
			samples.push_back(samples_pair.first[i].second->toBlazingTableView());
		}
		samples.push_back(selfSamples);
		std::vector<size_t> total_rows_tables = samples_pair.second;
		total_rows_tables.push_back(table_num_rows);
		std::size_t totalNumRows = std::accumulate(total_rows_tables.begin(), total_rows_tables.end(), std::size_t(0));

		partitionPlan = generate_partition_plan(samples, totalNumRows, avg_bytes_per_row, query_part, context);
		distributePartitionPlan(context, partitionPlan->toBlazingTableView());
	} else {
		context->incrementQuerySubstep();
		sendSamplesToMaster(context, selfSamples, table_num_rows);
		context->incrementQuerySubstep();
		partitionPlan = getPartitionPlan(context);
	}
	return partitionPlan;
}

std::vector<std::pair<int, std::unique_ptr<ral::frame::BlazingTable>>>
distribute_table_partitions(const ral::frame::BlazingTableView & partitionPlan,
													const ral::frame::BlazingTableView & sortedTable,
													const std::string & query_part,
													blazingdb::manager::Context * context) {
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	cudf::size_type limitRows;
	std::tie(sortColIndices, sortOrderTypes, limitRows) = get_sort_vars(query_part);

	std::vector<NodeColumnView> partitions = partitionData(context, sortedTable, partitionPlan, sortColIndices, sortOrderTypes);

	int num_nodes = context->getTotalNodes();
	int num_partitions = partitions.size();
	std::vector<int32_t> part_ids(num_partitions);
    int32_t count = 0;
	std::generate(part_ids.begin(), part_ids.end(), [count, num_partitions_per_node=num_partitions/num_nodes] () mutable { return (count++)%(num_partitions_per_node); });

	distributeTablePartitions(context, partitions, part_ids);

	std::vector<std::pair<int, std::unique_ptr<ral::frame::BlazingTable>>> self_partitions;
	for (size_t i = 0; i < partitions.size(); i++) {
		auto & partition = partitions[i];
		if(partition.first == CommunicationData::getInstance().getSelfNode()) {
			std::unique_ptr<ral::frame::BlazingTable> table = partition.second.clone();
			self_partitions.emplace_back(part_ids[i], std::move(table));
		}
	}
	return self_partitions;
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
