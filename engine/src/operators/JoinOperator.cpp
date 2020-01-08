#include "operators/JoinOperator.h"
#include "CalciteInterpreter.h"
#include "CodeTimer.h"
#include "ColumnManipulation.cuh"
#include "JoinProcessor.h"
#include "communication/CommunicationData.h"
#include "config/GPUManager.cuh"
#include "cuDF/safe_nvcategory_gather.hpp"
#include "distribution/NodeColumns.h"
#include "distribution/primitives.h"
#include "exception/RalException.h"
#include "utilities/CommonOperations.h"
#include "utilities/RalColumn.h"
#include "utilities/StringUtils.h"
#include "utilities/TableWrapper.h"
#include <algorithm>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <future>
#include <numeric>

namespace {
using blazingdb::manager::Context;
using blazingdb::transport::Node;
using ral::distribution::NodeColumns;
}  // namespace

const std::string INNER_JOIN = "inner";

namespace ral {
namespace operators {

class JoinOperator {
public:
	JoinOperator(Context * context);

public:
	virtual blazing_frame operator()(blazing_frame & input, const std::string & query_part) = 0;

protected:
	void evaluate_join(blazing_frame & input, const std::string & query_part);

	void materialize_column(blazing_frame & input, bool is_inner_join);

protected:
	Context * context_;
	CodeTimer timer_;

protected:
	gdf_column_cpp left_indices_;
	gdf_column_cpp right_indices_;
};


class LocalJoinOperator : public JoinOperator {
public:
	LocalJoinOperator(Context * context);

public:
	blazing_frame operator()(blazing_frame & input, const std::string & query_part) override;
};


class DistributedJoinOperator : public JoinOperator {
public:
	DistributedJoinOperator(Context * context);

public:
	blazing_frame operator()(blazing_frame & input, const std::string & query_part) override;

protected:
	blazing_frame process_distribution(blazing_frame & frame, const std::string & query);

	blazing_frame process_hash_based_distribution(blazing_frame & frame, const std::string & query);

	std::vector<gdf_column_cpp> process_distribution_table(
		std::vector<gdf_column_cpp> & table, std::vector<int> & columnIndices);

	std::vector<gdf_column_cpp> concat_columns(
		std::vector<gdf_column_cpp> & local_table, std::vector<NodeColumns> & remote_partition);
};

}  // namespace operators
}  // namespace ral


namespace ral {
namespace operators {

JoinOperator::JoinOperator(Context * context) : context_{context} {
	left_indices_ = ral::utilities::create_column(0, ral::traits::dtype<gdf_index_type>);
	right_indices_ = ral::utilities::create_column(0, ral::traits::dtype<gdf_index_type>);
}

// TODO: On error clean up everything here so we dont run out of memory
void JoinOperator::evaluate_join(blazing_frame & input, const std::string & query) {
	std::string condition = get_named_expression(query, "condition");
	std::string join_type = get_named_expression(query, "joinType");

	for(auto & table : input.get_columns()) {
		cudf::table temp_table = ral::utilities::create_table(table);
		gather_and_remap_nvcategory(temp_table);
	}

	::evaluate_join(condition, join_type, input, left_indices_.get_gdf_column(), right_indices_.get_gdf_column());
}


void JoinOperator::materialize_column(blazing_frame & input, bool is_inner_join) {
	std::vector<gdf_column_cpp> new_columns(input.get_size_columns());
	size_t first_table_end_index = input.get_size_column();
	int column_width;
	for(int column_index = 0; column_index < input.get_size_columns(); column_index++) {
		gdf_column_cpp output;

		column_width = ral::traits::get_dtype_size_in_bytes(input.get_column(column_index).get_gdf_column());

		if(is_inner_join) {
			if(input.get_column(column_index).valid())
				output.create_gdf_column(input.get_column(column_index).dtype(),
					input.get_column(column_index).dtype_info(),
					left_indices_.size(),
					nullptr,
					column_width,
					input.get_column(column_index).name());
			else
				output.create_gdf_column(input.get_column(column_index).dtype(),
					input.get_column(column_index).dtype_info(),
					left_indices_.size(),
					nullptr,
					nullptr,
					column_width,
					input.get_column(column_index).name());
		} else {
			if(!input.get_column(column_index).valid())
				input.get_column(column_index).allocate_set_valid();

			output.create_gdf_column(input.get_column(column_index).dtype(),
				input.get_column(column_index).dtype_info(),
				left_indices_.size(),
				nullptr,
				column_width,
				input.get_column(column_index).name());
		}

		if(left_indices_.size() != 0 && right_indices_.size() != 0) {  // Do not materialize if the join output is empty
			::materialize_column(input.get_column(column_index).get_gdf_column(),
				output.get_gdf_column(),
				(column_index < first_table_end_index ? left_indices_.get_gdf_column()
													  : right_indices_.get_gdf_column()));
		} else {
			init_string_category_if_null(output.get_gdf_column());
		}

		// TODO: On error clean up all the resources
		// free_gdf_column(input.get_column(column_index));
		output.update_null_count();

		new_columns[column_index] = output;
	}
	input.clear();
	input.add_table(new_columns);
}


LocalJoinOperator::LocalJoinOperator(Context * context) : JoinOperator(context) {}

blazing_frame LocalJoinOperator::operator()(blazing_frame & input, const std::string & query) {
	// Evaluate join
	evaluate_join(input, query);
	Library::Logging::Logger().logInfo(timer_.logDuration(*context_, "LocalJoinOperator part 1 evaluate_join"));
	timer_.reset();

	// Materialize columns
	bool is_inner_join = get_named_expression(query, "joinType") == INNER_JOIN;
	materialize_column(input, is_inner_join);
	Library::Logging::Logger().logInfo(timer_.logDuration(*context_, "LocalJoinOperator part 2 materialize_column"));
	timer_.reset();

	return input;
}


DistributedJoinOperator::DistributedJoinOperator(Context * context) : JoinOperator(context) {}

blazing_frame DistributedJoinOperator::operator()(blazing_frame & frame, const std::string & query) {
	// Execute distribution
	std::string join_type = get_named_expression(query, "joinType");
	if(join_type == INNER_JOIN) {
		frame = process_distribution(frame, query);
	} else {
		frame = process_hash_based_distribution(frame, query);
	}
	Library::Logging::Logger().logInfo(
		timer_.logDuration(*context_, "DistributedJoinOperator part 1 process_distribution"));
	timer_.reset();

	// Evaluate join
	evaluate_join(frame, query);
	Library::Logging::Logger().logInfo(timer_.logDuration(*context_, "DistributedJoinOperator part 2 evaluate_join"));
	timer_.reset();

	// Materialize columns
	bool is_inner_join = get_named_expression(query, "joinType") == INNER_JOIN;
	materialize_column(frame, is_inner_join);
	Library::Logging::Logger().logInfo(
		timer_.logDuration(*context_, "DistributedJoinOperator part 3 materialize_column"));
	timer_.reset();

	// Done
	return frame;
}

std::vector<gdf_column_cpp> DistributedJoinOperator::process_distribution_table(
	std::vector<gdf_column_cpp> & table, std::vector<int> & columnIndices) {
	cudf::table temp_table = ral::utilities::create_table(table);
	gather_and_remap_nvcategory(temp_table);

	std::vector<NodeColumns> partitions = ral::distribution::generateJoinPartitions(*context_, table, columnIndices);

	context_->incrementQuerySubstep();
	distributePartitions(*context_, partitions);
	std::vector<NodeColumns> remote_node_columns = ral::distribution::collectPartitions(*context_);

	auto it = std::find_if(partitions.begin(), partitions.end(), [](const auto & e) {
		return e.getNode() == ral::communication::CommunicationData::getInstance().getSelfNode();
	});
	assert(it != partitions.end());
	std::vector<gdf_column_cpp> local_table = it->getColumns();

	return concat_columns(local_table, remote_node_columns);
}

blazing_frame DistributedJoinOperator::process_distribution(blazing_frame & frame, const std::string & query) {
	// First lets find out if we are joining against a small table. If so, we will want to replicate that small table
	std::vector<std::vector<gdf_column_cpp>> tables = frame.get_columns();
	assert(tables.size() == 2);

	int self_node_idx = context_->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode());

	context_->incrementQuerySubstep();
	gdf_size_type local_num_rows_left = frame.get_num_rows_in_table(0);
	gdf_size_type local_num_rows_right = frame.get_num_rows_in_table(1);
	ral::distribution::distributeLeftRightNumRows(*context_, local_num_rows_left, local_num_rows_right);
	std::vector<gdf_size_type> nodes_num_rows_left;
	std::vector<gdf_size_type> nodes_num_rows_right;
	ral::distribution::collectLeftRightNumRows(*context_, nodes_num_rows_left, nodes_num_rows_right);
	nodes_num_rows_left[self_node_idx] = local_num_rows_left;
	nodes_num_rows_right[self_node_idx] = local_num_rows_right;

	gdf_size_type total_rows_left = std::accumulate(nodes_num_rows_left.begin(), nodes_num_rows_left.end(), 0);
	gdf_size_type total_rows_right = std::accumulate(nodes_num_rows_right.begin(), nodes_num_rows_right.end(), 0);

	size_t row_width_bytes_left = 0;
	size_t row_width_bytes_right = 0;
	for(auto & column : tables[0]) {
		if(column.dtype() == GDF_STRING_CATEGORY)
			row_width_bytes_left += ral::traits::get_dtype_size_in_bytes(column.dtype()) *
									5;  // WSM TODO. Here i am adding a fudge factor trying to account for the fact that
										// strings can occupy a lot more. We needa better way to easily figure out how
										// mmuch space a string column actually occupies
		else
			row_width_bytes_left += ral::traits::get_dtype_size_in_bytes(column.dtype());
	}
	for(auto & column : tables[1]) {
		if(column.dtype() == GDF_STRING_CATEGORY)
			row_width_bytes_right += ral::traits::get_dtype_size_in_bytes(column.dtype()) *
									 5;  // WSM TODO. Here i am adding a fudge factor trying to account for the fact
										 // that strings can occupy a lot more. We needa better way to easily figure out
										 // how mmuch space a string column actually occupies
		else
			row_width_bytes_right += ral::traits::get_dtype_size_in_bytes(column.dtype());
	}
	int num_nodes = context_->getTotalNodes();

	bool scatter_left = false;
	bool scatter_right = false;
	size_t estimate_regular_distribution =
		(total_rows_left * row_width_bytes_left + total_rows_right * row_width_bytes_right) * (num_nodes - 1) /
		num_nodes;
	size_t estimate_scatter_left = (total_rows_left * row_width_bytes_left) * (num_nodes - 1);
	size_t estimate_scatter_right = (total_rows_right * row_width_bytes_right) * (num_nodes - 1);
	size_t MAX_SCATTER_MEM_OVERHEAD = 500000000;  // 500Mb  how much extra memory consumption per node are we ok with
												  // WSM TODO get this value from config
	if(estimate_scatter_left < estimate_regular_distribution ||
		estimate_scatter_right < estimate_regular_distribution) {
		if(estimate_scatter_left < estimate_scatter_right &&
			total_rows_left * row_width_bytes_left < MAX_SCATTER_MEM_OVERHEAD) {
			scatter_left = true;
		} else if(estimate_scatter_right < estimate_scatter_left &&
				  total_rows_right * row_width_bytes_right < MAX_SCATTER_MEM_OVERHEAD) {
			scatter_right = true;
		}
	}

	if(scatter_left || scatter_right) {
		context_->incrementQuerySubstep();
		int num_to_collect = 0;
		std::vector<gdf_column_cpp> data_to_scatter;
		if(scatter_left) {
			Library::Logging::Logger().logTrace(
				ral::utilities::buildLogString(std::to_string(context_->getContextToken()),
					std::to_string(context_->getQueryStep()),
					std::to_string(context_->getQuerySubstep()),
					"join process_distribution scatter_left"));
			data_to_scatter = frame.get_table(0);
			if(local_num_rows_left > 0) {
				ral::distribution::scatterData(*context_, data_to_scatter);
			}
			for(size_t i = 0; i < nodes_num_rows_left.size(); i++) {
				if(i != self_node_idx && nodes_num_rows_left[i] > 0) {
					num_to_collect++;
				}
			}
		} else {
			Library::Logging::Logger().logTrace(
				ral::utilities::buildLogString(std::to_string(context_->getContextToken()),
					std::to_string(context_->getQueryStep()),
					std::to_string(context_->getQuerySubstep()),
					"join process_distribution scatter_right"));
			data_to_scatter = frame.get_table(1);
			if(local_num_rows_right > 0) {  // this node has data on the right to scatter
				ral::distribution::scatterData(*context_, data_to_scatter);
			}
			for(size_t i = 0; i < nodes_num_rows_right.size(); i++) {
				if(i != self_node_idx && nodes_num_rows_right[i] > 0) {
					num_to_collect++;
				}
			}
		}

		blazing_frame join_frame;
		std::vector<gdf_column_cpp> cluster_shared_table;
		if(num_to_collect > 0) {
			std::vector<NodeColumns> collected_partitions =
				ral::distribution::collectSomePartitions(*context_, num_to_collect);
			cluster_shared_table = concat_columns(data_to_scatter, collected_partitions);
		} else {
			cluster_shared_table = data_to_scatter;
		}

		if(scatter_left) {
			join_frame.add_table(cluster_shared_table);
			join_frame.add_table(tables[1]);
		} else {
			join_frame.add_table(tables[0]);
			join_frame.add_table(cluster_shared_table);
		}
		return join_frame;

	} else {
		Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context_->getContextToken()),
			std::to_string(context_->getQueryStep()),
			std::to_string(context_->getQuerySubstep()),
			"join process_distribution hash based distribution"));
		return process_hash_based_distribution(frame, query);
	}
}

blazing_frame DistributedJoinOperator::process_hash_based_distribution(
	blazing_frame & frame, const std::string & query) {
	std::vector<int> globalColumnIndices;
	parseJoinConditionToColumnIndices(get_named_expression(query, "condition"), globalColumnIndices);

	int processedColumns = 0;
	blazing_frame join_frame;
	for(auto & table : frame.get_columns()) {
		// Get col indices relative to a table, similar to blazing_frame::get_column
		std::vector<int> localIndices;
		std::for_each(globalColumnIndices.begin(), globalColumnIndices.end(), [&](int i) {
			if(i >= processedColumns && i < processedColumns + table.size()) {
				localIndices.push_back(i - processedColumns);
			}
		});
		processedColumns += table.size();

		join_frame.add_table(process_distribution_table(table, localIndices));
	}

	return join_frame;
}


std::vector<gdf_column_cpp> DistributedJoinOperator::concat_columns(
	std::vector<gdf_column_cpp> & local_table, std::vector<NodeColumns> & remote_node_columns) {
	// first lets collect all the non-empty tables
	std::vector<std::vector<gdf_column_cpp>> all_tables(remote_node_columns.size() + 1);
	all_tables[0] = local_table;
	for(size_t i = 0; i < remote_node_columns.size(); i++) {
		all_tables[i + 1] = remote_node_columns[i].getColumns();
	}
	// Concatenate table
	return ral::utilities::concatTables(all_tables);
}

}  // namespace operators
}  // namespace ral


namespace ral {
namespace operators {

const std::string LOGICAL_JOIN_TEXT = "LogicalJoin";

bool is_join(const std::string & query) { return (query.find(LOGICAL_JOIN_TEXT) != std::string::npos); }

blazing_frame process_join(Context * context, blazing_frame & frame, const std::string & query) {
	std::unique_ptr<JoinOperator> join_operator;
	if(context == nullptr) {
		join_operator = std::make_unique<LocalJoinOperator>(context);
	} else if(context->getTotalNodes() <= 1) {
		join_operator = std::make_unique<LocalJoinOperator>(context);
	} else {
		join_operator = std::make_unique<DistributedJoinOperator>(context);
	}
	return (*join_operator)(frame, query);
}

}  // namespace operators
}  // namespace ral
