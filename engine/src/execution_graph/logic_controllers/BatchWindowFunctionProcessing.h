#pragma once

#include "BatchProcessing.h"
#include "operators/OrderBy.h"
#include "operators/GroupBy.h"
#include "taskflow/distributing_kernel.h"

namespace ral {
namespace batch {

using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

/**
 * @brief This kernel computes the main Window Function (ROW_NUMBER, LAG, LEAD, MIN, ...)
 * to each batch already pattitioned and sorted
 * New columns will be added to each batch
 */

class ComputeWindowKernel : public kernel {
public:
	ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::unique_ptr<CudfColumn> compute_column_from_window_function(
		cudf::table_view input_cudf_view,
		cudf::column_view input_col_view,
		std::size_t pos, int & agg_param_count);

	std::string kernel_name() { return "ComputeWindow";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:
	// LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $1)], lag_col=[LAG($0, 5) OVER (PARTITION BY $1)], n_name=[$2])
	std::vector<int> column_indices_partitioned;   // column indices to be partitioned: [1]
	std::vector<int> column_indices_to_agg;        // column indices to be agg: [0, 0]
	std::vector<int> agg_param_values;     		   // due to LAG or LEAD: [5]
	int preceding_value;     	   // X PRECEDING
	int following_value;     		   // Y FOLLOWING
	std::string frame_type;                        // ROWS or RANGE
	std::vector<std::string> type_aggs_as_str;     // ["MIN", "LAG"]
	std::vector<AggregateKind> aggs_wind_func;     // [AggregateKind::MIN, AggregateKind::LAG]
};




const std::string UNKNOWN_OVERLAP_STATUS="UNKNOWN";
const std::string REQUESTED_OVERLAP_STATUS="REQUESTED";
const std::string INCOMPLETE_OVERLAP_STATUS="INCOMPLETE";
const std::string PROCESSING_OVERLAP_STATUS="PROCESSING"; // WSM TODO, do we need this?
const std::string DONE_OVERLAP_STATUS="DONE";

const std::string TASK_ARG_OP_TYPE="operation_type";
const std::string TASK_ARG_OVERLAP_TYPE="overlap_type";
const std::string TASK_ARG_OVERLAP_SIZE="overlap_size";
const std::string TASK_ARG_SOURCE_BATCH_INDEX="source_batch_index";
const std::string TASK_ARG_TARGET_BATCH_INDEX="target_batch_index";
const std::string TASK_ARG_TARGET_NODE_INDEX="target_node_index";

const std::string OVERLAP_TASK_TYPE="get_overlap";
const std::string PRECEDING_OVERLAP_TYPE="preceding";
const std::string FOLLOWING_OVERLAP_TYPE="following";
const std::string PRECEDING_REQUEST="preceding_request";
const std::string FOLLOWING_REQUEST="following_request";
const std::string PRECEDING_FULFILLMENT="preceding_fulfillment";
const std::string FOLLOWING_FULFILLMENT="following_fulfillment";


class OverlapAccumulatorKernel : public distributing_kernel {
public:
	OverlapAccumulatorKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "OverlapAccumulator";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

	void set_overlap_status(bool preceding, int index, std::string status);
	std::string get_overlap_status(bool preceding, int index);
	void combine_overlaps(bool preceding, int target_batch_index, std::unique_ptr<ral::frame::BlazingTable> new_overlap, std::string overlap_status);
	void combine_overlaps(bool preceding, int target_batch_index, std::unique_ptr<ral::cache::CacheData> new_overlap_cache_data, std::string overlap_status);
	void request_receiver();
	void prepare_overlap_task(bool preceding, int source_batch_index, int target_node_index, int target_batch_index, size_t overlap_size);
	void send_request(bool preceding, int source_node_index, int target_node_index, int target_batch_index, size_t overlap_size);


private:
	void update_num_batches();

	size_t num_batches;
	int preceding_value;     	   // X PRECEDING
	int following_value;     		   // Y FOLLOWING
	std::vector<std::string> preceding_overlap_statuses;
	std::vector<std::string> following_overlap_status;
	
	// these are the three input caches
	std::shared_ptr<ral::cache::CacheMachine> input_batches_cache;
	std::shared_ptr<ral::cache::CacheMachine> input_preceding_overlap_cache;
	std::shared_ptr<ral::cache::CacheMachine> input_following_overlap_cache;

	// these are the internal ones we want to work with. 
	// We need to use internal ones, because the input ones will get a status of finish applied externally, which make the array access work differently
	std::shared_ptr<ral::cache::CacheMachine> batches_cache;
	std::shared_ptr<ral::cache::CacheMachine> preceding_overlap_cache;
	std::shared_ptr<ral::cache::CacheMachine> following_overlap_cache;
	
	int self_node_index;

	std::vector<std::string> col_names;
	std::vector<cudf::data_type> schema;
};


} // namespace batch
} // namespace ral

