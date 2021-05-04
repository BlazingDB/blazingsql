#pragma once

#include "BatchProcessing.h"
#include "operators/OrderBy.h"
#include "operators/GroupBy.h"
#include "execution_kernels/distributing_kernel.h"

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
		std::size_t pos);

	std::string kernel_name() { return "ComputeWindow";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:
	// LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $1 ORDER BY $3 DESC)], lag_col=[LAG($0, 5) OVER (PARTITION BY $1)], n_name=[$2])
	std::vector<int> column_indices_partitioned;   // column indices to be partitioned: [1]
	std::vector<int> column_indices_ordered;   	   // column indices to be ordered: [3]
	std::vector<int> column_indices_to_agg;        // column indices to be agg: [0, 0]
	std::vector<int> agg_param_values;     		   // due to LAG or LEAD: [0, 5]
	int preceding_value;     	                   // X PRECEDING
	int following_value;     		               // Y FOLLOWING
	std::string frame_type;                        // ROWS or RANGE
	std::vector<std::string> type_aggs_as_str;     // ["MIN", "LAG"]
	std::vector<AggregateKind> aggs_wind_func;     // [AggregateKind::MIN, AggregateKind::LAG]
	bool remove_overlap; 						   // If we need to remove the overlaps after computing the windows
};


const std::string TASK_ARG_REMOVE_PRECEDING_OVERLAP="remove_preceding_overlap";
const std::string TASK_ARG_REMOVE_FOLLOWING_OVERLAP="remove_following_overlap";
const std::string TRUE = "true";
const std::string FALSE = "false";

const std::string UNKNOWN_OVERLAP_STATUS="UNKNOWN";
const std::string INCOMPLETE_OVERLAP_STATUS="INCOMPLETE";
const std::string DONE_OVERLAP_STATUS="DONE";

const std::string TASK_ARG_OP_TYPE="operation_type";
const std::string TASK_ARG_OVERLAP_TYPE="overlap_type";
const std::string TASK_ARG_OVERLAP_SIZE="overlap_size";
const std::string TASK_ARG_SOURCE_BATCH_INDEX="source_batch_index";
const std::string TASK_ARG_TARGET_BATCH_INDEX="target_batch_index";
const std::string TASK_ARG_TARGET_NODE_INDEX="target_node_index";

const std::string PRECEDING_OVERLAP_TYPE="preceding";
const std::string FOLLOWING_OVERLAP_TYPE="following";
const std::string BOTH_OVERLAP_TYPE="both_overlaps";
const std::string PRECEDING_REQUEST="preceding_request";
const std::string FOLLOWING_REQUEST="following_request";
const std::string PRECEDING_RESPONSE="preceding_response";
const std::string FOLLOWING_RESPONSE="following_response";


/**
* The OverlapGeneratorKernel is only used for window functions that have no partition by clause and that also have bounded window frames.
* The OverlapGeneratorKernel assumes that it will be following by OverlapAccumulatorKernel and has three output caches:
* - "batches"
* - "preceding_overlaps"
* - "following_overlaps"
*
* Its purpose is to take its input batches and create preceding and/or following overlaps depending on the preceding and
* following clauses of the window frames. It operates on each batch independently and does not communicate with other nodes.
* If it cannot create a complte overlap (the overlap size is bigger than the batch), it will set the overlap's status to INCOMPLETE, which is captured by the CacheData metadata for the overlap.
*/
class OverlapGeneratorKernel : public kernel {
public:
	OverlapGeneratorKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "OverlapGenerator";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:
	int preceding_value;     	   // X PRECEDING
	int following_value;     		   // Y FOLLOWING

	// these are the three output caches
	std::shared_ptr<ral::cache::CacheMachine> output_batches_cache;
	std::shared_ptr<ral::cache::CacheMachine> output_preceding_overlap_cache;
	std::shared_ptr<ral::cache::CacheMachine> output_following_overlap_cache;

	int self_node_index;
	int total_nodes;
};


/**
* The OverlapAccumulatorKernel assumes three input caches:
* - "batches"
* - "preceding_overlaps"
* - "following_overlaps"
* It assumes the previous kernel will fill "batches" N cacheData that is sorted and the batches are in order
* It assumes that preceding_overlaps and following_overlaps will contain N-1 cacheData that corresponds to the preceding and following overlaps copied from the batches
* The idea is that part of batches[x] will be copied to fill preceding_overlaps[x+1] and part will be copied to fill following_overlaps[x-1]
* The preceding_overlaps and following_overlaps will contain metadata to indicate if the overlaps are complete or incomplete (DONE_OVERLAP_STATUS or INCOMPLETE_OVERLAP_STATUS)
* For example, preceding_overlaps[x+1] would be set to INCOMPLETE_OVERLAP_STATUS if batches[x] was not big enough to fulfill the required preceding_value which is defined by the window frame (i.e. ROWS BETWEEN X PRECEDING AND Y FOLLOWING)
* 
* The purpose of OverlapAccumulatorKernel is to ensure that all INCOMPLETE_OVERLAP_STATUS overlaps coming from the previous kernel are COMPLETED by copying from other batches.
* The other purpose is to fill the overlaps of preceding_overlaps[0] and following_overlaps[N] with data that has to come from the neighboring nodes (or make a blank overlap if there is no neighbor)
* The OverlapAccumulatorKernel will comunicate with other nodes by sending overlap_requests (PRECEDING_REQUEST, FOLLOWING_REQUEST) which when received and responded to as PRECEDING_RESPONSE and FOLLOWING_RESPONSE
* 
* Right before outputting, OverlapAccumulatorKernel will combine preceding_overlaps[x], batches[x] and following_overlaps[x] together to make one batch pushed to the output.
* The following kernel, will then have in one batch with number of rows (preceding_overlaps[x]->num_rows() + batches[x]->num_rows() + following_overlaps[x]->num_rows()), which is the data necessary to procude a batches[x]->num_rows() worth out final output rows.
* 
* This kernel uses ConcatenatingCacheDatas a lot to try to reduce and postpone the materialization of data.
*/

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

private:
	void set_overlap_status(bool preceding, int index, std::string status);
	std::string get_overlap_status(bool preceding, int index);
	void combine_overlaps(bool preceding, int target_batch_index, std::unique_ptr<ral::frame::BlazingTable> new_overlap, std::string overlap_status);
	void combine_overlaps(bool preceding, int target_batch_index, std::unique_ptr<ral::cache::CacheData> new_overlap_cache_data, std::string overlap_status);

	void response_receiver();
	void preceding_request_receiver();
	void following_request_receiver();
	void message_receiver(std::vector<std::string> expected_message_ids, int messages_expected);

	void prepare_overlap_task(bool preceding, int source_batch_index, int target_node_index, int target_batch_index, size_t overlap_size);
	void send_request(bool preceding, int source_node_index, int target_node_index, int target_batch_index, size_t overlap_size);

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

