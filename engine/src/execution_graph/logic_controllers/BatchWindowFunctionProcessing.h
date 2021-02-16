#pragma once

#include "BatchProcessing.h"
#include "operators/OrderBy.h"
#include "operators/GroupBy.h"

namespace ral {
namespace batch {

//using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

/**
 * @brief This kernel computes the main Window Function (ROW_NUMBER, LAG, LEAD, MIN, ...) to each batch already pattitioned
 * A new column should be added to the batch
 */

class ComputeWindowKernel : public kernel {
public:
	ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::unique_ptr<CudfColumn> compute_column_from_window_function(cudf::table_view input_cudf_view, cudf::column_view input_col_view, std::size_t pos);

	std::string kernel_name() { return "ComputeWindow";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:
	std::vector<int> column_indices_partitioned;   // column indices to be partitioned
	std::vector<int> column_indices_wind_func;     // column indices to be agg
	std::vector<AggregateKind> aggs_wind_func; 
};


const UNKNOWN_OVERLAP_STATUS="UNKNOWN";
const REQUESTED_OVERLAP_STATUS="REQUESTED";
const INCOMPLETE_OVERLAP_STATUS="INCOMPLETE";
const PROCESSING_OVERLAP_STATUS="PROCESSING"; // WSM TODO, do we need this?
const DONE_OVERLAP_STATUS="DONE";

class OverlapAccumulatorKernel : public kernel {
public:
	OverlapAccumulatorKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "OverlapAccumulator";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

	void set_overlap_status(bool presceding, int index, std::string status);
	std::string get_overlap_status(bool presceding, int index);

private:
	void update_num_batches();

	size_t num_batches;
	bool have_all_batches = false;
	std::vector<std::string> presceding_overlap_statuses;
	std::vector<size_t> presceding_overlap_counters;
	size_t presceding_overlap_amount;
	std::vector<std::string> following_overlap_status;
	std::vector<size_t> following_overlap_counters;
	size_t following_overlap_amount;

	std::shared_ptr<ral::cache::CacheMachine> batches_cache;
	std::shared_ptr<ral::cache::CacheMachine> presceding_overlap_cache;
	std::shared_ptr<ral::cache::CacheMachine> following_overlap_cache;
	
	int self_node_index;

	int node_completions_received = 0;
	int node_completions_required = 0;	
};


} // namespace batch
} // namespace ral
