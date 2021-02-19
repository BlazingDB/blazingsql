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

	void set_overlap_status(bool presceding, int index, std::string status);
	std::string get_overlap_status(bool presceding, int index);
	void combine_overlaps(bool presceding, int target_batch_index, std::unique_ptr<ral::frame::BlazingTable> new_overlap, std::string overlap_status);
	void combine_overlaps(bool presceding, int target_batch_index, std::unique_ptr<ral::cache::CacheData> new_overlap_cache_data, std::string overlap_status);
	void request_receiver();
	void prepare_overlap_task(bool presceding, int source_batch_index, int target_node_index, int target_batch_index, size_t overlap_size);
	void send_request(bool presceding, int source_node_index, int target_node_index, int target_batch_index, size_t overlap_size);


private:
	void update_num_batches();

	size_t num_batches;
	bool have_all_batches = false;
	std::vector<std::string> presceding_overlap_statuses;
	size_t presceding_overlap_amount;
	std::vector<std::string> following_overlap_status;
	size_t following_overlap_amount;

	// these are the three input caches
	std::shared_ptr<ral::cache::CacheMachine> input_batches_cache;
	std::shared_ptr<ral::cache::CacheMachine> input_presceding_overlap_cache;
	std::shared_ptr<ral::cache::CacheMachine> input_following_overlap_cache;

	// these are the internal ones we want to work with. 
	// We need to use internal ones, because the input ones will get a status of finish applied externally, which make the array access work differently
	std::shared_ptr<ral::cache::CacheMachine> batches_cache;
	std::shared_ptr<ral::cache::CacheMachine> presceding_overlap_cache;
	std::shared_ptr<ral::cache::CacheMachine> following_overlap_cache;
	
	int self_node_index;

	std::vector<std::string> col_names;
	std::vector<cudf::data_type> schema;
};


} // namespace batch
} // namespace ral

