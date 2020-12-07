#pragma once

#include "BatchProcessing.h"
#include "communication/CommunicationData.h"
#include "operators/OrderBy.h"
#include "taskflow/distributing_kernel.h"

namespace ral {
namespace batch {
using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using namespace fmt::literals;


class PartitionSingleNodeKernel : public kernel {
public:
	PartitionSingleNodeKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, std::string kernel_process_name);

	virtual kstatus run();

private:
	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
};

class SortAndSampleKernel : public distributing_kernel {

std::size_t SAMPLES_MESSAGE_TRACKER_IDX = 0;
std::size_t PARTITION_PLAN_MESSAGE_TRACKER_IDX = 1;

public:
	SortAndSampleKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);
	void compute_partition_plan(std::vector<ral::frame::BlazingTableView> sampledTableViews, std::size_t avg_bytes_per_row, std::size_t local_total_num_rows);
	virtual kstatus run();

private:

};

class PartitionKernel : public distributing_kernel {
public:
	PartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, std::string kernel_process_name) override;

	virtual kstatus run();

private:
	std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
	std::vector<cudf::order> sortOrderTypes;
	std::vector<int> sortColIndices;
	int num_partitions_per_node;
};

/**
 * This kernel has a loop over all its different input caches.
 * It then pulls all the inputs from one cache and merges them.
 */
class MergeStreamKernel : public kernel {
public:
	MergeStreamKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);
	virtual kstatus run();
};


/**
 * @brief This kernel only returns a specified number of rows given by their corresponding logical limit expression.
 */

class LimitKernel : public distributing_kernel {
public:
	LimitKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, std::string kernel_process_name) override;

	virtual kstatus run();

private:
	std::atomic<int64_t> rows_limit;
};

} // namespace batch
} // namespace ral
