#pragma once

#include "BatchProcessing.h"
#include "operators/OrderBy.h"

namespace ral {
namespace batch {

//using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using namespace fmt::literals;


/**
 * @brief This kernel only SORT each batch
 */

// TODO: maybe we will want to split SortAndSampleKernel into a SortKernel and the sampling step should be moved into PartitionKernel
class SortKernel : public kernel {
public:
	SortKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "Sort";}

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:

};


/**
 * @brief This kernel will split a batch into multiple batches (as N diff keys contains each batch)
 */

class SplitByKeysKernel : public kernel {
public:
	SplitByKeysKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "SplitByKeys";}

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:
	std::vector<int> column_indices;
	std::vector<cudf::type_id> keys_values;
};


/**
 * @brief This kernel cocatenates all partitions that have the same Keys
 */

class ConcatPartitionsByKeysKernel : public kernel { // TODO: public distributing_kernel
public:
	ConcatPartitionsByKeysKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "ConcatPartitionsByKeys";}

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:

};


/**
 * @brief This kernel computes the main Window Function (ROW_NUMBER, DENSE_RANK, LAG, LEAD, ...)
 */

class ComputeWindowKernel : public kernel {
public:
	ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::string kernel_name() { return "ComputeWindow";}

	void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:

};

} // namespace batch
} // namespace ral
