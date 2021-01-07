#include "BatchWindowFunctionProcessing.h"
#include "CodeTimer.h"
#include <src/utilities/CommonOperations.h>
#include "taskflow/executor.h"

namespace ral {
namespace batch {

// BEGIN SortKernel

SortKernel::SortKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::SortKernel} {
    // TODO: rewrite the window#0=[window(partition {2} order by [1] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])]
    // just we want the ORDER BY details
    this->query_graph = query_graph;
}

void SortKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

}

kstatus SortKernel::run() {

    return kstatus::proceed;
}

// END SortKernel


// BEGIN SplitByKeysKernel

SplitByKeysKernel::SplitByKeysKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::SortKernel} {
    // TODO: rewrite the window#0=[window(partition {2} order by [1] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])]
    // just we want the PARTITION BY details
    this->query_graph = query_graph;
}

void SplitByKeysKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

}

kstatus SplitByKeysKernel::run() {

    return kstatus::proceed;
}

// END SplitByKeysKernel


// BEGIN ConcatPartitionsByKeysKernel

ConcatPartitionsByKeysKernel::ConcatPartitionsByKeysKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id,queryString, context, kernel_type::ConcatPartitionsByKeysKernel} {
    // TODO: rewrite the window#0=[window(partition {2} order by [1] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])]
    // just we want the PARTITION BY details
    this->query_graph = query_graph;
}

void ConcatPartitionsByKeysKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

}

kstatus ConcatPartitionsByKeysKernel::run() {

    return kstatus::proceed;
}

// END ConcatPartitionsByKeysKernel


// BEGIN ComputeWindowKernel

ComputeWindowKernel::ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::SortKernel} {
    // TODO: rewrite the window#0=[window(partition {2} order by [1] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])]
    // just we want the PARTITION BY details
    this->query_graph = query_graph;
}

void ComputeWindowKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

}

kstatus ComputeWindowKernel::run() {

    return kstatus::proceed;
}

// END ComputeWindowKernel


} // namespace batch
} // namespace ral
