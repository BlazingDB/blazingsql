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

    CodeTimer eventTimer(false);

    auto & input = inputs[0];
    auto sortedTable = ral::operators::sort(input->toBlazingTableView(), this->expression);

    if (sortedTable) {
        auto num_rows = sortedTable->num_rows();
        auto num_bytes = sortedTable->sizeInBytes();

        events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                        "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                        "query_id"_a=context->getContextToken(),
                        "kernel_id"_a=this->get_id(),
                        "input_num_rows"_a=num_rows,
                        "input_num_bytes"_a=num_bytes,
                        "output_num_rows"_a=num_rows,
                        "output_num_bytes"_a=num_bytes,
                        "event_type"_a="compute",
                        "timestamp_begin"_a=eventTimer.start_time(),
                        "timestamp_end"_a=eventTimer.end_time());
    }

    output->addToCache(std::move(sortedTable));
}

kstatus SortKernel::run() {

    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    while (cache_data != nullptr) {
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty();
    });
    lock.unlock();

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Sort Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

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
