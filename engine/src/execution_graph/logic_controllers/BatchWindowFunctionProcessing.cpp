#include "BatchWindowFunctionProcessing.h"
#include "CodeTimer.h"
#include <src/utilities/CommonOperations.h>
#include "taskflow/executor.h"
#include "parser/expression_utils.hpp"
#include <cudf/stream_compaction.hpp>

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

void SplitByKeysKernel::do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

    // TODO: get unique keys from each batch
    std::unique_ptr<ral::frame::BlazingTable> & input = inputs[0];

    // TODO: for now just support one col to the PARTIITION BY clause
    this->column_indices = get_colums_to_partition(this->expression);
    std::unique_ptr<cudf::table> unique_keys_table = cudf::drop_duplicates(input->view(), this->column_indices, cudf::duplicate_keep_option::KEEP_FIRST);

    // TODO: now we want to convert the col (which contains the keys) to a vector
    //std::vector<cudf::type_id> column_to_vector<cudf::size_type>(unique_keys_table->get_column(this->column_indices[0]);


    // TODO: merge all column_to_vector 

    // TODO: Implements the cudf::has_partition and cudf::split
    /*
    CudfTableView batch_view = input->view();
    std::vector<CudfTableView> partitioned;
    std::unique_ptr<CudfTable> hashed_data; // Keep table alive in this scope
    if (batch_view.num_rows() > 0) {
        std::vector<cudf::size_type> hased_data_offsets;
        std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(input->view(), columns_to_hash, num_partitions);
        // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
        std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
        partitioned = cudf::split(hashed_data->view(), split_indexes);
    } else {
        //  copy empty view
        for (auto i = 0; i < num_partitions; i++) {
            partitioned.push_back(batch_view);
        }
    }

    std::vector<ral::frame::BlazingTableView> partitions;
    for(auto partition : partitioned) {
        partitions.push_back(ral::frame::BlazingTableView(partition, input->names()));
    }
    */
}

kstatus SplitByKeysKernel::run() {

    CodeTimer timer;

    std::unique_ptr<ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();

    while(cache_data != nullptr ){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    if(logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="SplitByKeys Kernel tasks created",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty();
    });

    if(logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="SplitByKeys Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }
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
