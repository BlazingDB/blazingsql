#include "BatchWindowFunctionProcessing.h"
#include "CodeTimer.h"
#include <src/utilities/CommonOperations.h>
#include "taskflow/executor.h"
#include "parser/expression_utils.hpp"
#include "execution_graph/logic_controllers/BlazingColumn.h"

#include <cudf/stream_compaction.hpp>
#include "cudf/column/column_view.hpp"
#include <cudf/rolling.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/types.hpp>

namespace ral {
namespace batch {

// BEGIN SortKernel

SortKernel::SortKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::SortKernel} {
    this->query_graph = query_graph;
}

void SortKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

    CodeTimer eventTimer(false);

    std::unique_ptr<ral::frame::BlazingTable> & input = inputs[0];

    auto sortedTable = ral::operators::sort_partition_by(input->toBlazingTableView(), this->expression);

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
                        "event_type"_a="SortKernel compute",
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

    if(logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Sort Kernel tasks created",
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
                    "info"_a="Sort Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END SortKernel


// BEGIN SplitByKeysKernel

SplitByKeysKernel::SplitByKeysKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::SplitByKeysKernel} {
    this->query_graph = query_graph;
}

void SplitByKeysKernel::do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

    CodeTimer eventTimer(false);

    std::unique_ptr<ral::frame::BlazingTable> & input = inputs[0];

    // TODO: for now just support one col to the PARTIITION BY clause
    this->column_indices_partitioned = get_colums_to_partition(this->expression);
    
    // we want get only diff keys from each batch
    std::unique_ptr<cudf::table> unique_keys_table = cudf::drop_duplicates(input->view(), this->column_indices_partitioned, cudf::duplicate_keep_option::KEEP_FIRST);

    // TODO: now we want to convert the col (which contains the keys) to a vector (DISTRIBUTED) (unique_keys_table[this->column_indices_partitioned[0]])
    //std::vector<cudf::type_id> column_to_vector<cudf::size_type>(unique_keys_table->get_column(this->column_indices_partitioned[0]);
    // TODO: merge all column_to_vector ??

    // Using the cudf::has_partition and cudf::split
    cudf::table_view batch_view = input->view();
    std::size_t num_partitions = unique_keys_table->num_rows();
    std::vector<cudf::table_view> partitioned_cudf_view;
    std::unique_ptr<CudfTable> hashed_data; // Keep table alive in this scope
    if (batch_view.num_rows() > 0) {
        std::vector<cudf::size_type> hased_data_offsets;
        // NOTE: important! USe HASH_IDENTITY
        std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(input->view(), this->column_indices_partitioned, num_partitions, cudf::hash_id::HASH_IDENTITY);
        // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
        std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
        partitioned_cudf_view = cudf::split(hashed_data->view(), split_indexes);
    } else {
        //  copy empty view
        for (auto i = 0; i < num_partitions; i++) {
            partitioned_cudf_view.push_back(batch_view);
        }
    }

    for (std::size_t i = 0; i < partitioned_cudf_view.size(); i++) {
        std::string cache_id = "output_" + std::to_string(i);
        this->add_to_output_cache(
            std::make_unique<ral::frame::BlazingTable>(std::make_unique<cudf::table>(partitioned_cudf_view[i]), input->names()),
            cache_id
            );
    }
}

kstatus SplitByKeysKernel::run() {

    CodeTimer timer;

    std::unique_ptr<ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();

    while (cache_data != nullptr ){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                nullptr,
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

    /*
    for (auto i = 0; i < 5; i++) {
        int total_count = 1;
        std::string cache_id = "output_" + std::to_string(i);
        this->output_cache(cache_id)->wait_for_count(total_count);
    }*/

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


// BEGIN ComputeWindowKernel

ComputeWindowKernel::ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::ComputeWindowKernel} {
    this->query_graph = query_graph;
}

void ComputeWindowKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

    CodeTimer eventTimer(false);

    if (inputs.empty()) {
        // no op
    } else if(inputs.size() == 1) {
        output->addToCache(std::move(inputs[0])); // ERROR
    } else {

        for (std::size_t i = 0; i < inputs.size(); i++){
            std::string cache_id = "output_" + std::to_string(i);
            this->add_to_output_cache(std::move(inputs[i]),
            cache_id
            );
        }

    }

/*
    std::unique_ptr<ral::frame::BlazingTable> & input = inputs[0];

    std::vector<std::string> naames = input->names();
    
    // TODO: momentaneally we should add a new colum, due to the window function
    // TODO: for now just one window function is supported
    cudf::table_view input_cudf_view = input->view();

    cudf::column_view input_col_view = input_cudf_view.column(0); //TODO: window#0=[window(partition {2} aggs [MIN($0)])]  $0
    // TODO: how select the 6 ? maybe using input_cudf_view.size() when there is no between statement
    std::unique_ptr<CudfColumn> windowed_col = cudf::rolling_window(input_col_view, 5, 0, 1, cudf::make_min_aggregation());

    // TODO: add this windowed_col to 
    std::unique_ptr<CudfTable> cudf_input = input->releaseCudfTable();
    std::vector< std::unique_ptr<CudfColumn> > output_columns = cudf_input->release();
    output_columns.push_back(std::move(windowed_col));

    auto expected = std::make_unique<CudfTable>(std::move(output_columns));

    naames.push_back("min_keys");

    std::unique_ptr<ral::frame::BlazingTable> windowed_table = std::make_unique<ral::frame::BlazingTable>(std::move(expected), naames);

    if (windowed_table) {
        auto num_rows = windowed_table->num_rows();
        auto num_bytes = windowed_table->sizeInBytes();

        events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                        "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                        "query_id"_a=context->getContextToken(),
                        "kernel_id"_a=this->get_id(),
                        "input_num_rows"_a=num_rows,
                        "input_num_bytes"_a=num_bytes,
                        "output_num_rows"_a=num_rows,
                        "output_num_bytes"_a=num_bytes,
                        "event_type"_a="ComputeWindowKernel compute",
                        "timestamp_begin"_a=eventTimer.start_time(),
                        "timestamp_end"_a=eventTimer.end_time());
    }

    output->addToCache(std::move(windowed_table));
    */
}

kstatus ComputeWindowKernel::run() {
    CodeTimer timer;

    // TODO: update this code
    int batch_count = 0;
    for (std::size_t idx = 0; idx < this->input_.count(); idx++)
    {
        try {
            auto cache_id = "input_" + std::to_string(idx);
            // This Kernel needs all of the input before it can do any output. So lets wait until all the input is available
            this->input_.get_cache(cache_id)->wait_until_finished();
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
            while(this->input_.get_cache(cache_id)->wait_for_next()){
                std::unique_ptr <ral::cache::CacheData> cache_data = this->input_.get_cache(cache_id)->pullCacheData();
                if(cache_data != nullptr) {
                    inputs.push_back(std::move(cache_data));
                }
            }

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    nullptr,
                    this);
            batch_count++;
        } catch(const std::exception& e) {
            // TODO add retry here
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="In ComputeWindow Kernel batch {} for {}. What: {} . max_memory_used: {}"_format(batch_count, expression, e.what(), blazing_device_memory_resource::getInstance().get_full_memory_summary()),
                                "duration"_a="");
            throw;
        }
    }

    /*
    // TODO: for now just using the same as SortKernel
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

    if(logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="ComputeWindow Kernel tasks created",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }*/

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty();
    });
    
    /*
    for (auto i = 0; i < 5; i++) {
        int total_count = 1;
        std::string cache_id = "output_" + std::to_string(i);
        this->output_cache(cache_id)->wait_for_count(total_count);
    }*/

    if(logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="ComputeWindow Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END ComputeWindowKernel


// BEGIN ConcatPartitionsByKeysKernel

ConcatPartitionsByKeysKernel::ConcatPartitionsByKeysKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id,queryString, context, kernel_type::ConcatPartitionsByKeysKernel} {
    this->query_graph = query_graph;
}

void ConcatPartitionsByKeysKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    if (inputs.empty()) {
        // no op
    } else if(inputs.size() == 1) {
        output->addToCache(std::move(inputs[0]));
    } else {
        std::vector< ral::frame::BlazingTableView > tableViewsToConcat;
        for (std::size_t i = 0; i < inputs.size(); i++){
            tableViewsToConcat.emplace_back(inputs[i]->toBlazingTableView());
        }
        auto output_merge = ral::operators::merge(tableViewsToConcat, this->expression); // TODO: Merge or concatenates ???
        output->addToCache(std::move(output_merge));
    }
}

kstatus ConcatPartitionsByKeysKernel::run() {
    CodeTimer timer;

    // TODO: update this code
    int batch_count = 0;
    for (std::size_t idx = 0; idx < this->input_.count(); idx++)
    {
        try {
            auto cache_id = "input_" + std::to_string(idx);
            // This Kernel needs all of the input before it can do any output. So lets wait until all the input is available
            this->input_.get_cache(cache_id)->wait_until_finished();
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;

            while(this->input_.get_cache(cache_id)->wait_for_next()){
                std::unique_ptr <ral::cache::CacheData> cache_data = this->input_.get_cache(cache_id)->pullCacheData();
                if(cache_data != nullptr) {
                    inputs.push_back(std::move(cache_data));
                }
            }

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this);

            batch_count++;
        } catch(const std::exception& e) {
            // TODO add retry here
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="In ConcatPartitionsByKeys Kernel kernel batch {} for {}. What: {} . max_memory_used: {}"_format(batch_count, expression, e.what(), 
                                        blazing_device_memory_resource::getInstance().get_full_memory_summary()),
                                "duration"_a="");
            throw;
        }
    }

    if(logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="ConcatPartitionsByKey Kernel tasks created",
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
                    "info"_a="ConcatPartitionsByKey Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END ConcatPartitionsByKeysKernel

} // namespace batch
} // namespace ral
