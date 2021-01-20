#include "BatchProcessing.h"
#include "CodeTimer.h"
#include "communication/CommunicationData.h"
#include "ExceptionHandling/BlazingThread.h"
#include "io/data_parser/CSVParser.h"
#include "parser/expression_utils.hpp"
#include "taskflow/executor.h"
#include <cudf/types.hpp>
#include <src/utilities/DebuggingUtils.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include "execution_graph/logic_controllers/LogicalProject.h"

namespace ral {
namespace batch {

// BEGIN BatchSequence

BatchSequence::BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache, const ral::cache::kernel * kernel, bool ordered)
: cache{cache}, kernel{kernel}, ordered{ordered}
{}

void BatchSequence::set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
    this->cache = cache;
}

std::unique_ptr<ral::frame::BlazingTable> BatchSequence::next() {
    std::shared_ptr<spdlog::logger> cache_events_logger;
    cache_events_logger = spdlog::get("cache_events_logger");

    CodeTimer cacheEventTimer(false);

    cacheEventTimer.start();
    std::unique_ptr<ral::frame::BlazingTable> output;
    if (ordered) {
        output = cache->pullFromCache();
    } else {
        output = cache->pullUnorderedFromCache();
    }
    cacheEventTimer.stop();

    if(output){
        auto num_rows = output->num_rows();
        auto num_bytes = output->sizeInBytes();

        if(cache_events_logger != nullptr) {
            cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                        "ral_id"_a=cache->get_context()->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                        "query_id"_a=cache->get_context()->getContextToken(),
                        "source"_a=cache->get_id(),
                        "sink"_a=kernel->get_id(),
                        "num_rows"_a=num_rows,
                        "num_bytes"_a=num_bytes,
                        "event_type"_a="removeCache",
                        "timestamp_begin"_a=cacheEventTimer.start_time(),
                        "timestamp_end"_a=cacheEventTimer.end_time());
        }
    }

    return output;
}

bool BatchSequence::wait_for_next() {
    if (kernel) {
        std::string message_id = std::to_string((int)kernel->get_type_id()) + "_" + std::to_string(kernel->get_id());
    }

    return cache->wait_for_next();
}

bool BatchSequence::has_next_now() {
    return cache->has_next_now();
}

// END BatchSequence

// BEGIN BatchSequenceBypass

BatchSequenceBypass::BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache, const ral::cache::kernel * kernel)
: cache{cache}, kernel{kernel}
{}

void BatchSequenceBypass::set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
    this->cache = cache;
}

std::unique_ptr<ral::cache::CacheData> BatchSequenceBypass::next() {
    std::shared_ptr<spdlog::logger> cache_events_logger;
    cache_events_logger = spdlog::get("cache_events_logger");

    CodeTimer cacheEventTimer(false);

    cacheEventTimer.start();
    auto output = cache->pullCacheData();
    cacheEventTimer.stop();

    if (output) {
        auto num_rows = output->num_rows();
        auto num_bytes = output->sizeInBytes();

        cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                        "ral_id"_a=cache->get_context()->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                        "query_id"_a=cache->get_context()->getContextToken(),
                        "source"_a=cache->get_id(),
                        "sink"_a=kernel->get_id(),
                        "num_rows"_a=num_rows,
                        "num_bytes"_a=num_bytes,
                        "event_type"_a="removeCache",
                        "timestamp_begin"_a=cacheEventTimer.start_time(),
                        "timestamp_end"_a=cacheEventTimer.end_time());
    }

    return output;
}

bool BatchSequenceBypass::wait_for_next() {
    return cache->wait_for_next();
}

bool BatchSequenceBypass::has_next_now() {
    return cache->has_next_now();
}

// END BatchSequenceBypass

// BEGIN TableScan

TableScan::TableScan(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<ral::io::data_provider> provider, std::shared_ptr<ral::io::data_parser> parser, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::TableScanKernel), provider(provider), parser(parser), schema(schema), num_batches(0)
{
    if(parser->type() == ral::io::DataType::CUDF || parser->type() == ral::io::DataType::DASK_CUDF){
        num_batches = std::max(provider->get_num_handles(), (size_t)1);
    } else if (parser->type() == ral::io::DataType::CSV)	{
        auto csv_parser = static_cast<ral::io::csv_parser*>(parser.get());
        num_batches = 0;
        size_t max_bytes_chunk_size = csv_parser->max_bytes_chunk_size();
        if (max_bytes_chunk_size > 0) {
            int file_idx = 0;
            while (provider->has_next()) {
                auto data_handle = provider->get_next();
                int64_t file_size = data_handle.file_handle->GetSize().ValueOrDie();
                size_t num_chunks = (file_size + max_bytes_chunk_size - 1) / max_bytes_chunk_size;
                std::vector<int> file_row_groups(num_chunks);
                std::iota(file_row_groups.begin(), file_row_groups.end(), 0);
                schema.get_rowgroups()[file_idx] = std::move(file_row_groups);
                num_batches += num_chunks;
                file_idx++;
            }
            provider->reset();
        } else {
            num_batches = provider->get_num_handles();
        }
    } else {
        num_batches = provider->get_num_handles();
    }

    this->query_graph = query_graph;
}

void TableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    output->addToCache(std::move(inputs[0]));
}

kstatus TableScan::run() {
    CodeTimer timer;

    std::vector<int> projections(schema.get_num_columns());
    std::iota(projections.begin(), projections.end(), 0);

    //if its empty we can just add it to the cache without scheduling
    if (!provider->has_next()) {
        this->add_to_output_cache(std::move(schema.makeEmptyBlazingTable(projections)));
    } else {

        while(provider->has_next()) {
            //retrieve the file handle but do not open the file
            //this will allow us to prevent from having too many open file handles by being
            //able to limit the number of file tasks
            auto handle = provider->get_next(true);
            auto file_schema = schema.fileSchema(file_index);
            auto row_group_ids = schema.get_rowgroup_ids(file_index);
            //this is the part where we make the task now
            std::unique_ptr<ral::cache::CacheData> input =
                std::make_unique<ral::cache::CacheDataIO>(handle,parser,schema,file_schema,row_group_ids,projections);
            std::vector<std::unique_ptr<ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(input));
            auto output_cache = this->output_cache();

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    output_cache,
                    this);

            /*if (this->has_limit_ && output_cache->get_num_rows_added() >= this->limit_rows_) {
            //	break;
            }*/
            file_index++;
        }

        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="TableScan Kernel tasks created",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty();
        });
    }
    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="TableScan Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

    return kstatus::proceed;
}

std::pair<bool, uint64_t> TableScan::get_estimated_output_num_rows(){
    double rows_so_far = (double)this->output_.total_rows_added();
    double batches_so_far = (double)this->output_.total_batches_added();
    if (batches_so_far == 0 || num_batches == 0) {
        return std::make_pair(false, 0);
    } else {
        return std::make_pair(true, (uint64_t)(rows_so_far/(batches_so_far/((double)num_batches))));
    }
}

// END TableScan

// BEGIN BindableTableScan

BindableTableScan::BindableTableScan(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<ral::io::data_provider> provider, std::shared_ptr<ral::io::data_parser> parser, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::BindableTableScanKernel), provider(provider), parser(parser), schema(schema) {
    this->query_graph = query_graph;
    this->filtered = is_filtered_bindable_scan(expression);
}

void BindableTableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    auto & input = inputs[0];
    if(this->filtered) {
        auto columns = ral::processor::process_filter(input->toBlazingTableView(), expression, this->context.get());
        columns->setNames(fix_column_aliases(columns->names(), expression));

        output->addToCache(std::move(columns));
    } else {
        input->setNames(fix_column_aliases(input->names(), expression));
        output->addToCache(std::move(input));
    }
}

kstatus BindableTableScan::run() {
    CodeTimer timer;

    std::vector<int> projections = get_projections(expression);
    if(projections.size() == 0){
        projections.resize(schema.get_num_columns());
        std::iota(projections.begin(), projections.end(), 0);
    }

    //if its empty we can just add it to the cache without scheduling
    if (!provider->has_next()) {
        auto empty = schema.makeEmptyBlazingTable(projections);
        empty->setNames(fix_column_aliases(empty->names(), expression));
        this->add_to_output_cache(std::move(empty));
    
    } else {    

        while(provider->has_next()) {
            //retrieve the file handle but do not open the file
            //this will allow us to prevent from having too many open file handles by being
            //able to limit the number of file tasks
            auto handle = provider->get_next(true);
            auto file_schema = schema.fileSchema(file_index);
            auto row_group_ids = schema.get_rowgroup_ids(file_index);
            //this is the part where we make the task now
            std::unique_ptr<ral::cache::CacheData> input =
                std::make_unique<ral::cache::CacheDataIO>(handle,parser,schema,file_schema,row_group_ids,projections);
            std::vector<std::unique_ptr<ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(input));

            auto output_cache = this->output_cache();

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    output_cache,
                    this);

            file_index++;
            /*if (this->has_limit_ && output_cache->get_num_rows_added() >= this->limit_rows_) {
            //	break;
            }*/
        }

        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="BindableTableScan Kernel tasks created",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty();
        });
    }

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="BindableTableScan Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    return kstatus::proceed;
}

std::pair<bool, uint64_t> BindableTableScan::get_estimated_output_num_rows(){
    double rows_so_far = (double)this->output_.total_rows_added();
    double current_batch = (double)file_index;
    if (current_batch == 0 || num_batches == 0){
        return std::make_pair(false, 0);
    } else {
        return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
    }
}

// END BindableTableScan

// BEGIN Projection

Projection::Projection(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::ProjectKernel)
{
    this->query_graph = query_graph;
}

void Projection::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    auto & input = inputs[0];
    auto columns = ral::processor::process_project(std::move(input), expression, this->context.get());
    output->addToCache(std::move(columns));
}

kstatus Projection::run() {
    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
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
                                "info"_a="Projection Kernel tasks created",
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
                                "info"_a="Projection Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }
    return kstatus::proceed;
}

// END Projection

// BEGIN Filter

Filter::Filter(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::FilterKernel)
{
    this->query_graph = query_graph;
}

void Filter::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    auto & input = inputs[0];
    auto columns = ral::processor::process_filter(input->toBlazingTableView(), expression, this->context.get());
    output->addToCache(std::move(columns));
}

kstatus Filter::run() {
    CodeTimer timer;
    CodeTimer eventTimer(false);

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    while(cache_data != nullptr){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Filter Kernel tasks created",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty();
    });

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Filter Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

    return kstatus::proceed;
}

std::pair<bool, uint64_t> Filter::get_estimated_output_num_rows(){
    std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
    if (total_in.first){
        double out_so_far = (double)this->output_.total_bytes_added();
        double in_so_far = (double)this->total_input_bytes_processed;
        if (in_so_far == 0){
            return std::make_pair(false, 0);
        } else {
            return std::make_pair(true, (uint64_t)( ((double)total_in.second) *out_so_far/in_so_far) );
        }
    } else {
        return std::make_pair(false, 0);
    }
}

// END Filter

// BEGIN Print

kstatus Print::run() {
    std::lock_guard<std::mutex> lg(print_lock);
    BatchSequence input(this->input_cache(), this);
    while (input.wait_for_next() ) {
        auto batch = input.next();
        ral::utilities::print_blazing_table_view(batch->toBlazingTableView());
    }
    return kstatus::stop;
}

// END Print

// BEGIN OutputKernel

kstatus OutputKernel::run() {
    while (this->input_.get_cache()->wait_for_next()) {
        CodeTimer cacheEventTimer(false);

        cacheEventTimer.start();
        auto temp_output = std::move(this->input_.get_cache()->pullFromCache());
        cacheEventTimer.stop();

        if(temp_output){
            auto num_rows = temp_output->num_rows();
            auto num_bytes = temp_output->sizeInBytes();

            cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                            "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                            "query_id"_a=context->getContextToken(),
                            "source"_a=this->input_.get_cache()->get_id(),
                            "sink"_a=this->get_id(),
                            "num_rows"_a=num_rows,
                            "num_bytes"_a=num_bytes,
                            "event_type"_a="removeCache",
                            "timestamp_begin"_a=cacheEventTimer.start_time(),
                            "timestamp_end"_a=cacheEventTimer.end_time());

            output.emplace_back(std::move(temp_output));
        }
    }
    done = true;

    return kstatus::stop;
}

frame_type OutputKernel::release() {
    return std::move(output);
}

bool OutputKernel::is_done() {
    return done.load();
}

// END OutputKernel

} // namespace batch
} // namespace ral
