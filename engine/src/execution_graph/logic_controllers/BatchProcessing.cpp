#include "BatchProcessing.h"
#include "CodeTimer.h"
#include "communication/CommunicationData.h"
#include "ExceptionHandling/BlazingThread.h"
#include "io/data_parser/CSVParser.h"
#include "parser/expression_utils.hpp"
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

RecordBatch BatchSequence::next() {
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

// BEGIN DataSourceSequence

DataSourceSequence::DataSourceSequence(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
    : context(context), loader(loader), schema(schema), batch_index{0}, n_batches{0}
{
    // n_partitions{n_partitions}: TODO Update n_batches using data_loader
    this->provider = loader.get_provider();
    this->parser = loader.get_parser();

    n_files = schema.get_files().size();
    for (size_t index = 0; index < n_files; index++) {
        all_row_groups.push_back(schema.get_rowgroup_ids(index));
    }

    is_empty_data_source = (n_files == 0 && parser->get_num_partitions() == 0);
    is_gdf_parser = parser->get_num_partitions() > 0;
    if(is_gdf_parser){
        n_batches = std::max(parser->get_num_partitions(), (size_t)1);
    } else if (parser->type() == ral::io::DataType::CSV)	{
        auto csv_parser = static_cast<ral::io::csv_parser*>(parser.get());

        n_batches = 0;
        size_t max_bytes_chuck_size = csv_parser->max_bytes_chuck_size();
        if (max_bytes_chuck_size > 0) {
            int file_idx = 0;
            while (provider->has_next()) {
                auto data_handle = provider->get_next();
                int64_t file_size = data_handle.fileHandle->GetSize().ValueOrDie();
                size_t num_chunks = (file_size + max_bytes_chuck_size - 1) / max_bytes_chuck_size;
                std::vector<int> file_row_groups(num_chunks);
                std::iota(file_row_groups.begin(), file_row_groups.end(), 0);
                all_row_groups[file_idx] = std::move(file_row_groups);
                n_batches += num_chunks;
                file_idx++;
            }
            provider->reset();
        } else {
            n_batches = n_files;
        }
    }	else {
        n_batches = 0;
        for (auto &&row_group : all_row_groups) {
            n_batches += std::max(row_group.size(), (size_t)1);
        }
    }

    if (!is_empty_data_source && !is_gdf_parser && has_next()) {
        current_data_handle = provider->get_next();
    }
}

RecordBatch DataSourceSequence::next() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!has_next()) {
        return nullptr;
    }

    if (is_empty_data_source) {
        batch_index++;
        return schema.makeEmptyBlazingTable(projections);
    }

    if(is_gdf_parser){
        auto ret = loader.load_batch(context.get(), projections, schema, ral::io::data_handle(), 0, {static_cast<cudf::size_type>(batch_index)} );
        batch_index++;

        return std::move(ret);
    }

    auto local_cur_data_handle = current_data_handle;
    auto local_cur_file_index = cur_file_index;
    auto local_all_row_groups = all_row_groups[cur_file_index];
    if (file_batch_index > 0 && file_batch_index >= local_all_row_groups.size()) {
        // a file handle that we can use in case errors occur to tell the user which file had parsing issues
        assert(provider->has_next());
        current_data_handle = provider->get_next();

        file_batch_index = 0;
        cur_file_index++;

        local_cur_data_handle = current_data_handle;
        local_cur_file_index = cur_file_index;
        local_all_row_groups = all_row_groups[cur_file_index];
    }

    std::vector<cudf::size_type> local_row_group;
    if (!local_all_row_groups.empty()) {
        local_row_group = { local_all_row_groups[file_batch_index] };
    }

    file_batch_index++;
    batch_index++;

    lock.unlock();

    try {
        return loader.load_batch(context.get(), projections, schema, local_cur_data_handle, local_cur_file_index, local_row_group);
    }	catch(const std::exception& e) {
        auto logger = spdlog::get("batch_logger");
        logger->error("{query_id}|||{info}|||||",
                                    "query_id"_a=context->getContextToken(),
                                    "info"_a="In DataSourceSequence while reading file {}. What: {}"_format(local_cur_data_handle.uri.toString(), e.what()));
        throw;
    }
}

bool DataSourceSequence::has_next() {
    return (is_empty_data_source && batch_index < 1) || (batch_index.load() < n_batches);
}

void DataSourceSequence::set_projections(std::vector<int> projections) {
    this->projections = projections;
}

size_t DataSourceSequence::get_batch_index() {
    return batch_index.load();
}

size_t DataSourceSequence::get_num_batches() {
    return n_batches;
}

// END DataSourceSequence

// BEGIN TableScan

TableScan::TableScan(std::size_t kernel_id, const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::TableScanKernel), input(loader, schema, context)
{
    this->query_graph = query_graph;
}


kstatus TableScan::run() {
    CodeTimer timer;

    int table_scan_kernel_num_threads = 4;
    std::map<std::string, std::string> config_options = context->getConfigOptions();
    auto it = config_options.find("TABLE_SCAN_KERNEL_NUM_THREADS");
    if (it != config_options.end()){
        table_scan_kernel_num_threads = std::stoi(config_options["TABLE_SCAN_KERNEL_NUM_THREADS"]);
    }
    bool has_limit = this->has_limit_;
    size_t limit_ = this->limit_rows_;

    // want to read only one file at a time to avoid OOM when `select * from table limit N`
    if (has_limit) {
        table_scan_kernel_num_threads = 1;
    }

    cudf::size_type current_rows = 0;
    std::vector<BlazingThread> threads;
    for (int i = 0; i < table_scan_kernel_num_threads; i++) {
        threads.push_back(BlazingThread([this, &has_limit, &limit_, &current_rows]() {
            CodeTimer eventTimer(false);

            std::unique_ptr<ral::frame::BlazingTable> batch;
            while(batch = input.next()) {
                eventTimer.start();
                eventTimer.stop();
                current_rows += batch->num_rows();

                events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                                "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                                "query_id"_a=context->getContextToken(),
                                "kernel_id"_a=this->get_id(),
                                "input_num_rows"_a=batch->num_rows(),
                                "input_num_bytes"_a=batch->sizeInBytes(),
                                "output_num_rows"_a=batch->num_rows(),
                                "output_num_bytes"_a=batch->sizeInBytes(),
                                "event_type"_a="compute",
                                "timestamp_begin"_a=eventTimer.start_time(),
                                "timestamp_end"_a=eventTimer.end_time());

                this->add_to_output_cache(std::move(batch));
                
                if (has_limit && current_rows >= limit_) {
                    break;
                }
            }
        }));
    }
    for (auto &&t : threads) {
        t.join();
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
    double num_batches = (double)this->input.get_num_batches();
    double current_batch = (double)this->input.get_batch_index();
    if (current_batch == 0 || num_batches == 0){
        return std::make_pair(false, 0);
    } else {
        return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
    }
}

// END TableScan

// BEGIN BindableTableScan

BindableTableScan::BindableTableScan(std::size_t kernel_id, const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::BindableTableScanKernel), input(loader, schema, context)
{
    this->query_graph = query_graph;
}

kstatus BindableTableScan::run() {
    CodeTimer timer;

    input.set_projections(get_projections(expression));

    int table_scan_kernel_num_threads = 4;
    std::map<std::string, std::string> config_options = context->getConfigOptions();
    auto it = config_options.find("TABLE_SCAN_KERNEL_NUM_THREADS");
    if (it != config_options.end()){
        table_scan_kernel_num_threads = std::stoi(config_options["TABLE_SCAN_KERNEL_NUM_THREADS"]);
    }

    bool has_limit = this->has_limit_;
    size_t limit_ = this->limit_rows_;
    cudf::size_type current_rows = 0;
    std::vector<BlazingThread> threads;
    for (int i = 0; i < table_scan_kernel_num_threads; i++) {
        threads.push_back(BlazingThread([expression = this->expression, &limit_, &has_limit, &current_rows, this]() {

            CodeTimer eventTimer(false);

            std::unique_ptr<ral::frame::BlazingTable> batch;

            while(batch = input.next()) {
                try {
                    eventTimer.start();
                    auto log_input_num_rows = batch->num_rows();
                    auto log_input_num_bytes = batch->sizeInBytes();

                    if(is_filtered_bindable_scan(expression)) {
                        auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, this->context.get());
                        current_rows += columns->num_rows();
                        columns->setNames(fix_column_aliases(columns->names(), expression));
                        eventTimer.stop();

                        if( columns ) {
                            auto log_output_num_rows = columns->num_rows();
                            auto log_output_num_bytes = columns->sizeInBytes();

                            events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                                            "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                                            "query_id"_a=context->getContextToken(),
                                            "kernel_id"_a=this->get_id(),
                                            "input_num_rows"_a=log_input_num_rows,
                                            "input_num_bytes"_a=log_input_num_bytes,
                                            "output_num_rows"_a=log_output_num_rows,
                                            "output_num_bytes"_a=log_output_num_bytes,
                                            "event_type"_a="compute",
                                            "timestamp_begin"_a=eventTimer.start_time(),
                                            "timestamp_end"_a=eventTimer.end_time());
                        }

                        this->add_to_output_cache(std::move(columns));
                    }
                    else{
                        current_rows += batch->num_rows();
                        batch->setNames(fix_column_aliases(batch->names(), expression));

                        auto log_output_num_rows = batch->num_rows();
                        auto log_output_num_bytes = batch->sizeInBytes();
                        eventTimer.stop();

                        events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                                        "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                                        "query_id"_a=context->getContextToken(),
                                        "kernel_id"_a=this->get_id(),
                                        "input_num_rows"_a=log_input_num_rows,
                                        "input_num_bytes"_a=log_input_num_bytes,
                                        "output_num_rows"_a=log_output_num_rows,
                                        "output_num_bytes"_a=log_output_num_bytes,
                                        "event_type"_a="compute",
                                        "timestamp_begin"_a=eventTimer.start_time(),
                                        "timestamp_end"_a=eventTimer.end_time());

                        this->add_to_output_cache(std::move(batch));
                    }

                    // useful when the Algebra Relacional only contains: BindableTableScan and LogicalLimit
                    if (has_limit && current_rows >= limit_) {
                        break;
                    }

                } catch(const std::exception& e) {
                    // TODO add retry here
                    logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                                    "query_id"_a=context->getContextToken(),
                                                    "step"_a=context->getQueryStep(),
                                                    "substep"_a=context->getQuerySubstep(),
                                                    "info"_a="In BindableTableScan kernel batch for {}. What: {}"_format(expression, e.what()),
                                                    "duration"_a="");
                    throw;
                }
            }
        }));
    }
    for (auto &&t : threads) {
        t.join();
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
    double num_batches = (double)this->input.get_num_batches();
    double current_batch = (double)this->input.get_batch_index();
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

kstatus Projection::run() {
    CodeTimer timer;
    CodeTimer eventTimer(false);

    BatchSequence input(this->input_cache(), this);
    int batch_count = 0;
    while (input.wait_for_next()) {
        try {
            auto batch = input.next();

            auto log_input_num_rows = batch ? batch->num_rows() : 0;
            auto log_input_num_bytes = batch ? batch->sizeInBytes() : 0;

            eventTimer.start();
            auto columns = ral::processor::process_project(std::move(batch), expression, context.get());
            eventTimer.stop();

            if(columns){
                auto log_output_num_rows = columns->num_rows();
                auto log_output_num_bytes = columns->sizeInBytes();
                if(events_logger != nullptr) {
                    events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                                "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                                "query_id"_a=context->getContextToken(),
                                "kernel_id"_a=this->get_id(),
                                "input_num_rows"_a=log_input_num_rows,
                                "input_num_bytes"_a=log_input_num_bytes,
                                "output_num_rows"_a=log_output_num_rows,
                                "output_num_bytes"_a=log_output_num_bytes,
                                "event_type"_a="compute",
                                "timestamp_begin"_a=eventTimer.start_time(),
                                "timestamp_end"_a=eventTimer.end_time());
                }
            }

            this->add_to_output_cache(std::move(columns));
            batch_count++;
        } catch(const std::exception& e) {
            // TODO add retry here
            if(logger != nullptr) {
                logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="In Projection kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                                        "duration"_a="");
            }
            throw;
        }
    }

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

kstatus Filter::run() {
    CodeTimer timer;
    CodeTimer eventTimer(false);

    BatchSequence input(this->input_cache(), this);
    int batch_count = 0;
    while (input.wait_for_next()) {
        try {
            auto batch = input.next();

            auto log_input_num_rows = batch->num_rows();
            auto log_input_num_bytes = batch->sizeInBytes();

            eventTimer.start();
            auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
            eventTimer.stop();

            auto log_output_num_rows = columns->num_rows();
            auto log_output_num_bytes = columns->sizeInBytes();

            events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                            "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                            "query_id"_a=context->getContextToken(),
                            "kernel_id"_a=this->get_id(),
                            "input_num_rows"_a=log_input_num_rows,
                            "input_num_bytes"_a=log_input_num_bytes,
                            "output_num_rows"_a=log_output_num_rows,
                            "output_num_bytes"_a=log_output_num_bytes,
                            "event_type"_a="compute",
                            "timestamp_begin"_a=eventTimer.start_time(),
                            "timestamp_end"_a=eventTimer.end_time());

            this->add_to_output_cache(std::move(columns));
            batch_count++;
        } catch(const std::exception& e) {
            // TODO add retry here
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="In Filter kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                                        "duration"_a="");
            throw;
        }
    }

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
        double out_so_far = (double)this->output_.total_rows_added();
        double in_so_far = (double)this->input_.total_rows_added();
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

    return kstatus::stop;
}

frame_type OutputKernel::release() {
    return std::move(output);
}

// END OutputKernel

} // namespace batch
} // namespace ral