#include "BatchProcessing.h"
#include "CodeTimer.h"
#include "communication/CommunicationData.h"
#include "ExceptionHandling/BlazingThread.h"
#include "io/data_parser/CSVParser.h"

#ifdef MYSQL_SUPPORT
#include "io/data_provider/sql/MySQLDataProvider.h"
#endif

// TODO percy
//#include "io/data_parser/sql/PostgreSQLParser.h"

#include "parser/expression_utils.hpp"
#include "taskflow/executor.h"
#include <cudf/types.hpp>
#include <src/utilities/DebuggingUtils.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "io/data_provider/sql/AbstractSQLDataProvider.h"

namespace ral {
namespace batch {

// Use get_projections and if there are no projections or expression is empty
// then returns a filled array with the sequence of all columns (0, 1, ..., n)
std::vector<int> get_projections_wrapper(size_t num_columns, const std::string &expression = "")
{
  if (expression.empty()) {
    std::vector<int> projections(num_columns);
    std::iota(projections.begin(), projections.end(), 0);
    return projections;
  }

  std::vector<int> projections = get_projections(expression);
  if(projections.size() == 0){
      projections.resize(num_columns);
      std::iota(projections.begin(), projections.end(), 0);
  }
  return projections;
}

// BEGIN BatchSequence

BatchSequence::BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache, const ral::cache::kernel * kernel, bool ordered)
: cache{cache}, kernel{kernel}, ordered{ordered}
{}

void BatchSequence::set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
    this->cache = cache;
}

std::unique_ptr<ral::frame::BlazingTable> BatchSequence::next() {
    if (ordered) {
        return cache->pullFromCache();
    } else {
        return cache->pullUnorderedFromCache();
    }
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
    return cache->pullCacheData();
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
    } else if (parser->type() == ral::io::DataType::MYSQL)	{
#ifdef MYSQL_SUPPORT
      ral::io::set_sql_projections<ral::io::mysql_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns()));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support MySQL integration");
#endif
    } else {
        num_batches = provider->get_num_handles();
    }

    this->query_graph = query_graph;
}

ral::execution::task_result TableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    try{
        output->addToCache(std::move(inputs[0]));
    }catch(const rmm::bad_alloc& e){
        //can still recover if the input was not a GPUCacheData 
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
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
                CacheDataDispatcher(handle, parser, schema, file_schema, row_group_ids, projections);

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

        if(logger) {
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="TableScan Kernel tasks created",
                                        "duration"_a=timer.elapsed_time(),
                                        "kernel_id"_a=this->get_id());
        }

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
        });

        if(auto ep = ral::execution::executor::get_instance()->last_exception()){
            std::rethrow_exception(ep);
        }
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="TableScan Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

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
    } else if (parser->type() == ral::io::DataType::MYSQL)	{
#ifdef MYSQL_SUPPORT
      ral::io::set_sql_projections<ral::io::mysql_data_provider>(provider.get(), get_projections_wrapper(schema.get_num_columns(), queryString));
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support MySQL integration");
#endif
    } else {
        num_batches = provider->get_num_handles();
    }
}

ral::execution::task_result BindableTableScan::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    auto & input = inputs[0];
    std::unique_ptr<ral::frame::BlazingTable> filtered_input;

    try{
        if(this->filtered) {
            filtered_input = ral::processor::process_filter(input->toBlazingTableView(), expression, this->context.get());
            filtered_input->setNames(fix_column_aliases(filtered_input->names(), expression));
            output->addToCache(std::move(filtered_input));
        } else {
            input->setNames(fix_column_aliases(input->names(), expression));
            output->addToCache(std::move(input));
        }
    }catch(const rmm::bad_alloc& e){
        //can still recover if the input was not a GPUCacheData
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus BindableTableScan::run() {
    CodeTimer timer;

    std::vector<int> projections = get_projections_wrapper(schema.get_num_columns(), expression);

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
                CacheDataDispatcher(handle, parser, schema, file_schema, row_group_ids, projections);
            std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
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

        if(logger){
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="BindableTableScan Kernel tasks created",
                                        "duration"_a=timer.elapsed_time(),
                                        "kernel_id"_a=this->get_id());
        }

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
        });

        if(auto ep = ral::execution::executor::get_instance()->last_exception()){
            std::rethrow_exception(ep);
        }
    }

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="BindableTableScan Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }
    return kstatus::proceed;
}

std::pair<bool, uint64_t> BindableTableScan::get_estimated_output_num_rows(){
    double rows_so_far = (double)this->output_.total_rows_added();
    double current_batch = (double)file_index;
    if (current_batch == 0 || num_batches == 0){
        return std::make_pair(false, 0);
    } else {
        return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/(double)num_batches)));
    }
}

// END BindableTableScan

// BEGIN Projection

Projection::Projection(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
: kernel(kernel_id, queryString, context, kernel_type::ProjectKernel)
{
    this->query_graph = query_graph;
}

ral::execution::task_result Projection::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    try{
        auto & input = inputs[0];
        auto columns = ral::processor::process_project(std::move(input), expression, this->context.get());
        output->addToCache(std::move(columns));
    }catch(const rmm::bad_alloc& e){
        //can still recover if the input was not a GPUCacheData 
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus Projection::run() {
    CodeTimer timer;

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    RAL_EXPECTS(cache_data != nullptr, "ERROR: Projection::run() first input CacheData was nullptr");

    // When this kernel will project all the columns (with or without aliases)
    // we want to avoid caching and decahing for this kernel
    bool bypassing_project, bypassing_project_with_aliases;
    std::vector<std::string> aliases;
    std::vector<std::string> column_names = cache_data->names();
    std::tie(bypassing_project, bypassing_project_with_aliases, aliases) = bypassingProject(this->expression, column_names);

    while(cache_data != nullptr){
        if (bypassing_project_with_aliases) {
            cache_data->set_names(aliases);
            this->add_to_output_cache(std::move(cache_data));
        } else if (bypassing_project) {
            this->add_to_output_cache(std::move(cache_data));
        } else {
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(cache_data));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this);
        }
        cache_data = this->input_cache()->pullCacheData();
    }

    if(logger) {
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
        return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
    });

    if(auto ep = ral::execution::executor::get_instance()->last_exception()){
        std::rethrow_exception(ep);
    }

    if(logger) {
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

ral::execution::task_result Filter::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    std::unique_ptr<ral::frame::BlazingTable> columns;
    try{
        auto & input = inputs[0];
        columns = ral::processor::process_filter(input->toBlazingTableView(), expression, this->context.get());
        output->addToCache(std::move(columns));
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus Filter::run() {
    CodeTimer timer;

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

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="Filter Kernel tasks created",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
    });

    if(auto ep = ral::execution::executor::get_instance()->last_exception()){
        std::rethrow_exception(ep);
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="Filter Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

std::pair<bool, uint64_t> Filter::get_estimated_output_num_rows(){
    std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
    if (total_in.first){
        double out_so_far = (double)this->output_.total_rows_added();
        double in_so_far = (double)this->total_input_rows_processed;
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
        std::unique_ptr<frame::BlazingTable> temp_output = this->input_.get_cache()->pullFromCache();

        if(temp_output){
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
