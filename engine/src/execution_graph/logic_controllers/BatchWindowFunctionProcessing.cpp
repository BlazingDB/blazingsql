#include "BatchWindowFunctionProcessing.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "taskflow/executor.h"
#include "CodeTimer.h"

#include <src/utilities/CommonOperations.h>
#include "parser/expression_utils.hpp"
#include <blazingdb/io/Util/StringUtil.h>

#include <cudf/concatenate.hpp>
#include <cudf/stream_compaction.hpp>
#include "cudf/column/column_view.hpp"
#include <cudf/rolling.hpp>
#include <cudf/filling.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/types.hpp>
#include <cudf/copying.hpp>

namespace ral {
namespace batch {

// BEGIN ComputeWindowKernel

ComputeWindowKernel::ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::ComputeWindowKernel} {
    this->query_graph = query_graph;
}

// TODO: support for LAG(), LEAD(), currently looks like Calcite has an issue obtaining the optimized plan
// TODO: Support for RANK() and DENSE_RANK() file an cudf feature/request
// TODO: Support for first_value() and last_value() file an cudf feature/request
std::unique_ptr<CudfColumn> ComputeWindowKernel::compute_column_from_window_function(cudf::table_view input_cudf_view, cudf::column_view input_col_view, std::size_t pos) {
    std::unique_ptr<cudf::aggregation> window_function = ral::operators::makeCudfAggregation(this->aggs_wind_func[pos]);
    std::unique_ptr<CudfColumn> windowed_col;
    std::vector<cudf::column_view> table_to_rolling;

    // want all columns to be partitioned
    for (std::size_t col_i = 0; col_i < this->column_indices_partitioned.size(); ++col_i) {
        table_to_rolling.push_back(input_cudf_view.column(this->column_indices_partitioned[col_i]));
    }

    cudf::table_view table_view_with_single_col(table_to_rolling);

    if (this->expression.find("order by") != std::string::npos) {
        // default ROWS/RANGE statement
        if (this->expression.find("UNBOUNDED PRECEDING and CURRENT ROW") != std::string::npos || this->expression.find("between") == std::string::npos) {
            windowed_col = cudf::grouped_rolling_window(table_view_with_single_col , input_col_view, input_col_view.size(), 0, 1, window_function);
        } else {
            throw std::runtime_error("In Window Function: RANGE or ROWS bound is not currently supported");
        }
        
    } else {
        windowed_col = cudf::grouped_rolling_window(table_view_with_single_col , input_col_view, input_col_view.size(), input_col_view.size(), 1, window_function);
    }

    return std::move(windowed_col);
}

ral::execution::task_result ComputeWindowKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    if (inputs.size() == 0) {
        return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    CodeTimer eventTimer(false);

    std::unique_ptr<ral::frame::BlazingTable> & input = inputs[0];

    try{
        cudf::table_view input_cudf_view = input->view();

        // saving the names of the columns and after we will add one by each new col
        std::vector<std::string> input_names = input->names();
        this->column_indices_wind_func = get_columns_to_apply_window_function(this->expression);
        std::tie(this->column_indices_partitioned, std::ignore) = ral::operators::get_vars_to_partition(this->expression);
        std::vector<std::string> aggs_wind_func_str = get_window_function_agg(this->expression); // return MIN  MAX  COUNT

        // fill all the Kind aggregations
        for (std::size_t col_i = 0; col_i < aggs_wind_func_str.size(); ++col_i) {
            AggregateKind aggr_kind_i = ral::operators::get_aggregation_operation(aggs_wind_func_str[col_i]);
            this->aggs_wind_func.push_back(aggr_kind_i);
        }

        std::vector< std::unique_ptr<CudfColumn> > new_wind_funct_cols;
        for (std::size_t col_i = 0; col_i < aggs_wind_func_str.size(); ++col_i) {
            cudf::column_view input_col_view = input_cudf_view.column(column_indices_wind_func[col_i]);

            // calling main window function
            std::unique_ptr<CudfColumn> windowed_col = compute_column_from_window_function(input_cudf_view, input_col_view, col_i);
            new_wind_funct_cols.push_back(std::move(windowed_col));
            input_names.push_back("");
        }

        // Adding these new columns
        std::unique_ptr<cudf::table> cudf_input = input->releaseCudfTable();
        std::vector< std::unique_ptr<CudfColumn> > output_columns = cudf_input->release();
        for (std::size_t col_i = 0; col_i < new_wind_funct_cols.size(); ++col_i) {
            output_columns.push_back(std::move(new_wind_funct_cols[col_i]));
        }

        std::unique_ptr<cudf::table> cudf_table_window = std::make_unique<cudf::table>(std::move(output_columns));
        std::unique_ptr<ral::frame::BlazingTable> windowed_table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table_window), input_names);

        if (windowed_table) {
            cudf::size_type num_rows = windowed_table->num_rows();
            std::size_t num_bytes = windowed_table->sizeInBytes();

        }

        output->addToCache(std::move(windowed_table));
    }catch(rmm::bad_alloc e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(std::exception e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus ComputeWindowKernel::run() {
    CodeTimer timer;

    std::unique_ptr<ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();

    while (cache_data != nullptr ){
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

    if (logger != nullptr) {
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


// START OverlapAccumulatorKernel

OverlapAccumulatorKernel::OverlapAccumulatorKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::OverlapAccumulatorKernel} {
    this->query_graph = query_graph;
    this->input_.add_port("batches", "presceding_overlaps", "following_overlaps");

    this->num_batches = 0;
	this->have_all_batches = false;

    batches_cache = this->input_.get_cache("batches");
    presceding_overlap_cache = this->input_.get_cache("presceding_overlaps");
    following_overlap_cache = this->input_.get_cache("following_overlaps");

    auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
	self_node_index = context->getNodeIndex(self_node);
    
}

void OverlapAccumulatorKernel::update_num_batches(){
    std::lock_guard(completion_mutex);
    std::vector<size_t> batch_indexes = batches_cache->get_all_indexes();
    size_t max_index = std::max_element(batch_indexes.begin(), batch_indexes.end());
    num_batches = max_index + 1;

    presceding_overlap_statuses.resize(num_batches, UNKNOWN_OVERLAP_STATUS);
    following_overlap_status.resize(num_batches, UNKNOWN_OVERLAP_STATUS);
    presceding_overlap_counters.resize(num_batches, 0);
    following_overlap_counters.resize(num_batches, 0);

    have_all_batches = batches_cache->is_finished();
}

const std::string TASK_ARG_OP_TYPE="operation_type";
const std::string TASK_ARG_OVERLAP_TYPE="overlap_type";
const std::string TASK_ARG_OVERLAP_SIZE="overlap_size";
// const std::string TASK_ARG_SOURCE_BATCH_INDEX="source_batch_index";
const std::string TASK_ARG_TARGET_BATCH_INDEX="target_batch_index";
const std::string TASK_ARG_TARGET_NODE_INDEX="target_node_index";

const std::string OVERLAP_TASK_TYPE="get_overlap";
const std::string PRESCEDING_OVERLAP_TYPE="presceding";
const std::string FOLLOWING_OVERLAP_TYPE="following";
const std::string NODE_COMPLETED_REQUEST="node_completed";
const std::string PRESCEDING_REQUEST="presceding_request";
const std::string FOLLOWING_REQUEST="following_request";
const std::string PRESCEDING_FULFILLMENT="presceding_fulfillment";
const std::string FOLLOWING_FULFILLMENT="following_fulfillment";


void OverlapAccumulatorKernel::set_overlap_status(bool presceding, int index, std::string status){
    std::lock_guard(completion_mutex);
    if (presceding){
        presceding_overlap_statuses[index] = status;
    } else {
        following_overlap_status[index] = status;
    }
}

std::string OverlapAccumulatorKernel::get_overlap_status(bool presceding, int index){
    std::lock_guard(completion_mutex);
    if (presceding){
        return presceding_overlap_statuses[index];
    } else {
        return following_overlap_status[index];
    }
}


ral::execution::task_result OverlapAccumulatorKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

    try {
        std::string operation_type = args.at(TASK_ARG_OP_TYPE);
        std::string overlap_type = args.at(TASK_ARG_OVERLAP_TYPE);
        size_t overlap_size = std::stoll(args.at(TASK_ARG_OVERLAP_SIZE));
        int target_batch_index = std::stoi(args.at(TASK_ARG_OVERLAP_SIZE));
        int target_node_index = std::stoi(args.at(TASK_ARG_TARGET_NODE_INDEX));

        if (operation_type == OVERLAP_TASK_TYPE){

            std::vector<BlazingTableView> tables_to_concat;
            size_t rows_remaining = overlap_size;
            
            if (overlap_type == PRESCEDING_OVERLAP_TYPE) {
                
                for (int i = inputs.size() -1; i >= 0; i--){
                    size_t cur_table_size = inputs[i]->rum_rows();
                    if (cur_table_size > rows_remaining){
                        bool front = false;
                        inputs[i] = ral::utilities::getLimitedRows(inputs[i]->toBlazingTableView(), rows_remaining, front);
                        tables_to_concat.push_front(inputs[i]->toBlazingTableView());
                        rows_remaining = 0;
                        break;
                    } else {
                        rows_remaining -= cur_table_size;
                        tables_to_concat.push_front(inputs[i]->toBlazingTableView());
                    }
                }

            } else { // if (overlap_type == FOLLOWING_OVERLAP_TYPE) {

                for (int i = 0; i < inputs.size(); i++){
                    size_t cur_table_size = inputs[i]->rum_rows();
                    if (cur_table_size > rows_remaining){
                        bool front = true;
                        inputs[i] = ral::utilities::getLimitedRows(inputs[i]->toBlazingTableView(), rows_remaining, front);
                        tables_to_concat.push_back(inputs[i]->toBlazingTableView());
                        rows_remaining = 0;
                        break;
                    } else {
                        rows_remaining -= cur_table_size;
                        tables_to_concat.push_back(inputs[i]->toBlazingTableView());
                    }
                }
            }
            std::unique_ptr<ral::frame::BlazingTable> output_table;
            if (tables_to_concat.size() == 1){
                output_table = std::move(inputs[i]);
            } else {
                output_table = ral::utilities::concatTables(tables_to_concat);
            }

            if (this->self_node_index == target_node_index){
                if (overlap_type == PRESCEDING_OVERLAP_TYPE) {
                    presceding_overlap_cache->put(target_batch_index, std::move(output_table));
                } else {
                    following_overlap_cache->put(target_batch_index, std::move(output_table));
                }
                set_overlap_status(overlap_type == PRESCEDING_OVERLAP_TYPE, target_batch_index, rows_remaining == 0 ? DONE_OVERLAP_STATUS : INCOMPLETE_OVERLAP_STATUS);
            } else {
                 //send to node
                ral::cache::MetadataDictionary extra_metadata;
                extra_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, overlap_type == PRESCEDING_OVERLAP_TYPE ? PRESCEDING_FULFILLMENT : FOLLOWING_FULFILLMENT);
                extra_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(this->self_node_index));
                extra_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, std::to_string(target_node_index));
                extra_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(target_batch_index));
                extra_metadata.add_value(ral::cache::OVERLAP_STATUS, rows_remaining == 0 ? DONE_OVERLAP_STATUS : INCOMPLETE_OVERLAP_STATUS);
                
                std::vector<std::string> target_ids = {std::to_string(target_node_index)};
                send_message(std::move(concated),
                    false, //specific_cache
                    "", //cache_id
                    target_ids, //target_ids
                    "", //message_id_prefix
                    true, //always_add
                    false, //wait_for
                    0, //message_tracker_idx
                    extra_metadata);
                }
            }
        }
    }catch(rmm::bad_alloc e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(std::exception e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

// WSM STILL TO DO, need to build all the logic to receive requests
// idea, maybe we handle intra node fulfillments using the same logic as requests
// the fulfillment of requests needs to go down the line. When it gets to the last batch, if its still not fulfilled, then try to use its overlap
// if the overlap is not fulfilled THEN request it from the neighbor

// can there be a race condition between submitting a follow up request and a notification of all done from the neighbor? Is this a problem?
// yes, there can be. What we need to do is make sure that the fulfilled request are captured in the right order

// another idea. This request response logic, right now is to take a request response and do a concat and check the state
//      maybe we instead have the overlap caches being an vector of overlap caches with array access, so that each batch index can have multiple parts?



void OverlapAccumulatorKernel::request_receiver(){

    int node_completions_received = 0;
    int total_nodes = context->getTotalNodes();
    // We need a node completion message from every neighboring node
    int node_completions_required = 0;
    if (total_nodes > 1){
        if (self_node_index == 0 || self_node_index == total_nodes - 1) {
            node_completions_required = 1;
        } else {
            node_completions_required = 2;
        }
    }
    bool all_completions_received = false;
    while(all_completions_received){
        std::string message_id = std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id());
        auto message_cache_data = this->query_graph->get_input_message_cache()->pullCacheData(message_id);
        auto metadata = message_cache_data->getMetadata();
        if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == NODE_COMPLETED_REQUEST){
            std::lock_guard lock(completion_mutex);
            node_completions_received++;
            completion_cv.notify_all();
        } else if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == PRESCEDING_REQUEST
            || metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == FOLLOWING_REQUEST){

            size_t overlap_size = std::stoll(metadata.get_value(ral::cache::OVERLAP_SIZE));
            int target_batch_index = std::stoi(metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX));
            int target_node_index = std::stoi(metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX));
            int source_batch_ind = metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == PRESCEDING_REQUEST ? num_batches : 0;

            prepare_overlap_task(metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == PRESCEDING_REQUEST, 
                source_batch_ind, target_node_index, target_batch_index, overlap_size, nullptr);
        
        } else if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == PRESCEDING_FULFILLMENT
                        || metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == FOLLOWING_FULFILLMENT){

            int source_node_index = std::stoi(metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX));
            int target_node_index = std::stoi(metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX));
            int target_batch_index = std::stoi(metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX));

            assert(target_node_index == self_node_index, "ERROR: FULFILLMENT message arrived at the wrong destination");

            std::unique_ptr<ral::cache::CacheData> existing_overlap;
            if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == PRESCEDING_FULFILLMENT){
                existing_overlap = presceding_overlap_cache->get_or_wait_CacheData(target_batch_index);
            else { // FOLLOWING_FULFILLMENT
                existing_overlap = following_overlap_cache->get_or_wait_CacheData(target_batch_index);
            }
            std::vector<std::string> col_names = existing_overlap->names();
            std::vector<cudf::data_type> schema = existing_overlap->get_schema();

            std::vector<std::unique_ptr<ral::cache::CacheData>> overlap_parts;
            if (existing_overlap->get_type() == CacheDataType::CONCATENATING){
                ral::cache::ConcatCacheData * concat_cache_ptr = static_cast<ral::cache::ConcatCacheData *> (existing_overlap.get());
                std::vector<std::unique_ptr<ral::cache::CacheData>> existing_overlap_parts; = concat_cache_ptr->releaseCacheDatas();
                for (int i = 0; i < existing_overlap_parts.size(); i++){
                    auto existing_metadata = existing_overlap_parts[i]->get_metadata();
                    std::string node_index_str = existing_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX);
                    int node_index = self_node_index;
                    if (node_index_str.length() > 0){ // if the cache data has no metadata or no OVERLAP_SOURCE_NODE_INDEX, its because it originated from this node
                        node_index = std::stoi(node_index_str);
                    }
                    if (source_node_index < node_index && message_cache_data != nullptr){
                        overlap_parts.push_back(std::move(message_cache_data));
                        overlap_parts.push_back(std::move(existing_overlap_parts[i]));                        
                    } else {
                        overlap_parts.push_back(std::move(existing_overlap_parts[i]));
                    }
                }
            } else {
                if (source_node_index < self_node_index) {
                    overlap_parts.push_back(std::move(message_cache_data));
                    overlap_parts.push_back(std::move(existing_overlap));                        
                } else{
                    overlap_parts.push_back(std::move(existing_overlap));                        
                    overlap_parts.push_back(std::move(message_cache_data));
                }
            }
            std::unique_ptr<ral::cache::ConcatCacheData> new_cache_data = std::make_unique<ral::cache::ConcatCacheData>(std::move(overlap_parts), col_names, schema);
            size_t overlap_num_rows = new_cache_data->num_rows();
            if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == PRESCEDING_FULFILLMENT){
                presceding_overlap_cache->put(target_batch_index, std::move(new_cache_data));
                set_overlap_status(true, target_batch_index, overlap_num_rows == presceding_overlap_amount ? DONE_OVERLAP_STATUS : INCOMPLETE_OVERLAP_STATUS);
            else { // FOLLOWING_FULFILLMENT
                following_overlap_cache->put(target_batch_index, std::move(new_cache_data));
                set_overlap_status(false, target_batch_index, overlap_num_rows == following_overlap_amount ? DONE_OVERLAP_STATUS : INCOMPLETE_OVERLAP_STATUS);
            }

        } else {
            // TODO throw ERROR unknown request type in window function
        }



        {
            std::lock_guard lock(completion_mutex);
            all_completions_received = node_completions_received <  node_completions_required;            
        }
                

    }
}

void OverlapAccumulatorKernel::prepare_overlap_task(bool presceding, int source_batch_ind, int target_node_index, int target_batch_index, 
    size_t overlap_size, std::unique_ptr<ral::cache::CacheData> starting_overlap){
     
    std::vector<std::unique_ptr<ral::cache::CacheData>> cache_datas_for_task;
    while(overlap_rows_needed > 0){
        // Lets first try to fulfill the overlap needed from this node
        if (source_batch_ind >= 0 && source_batch_ind < this->num_batches){  // WSM TODO, is num_batches finalized?
            std::unique_ptr<ral::frame::CacheData> batch = batches_cache->get_or_wait_CacheData(source_batch_ind);
            overlap_rows_needed = batch->num_rows() > overlap_rows_needed ? 0 : overlap_rows_needed - batch->num_rows();
            if (presceding){
                source_batch_ind--;
                cache_datas_for_task.push_front(std::move(batch));
            } else {
                source_batch_ind++;
                cache_datas_for_task.push_back(std::move(batch));
            }
        } else {
            // prepare request
            std::vector<std::string> determination_messages_to_wait_for;
            std::vector<std::string> target_ids;
            if (presceding && this->self_node_index > 0){
                target_ids.push_back(std::to_string(this->self_node_index - 1));
            } else if (!presceding && ((this->self_node_index + 1) < this->num_batches )){
                target_ids.push_back(std::to_string(this->self_node_index + 1));
            }
            if (target_ids.size() > 0){
                ral::cache::MetadataDictionary extra_metadata;
                extra_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, presceding ? PRESCEDING_OVERLAP_TYPE : FOLLOWING_OVERLAP_TYPE);
            	extra_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(overlap_rows_needed));
                extra_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, std::to_string(target_node_index));
                extra_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(target_batch_index));

                send_message(nullptr,
                    false, //specific_cache
                    "", //cache_id
                    target_ids, //target_ids
                    "", //message_id_prefix
                    true, //always_add
                    false, //wait_for
                    0, //message_tracker_idx
                    extra_metadata);
            }
        }

    }
    if (cache_datas_for_task.size() > 0){ // we have data, so lets make a task
        // since its a presceding overlap, the original overlap goes after the data we are going to get more overlap from
        if (starting_overlap){
            if (presceding){
                cache_datas_for_task.push_back(std::move(starting_overlap));
            } else {
                cache_datas_for_task.push_front(std::move(starting_overlap));
            }
        }
        
        std::map<std::string, std::string> task_args;
        task_args[TASK_ARG_OP_TYPE] = OVERLAP_TASK_TYPE;
        task_args[TASK_ARG_OVERLAP_TYPE] = presceding ? PRESCEDING_OVERLAP_TYPE : FOLLOWING_OVERLAP_TYPE;
        task_args[TASK_ARG_OVERLAP_SIZE] = std::to_string(overlap_size);
        task_args[TASK_ARG_TARGET_BATCH_INDEX] = std::to_string(target_batch_index);
        task_args[TASK_ARG_TARGET_NODE_INDEX] = std::to_string(target_node_index);
        ral::execution::executor::get_instance()->add_task(
            std::move(cache_datas_for_task),
            presceding ? presceding_overlap_cache : following_overlap_cache,
            this,
            task_args);
    }
}


kstatus OverlapAccumulatorKernel::run() {

    bool all_done = false;

    
    while (!all_done){

        // WSM TODO. We may need to wait until num_batches is fulfilled, otherwise this gets too hard

        if (!have_all_batches){
            this->update_num_batches();
        }
        
        for (size_t batch_ind = 0; batch_ind < num_batches; batch_ind++){
            
            // check and update overlap statuses
            if (get_overlap_status(true, batch_ind) == UNKNOWN_OVERLAP_STATUS){
                if (presceding_overlap_cache->has_data_in_index_now(batch_ind)){
                    auto overlap_cache_data = presceding_overlap_cache->get_or_wait_CacheData(batch_ind);
                    auto metadata = overlap_cache_data->getMetadata();
                    assert(metadata.has_value(ral::cache::OVERLAP_STATUS),"ERROR: Overlap Data did not have OVERLAP_STATUS");
                    set_overlap_status(true, batch_ind, metadata.get_value(ral::cache::OVERLAP_STATUS));

                    // if overlap status INCOMPLETE try to complete it or file request
                    if (get_overlap_status(true, batch_ind) == INCOMPLETE_OVERLAP_STATUS){ 
                        // presceding overlap is originally populated from the end of the previous batch
                        size_t cur_overlap_size = overlap_cache_data->num_rows();
                        int overlap_rows_needed = this->presceding_overlap_amount - cur_overlap_size;
                        if (overlap_rows_needed <= 0){
                            // TODO log WARNING
                            std::cout<<"WARNING: Overlap said INCOMPLETE_OVERLAP_STATUS when it was actually ok")<<std::endl;
                        } else {
                            int source_batch_ind = batch_ind - 1;

                            prepare_overlap_task(true, source_batch_ind, this->self_node_index, batch_ind, 
                                this->presceding_overlap_amount, std::move(overlap_cache_data));                           
                        }
                    }
                }                
            }
            if (get_overlap_status(false, batch_ind) == UNKNOWN_OVERLAP_STATUS){
                if (following_overlap_cache->has_data_in_index_now(batch_ind)){
                    auto overlap_cache_data = following_overlap_cache->get_or_wait_CacheData(batch_ind);
                    auto metadata = overlap_cache_data->getMetadata();
                    assert(metadata.has_value(ral::cache::OVERLAP_STATUS),"ERROR: Overlap Data did not have OVERLAP_STATUS");
                    set_overlap_status(false, batch_ind, metadata.get_value(ral::cache::OVERLAP_STATUS));

                    // if overlap status INCOMPLETE try to complete it or file request
                    if (get_overlap_status(false, batch_ind) == INCOMPLETE_OVERLAP_STATUS){ 
                        // following overlap is originally populated from the beginning of the next batch
                        size_t cur_overlap_size = overlap_cache_data->num_rows();
                        int overlap_rows_needed = this->following_overlap_amount - cur_overlap_size;
                        if (overlap_rows_needed <= 0){
                            // TODO log WARNING
                            std::cout<<"WARNING: Overlap said INCOMPLETE_OVERLAP_STATUS when it was actually ok")<<std::endl;
                        } else {
                            int source_batch_ind = batch_ind +1 1;

                            prepare_overlap_task(false, source_batch_ind, this->self_node_index, batch_ind, 
                                this->following_overlap_amount, std::move(overlap_cache_data));                           
                        }
                    }
                }                             
            }

            

            // if overlap is DONE and first or last, file the ALL OK request response

            // if overlaps are done and presceding and following overlaps are done, then package up and push to the output
            


        }
        // check if all_done
    }

}




sortedMergerWithOverlap
- it cannot fulfill intranode requests, because it does not have the data

overlapperKernel
- has to be able to fulfill internode and intranode requests
- if it gets an incomplete overlap, try to fulfill it intranode. If not, submit request.



PROBLEM:"how to tell when its done?"
EASY way:
when a node has all batches completed, it notifies its neighbors that its done. (2 notifications per node)
When you get a notification from each side you can continue.
If you get a request, you need to see if you can fulfill it using your data and overlaps












ral::execution::task_result ComputeWindowKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

}


// END OverlapAccumulatorKernel

} // namespace batch
} // namespace ral
