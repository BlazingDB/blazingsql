#include "BatchOrderByProcessing.h"
#include "CodeTimer.h"
#include <src/utilities/CommonOperations.h>
#include "taskflow/executor.h"
#include "parser/expression_utils.hpp"
#include "execution_graph/logic_controllers/CPUCacheData.h"
#include "execution_graph/logic_controllers/GPUCacheData.h"

namespace ral {
namespace batch {

// BEGIN PartitionSingleNodeKernel

PartitionSingleNodeKernel::PartitionSingleNodeKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::PartitionSingleNodeKernel} {
    this->query_graph = query_graph;
    this->input_.add_port("input_a", "input_b");

    if (is_window_function(this->expression)) {
        if (window_expression_contains_partition_by(this->expression)){
            std::tie(sortColIndices, sortOrderTypes) = ral::operators::get_vars_to_partition(this->expression);
        } else {
            std::tie(sortColIndices, sortOrderTypes) = ral::operators::get_vars_to_orders(this->expression);
		}
    } else {
        std::tie(sortColIndices, sortOrderTypes, std::ignore) = ral::operators::get_sort_vars(this->expression);
    }
}

ral::execution::task_result PartitionSingleNodeKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> /*output*/,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    
    try{
        auto & input = inputs[0];

        auto partitions = ral::operators::partition_table(partitionPlan->toBlazingTableView(), input->toBlazingTableView(), this->sortOrderTypes, this->sortColIndices);

        for (std::size_t i = 0; i < partitions.size(); i++) {
            std::string cache_id = "output_" + std::to_string(i);
            this->add_to_output_cache(
                std::make_unique<ral::frame::BlazingTable>(std::make_unique<cudf::table>(partitions[i]), input->names()),
                cache_id
                );
        }
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus PartitionSingleNodeKernel::run() {
    CodeTimer timer;

    BatchSequence input_partitionPlan(this->input_.get_cache("input_b"), this);
    partitionPlan = std::move(input_partitionPlan.next());

    while(this->input_.get_cache("input_a")->wait_for_next()){
        std::unique_ptr <ral::cache::CacheData> cache_data = this->input_.get_cache("input_a")->pullCacheData();

        if(cache_data != nullptr){
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(cache_data));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    nullptr,
                    this);
        }
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="PartitionSingleNode Kernel tasks created",
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

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="PartitionSingleNode Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END PartitionSingleNodeKernel

// BEGIN SortAndSampleKernel

SortAndSampleKernel::SortAndSampleKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : distributing_kernel{kernel_id,queryString, context, kernel_type::SortAndSampleKernel}
{
    this->query_graph = query_graph;
    set_number_of_message_trackers(2); //default
    this->output_.add_port("output_a", "output_b");
    get_samples = true;
    already_computed_partition_plan = false;
}

void SortAndSampleKernel::make_partition_plan_task(){
    
    already_computed_partition_plan = true;

    std::vector<std::unique_ptr <ral::cache::CacheData> > sampleCacheDatas;
    // first lets take the local samples and convert them to CacheData to make a task
    for (std::size_t i = 0; i < samplesTables.size(); ++i) {
        std::unique_ptr <ral::cache::CacheData> cache_data = std::make_unique<ral::cache::GPUCacheData>(std::move(samplesTables[i]));
        sampleCacheDatas.push_back(std::move(cache_data));
    }

    if (this->context->getAllNodes().size() > 1 && context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode())){
        auto nodes = context->getAllNodes();
        // next lets take the samples from other nodes and add them to the set of samples for the task
        for(std::size_t i = 0; i < nodes.size(); ++i) {
            if(!(nodes[i] == ral::communication::CommunicationData::getInstance().getSelfNode())) {
                std::string message_id = std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + nodes[i].id();
                auto samples_cache_data = this->query_graph->get_input_message_cache()->pullCacheData(message_id);
                ral::cache::CPUCacheData * cache_ptr = static_cast<ral::cache::CPUCacheData *> (samples_cache_data.get());
                total_num_rows_for_sampling += std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]);
                total_bytes_for_sampling += std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]) * std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::AVG_BYTES_PER_ROW_METADATA_LABEL]);
                sampleCacheDatas.push_back(std::move(samples_cache_data));
            }
        }
    }

    ral::execution::executor::get_instance()->add_task(
            std::move(sampleCacheDatas),
            this->output_cache("output_b"),
            this,
            {{"operation_type", "compute_partition_plan"}});  

}

void SortAndSampleKernel::compute_partition_plan(
    std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputSamples) {

    // just in case there is no data
    size_t final_avg_bytes_per_row = total_num_rows_for_sampling <= 0 ? 1 : total_bytes_for_sampling / total_num_rows_for_sampling;

    if (this->context->getAllNodes().size() == 1) { // single node mode
        auto partitionPlan = ral::operators::generate_partition_plan(inputSamples,
            total_num_rows_for_sampling, final_avg_bytes_per_row, this->expression, this->context.get());
        this->add_to_output_cache(std::move(partitionPlan), "output_b");
    } else { // distributed mode
        if( ral::utilities::checkIfConcatenatingStringsWillOverflow(inputSamples)) {
            if(logger){
                logger->warn("{query_id}|{step}|{substep}|{info}",
                                "query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
                                "step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
                                "substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
                                "info"_a="In SortAndSampleKernel::compute_partition_plan Concatenating Strings will overflow strings length");
            }
        }

        if(context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode())) {
            context->incrementQuerySubstep();
            
            std::unique_ptr<ral::frame::BlazingTable> partitionPlan = ral::operators::generate_partition_plan(inputSamples, total_num_rows_for_sampling, final_avg_bytes_per_row, this->expression, this->context.get());

            broadcast(std::move(partitionPlan),
                this->output_.get_cache("output_b").get(),
                "", // message_id_prefix
                "output_b", // cache_id
                PARTITION_PLAN_MESSAGE_TRACKER_IDX, //message_tracker_idx (message tracker for the partitionPlan)
                true); // always_add

        } else {
            context->incrementQuerySubstep();

            // just to concat all the samples
            std::vector<ral::frame::BlazingTableView> sampledTableViews;
            for (std::size_t i = 0; i < inputSamples.size(); i++){
                sampledTableViews.push_back(inputSamples[i]->toBlazingTableView());
            }
            auto concatSamples = ral::utilities::concatTables(sampledTableViews);
            concatSamples->ensureOwnership();

            ral::cache::MetadataDictionary extra_metadata;
            extra_metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, total_num_rows_for_sampling);
            extra_metadata.add_value(ral::cache::AVG_BYTES_PER_ROW_METADATA_LABEL, final_avg_bytes_per_row);
            send_message(std::move(concatSamples),
                false, //specific_cache
                "", //cache_id
                {this->context->getMasterNode().id()}, //target_id
                "", //message_id_prefix
                true, //always_add
                false, //wait_for
                SAMPLES_MESSAGE_TRACKER_IDX, //message_tracker_idx (message tracker for samples)
                extra_metadata);

            context->incrementQuerySubstep();
        }        
    }    
}

bool SortAndSampleKernel::all_node_samples_are_available(){

    std::vector<std::string> messged_ids_expected;
    auto nodes = context->getAllNodes();
    for(std::size_t i = 0; i < nodes.size(); ++i) {
        if(!(nodes[i] == ral::communication::CommunicationData::getInstance().getSelfNode())) {
            messged_ids_expected.push_back(std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + nodes[i].id());
        }
    }
    return this->query_graph->get_input_message_cache()->has_messages_now(messged_ids_expected);
}

ral::execution::task_result SortAndSampleKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {

    try{
        auto& operation_type = args.at("operation_type");

        auto & input = inputs[0];
        if (operation_type == "ordering_and_get_samples") {
            auto sortedTable = ral::operators::sort(input->toBlazingTableView(), this->expression);

            if (get_samples.load()) {
                auto sampledTable = ral::operators::sample(input->toBlazingTableView(), this->expression);

                std::lock_guard<std::mutex> samples_lock(samples_mutex);
                if (get_samples.load()) {
                    population_sampled += sampledTable->num_rows(); 
                    total_num_rows_for_sampling += input->view().num_rows();
                    total_bytes_for_sampling += input->sizeInBytes();
                    samplesTables.push_back(std::move(sampledTable));
                    if (population_sampled > max_order_by_samples) {
                        get_samples = false;  // we got enough samples, at least as max_order_by_samples
                    }
                }
            }

            if (!get_samples.load() && !already_computed_partition_plan.load()){
                if (context->getTotalNodes() > 1 && context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode())){
                    if (all_node_samples_are_available()){
                        make_partition_plan_task();
                    }
                } else {
                    make_partition_plan_task();
                }
            }

            if(sortedTable){
                auto num_rows = sortedTable->num_rows();
                auto num_bytes = sortedTable->sizeInBytes();
            }

            output->addToCache(std::move(sortedTable), "output_a");
        }
        else if (operation_type == "compute_partition_plan") {
            compute_partition_plan(std::move(inputs));
        }
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus SortAndSampleKernel::run() {
    CodeTimer timer;

    std::map<std::string, std::string> config_options = context->getConfigOptions();
    auto it = config_options.find("MAX_ORDER_BY_SAMPLES_PER_NODE");
    if (it != config_options.end()){
        max_order_by_samples = std::stoi(config_options["MAX_ORDER_BY_SAMPLES_PER_NODE"]);
    }

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();
    while (cache_data != nullptr) {
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache("output_a"),
                this,
                {{"operation_type", "ordering_and_get_samples"}});

        cache_data = this->input_cache()->pullCacheData();
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
    });

    if(auto ep = ral::execution::executor::get_instance()->last_exception()){
        std::rethrow_exception(ep);
    }
    lock.unlock();

    // If during the other ordering_and_get_samples tasks the computing the partition plan was not made (max_order_by_samples was not reached), then lets do it here
    if (!already_computed_partition_plan.load()) {
        make_partition_plan_task();

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock,[this]{
            return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
        });

        if(auto ep = ral::execution::executor::get_instance()->last_exception()){
            std::rethrow_exception(ep);
        }
    }

    this->output_cache("output_b")->wait_for_count(1); // waiting for the partition_plan to arrive before continuing

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="SortAndSample Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END SortAndSampleKernel

// BEGIN PartitionKernel

PartitionKernel::PartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : distributing_kernel{kernel_id, queryString, context, kernel_type::PartitionKernel} {
    this->query_graph = query_graph;
    this->input_.add_port("input_a", "input_b");

    std::map<std::string, std::string> config_options = context->getConfigOptions();
    int max_num_order_by_partitions_per_node = 8;
    auto it = config_options.find("MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE");
    if (it != config_options.end()){
        max_num_order_by_partitions_per_node = std::stoi(config_options["MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE"]);
    }
    set_number_of_message_trackers(max_num_order_by_partitions_per_node);

    if (is_window_function(this->expression)) {
        if (window_expression_contains_partition_by(this->expression)){
            std::tie(sortColIndices, sortOrderTypes) = ral::operators::get_vars_to_partition(this->expression);
        } else {
            std::tie(sortColIndices, sortOrderTypes) = ral::operators::get_vars_to_orders(this->expression);
		}
    } else {
        std::tie(sortColIndices, sortOrderTypes, std::ignore) = ral::operators::get_sort_vars(this->expression);
    }
}

ral::execution::task_result PartitionKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> /*output*/,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    try{
        auto & input = inputs[0];

        std::vector<ral::distribution::NodeColumnView> partitions = ral::distribution::partitionData(this->context.get(), input->toBlazingTableView(), partitionPlan->toBlazingTableView(), sortColIndices, sortOrderTypes);
        std::vector<int32_t> part_ids(partitions.size());
        std::generate(part_ids.begin(), part_ids.end(), [count=0, num_partitions_per_node = num_partitions_per_node] () mutable { return (count++) % (num_partitions_per_node); });

        scatterParts(partitions,
            "", //message_id_prefix
            part_ids
        );
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus PartitionKernel::run() {
    CodeTimer timer;

    BatchSequence input_partitionPlan(this->input_.get_cache("input_b"), this);
    partitionPlan = input_partitionPlan.next();
    assert(partitionPlan != nullptr);

    context->incrementQuerySubstep();

    std::map<std::string, std::map<int32_t, int> > node_count;

    auto nodes = context->getAllNodes();

    // If we have no partitionPlan, its because we have no data, therefore its one partition per node
    num_partitions_per_node = partitionPlan->num_rows() == 0 ? 1 : (partitionPlan->num_rows() + 1) / this->context->getTotalNodes();

    std::map<int32_t, int> temp_partitions_map;
    for (int i = 0; i < num_partitions_per_node; i++) {
        temp_partitions_map[i] = 0;
    }
    for (auto &&node : nodes) {
        node_count.emplace(node.id(), temp_partitions_map);
    }

    std::unique_ptr <ral::cache::CacheData> cache_data = this->input_.get_cache("input_a")->pullCacheData();
    while(cache_data != nullptr){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                nullptr,
                this);

        cache_data = this->input_.get_cache("input_a")->pullCacheData();
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Partition Kernel tasks created",
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

    for (auto i = 0; i < num_partitions_per_node; i++) {
        std::string cache_id = "output_" + std::to_string(i);
        send_total_partition_counts(
            "", //message_prefix
            cache_id, //cache_id
            i //message_tracker_idx
        );
    }

    for (auto i = 0; i < num_partitions_per_node; i++) {
        int total_count = get_total_partition_counts(i);
        std::string cache_id = "output_" + std::to_string(i);
        this->output_cache(cache_id)->wait_for_count(total_count);
    }

    if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="Partition Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}
// END PartitionKernel

// BEGIN MergeStreamKernel

MergeStreamKernel::MergeStreamKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::MergeStreamKernel}  {
    this->query_graph = query_graph;
}

ral::execution::task_result MergeStreamKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    try{
        if (inputs.empty()) {
            // no op
        } else if(inputs.size() == 1) {
            output->addToCache(std::move(inputs[0]));
        } else {
            std::vector< ral::frame::BlazingTableView > tableViewsToConcat;
            for (std::size_t i = 0; i < inputs.size(); i++){
                tableViewsToConcat.emplace_back(inputs[i]->toBlazingTableView());
            }
            auto output_merge = ral::operators::merge(tableViewsToConcat, this->expression);
            output->addToCache(std::move(output_merge));
        }
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus MergeStreamKernel::run() {
    CodeTimer timer;

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
            if(logger){
                logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="In MergeStream kernel batch {} for {}. What: {} . max_memory_used: {}"_format(batch_count, expression, e.what(), blazing_device_memory_resource::getInstance().get_full_memory_summary()),
                                    "duration"_a="");
            }
            throw;
        }
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="MergeStream Kernel tasks created",
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
                                "info"_a="MergeStream Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END MergeStreamKernel

// BEGIN LimitKernel

LimitKernel::LimitKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : distributing_kernel{kernel_id,queryString, context, kernel_type::LimitKernel}  {
    this->query_graph = query_graph;
    set_number_of_message_trackers(1); //default
}

ral::execution::task_result LimitKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {
    try{
        CodeTimer eventTimer(false);
        auto & input = inputs[0];
        if (rows_limit<0) {
            output->addToCache(std::move(input));
        } else {
            auto log_input_num_rows = input->num_rows();
            auto log_input_num_bytes = input->sizeInBytes();

            std::unique_ptr<ral::frame::BlazingTable> limited_input;
            bool output_is_just_input;

            eventTimer.start();
            std::tie(limited_input, output_is_just_input, rows_limit) = ral::operators::limit_table(input->toBlazingTableView(), rows_limit);
            eventTimer.stop();

            auto log_output_num_rows = output_is_just_input ? input->num_rows() : limited_input->num_rows();
            auto log_output_num_bytes = output_is_just_input ? input->sizeInBytes() : limited_input->sizeInBytes();

            if (output_is_just_input)
                output->addToCache(std::move(input));
            else
                output->addToCache(std::move(limited_input));
        }
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }
    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus LimitKernel::run() {
    CodeTimer timer;
    CodeTimer eventTimer(false);

    int64_t total_batch_rows = 0;
    std::vector<std::unique_ptr<ral::cache::CacheData>> cache_vector;
    BatchSequenceBypass input_seq(this->input_cache(), this);
    while (input_seq.wait_for_next()) {
        auto batch = input_seq.next();
        total_batch_rows += batch->num_rows();
        cache_vector.push_back(std::move(batch));
    }

    cudf::size_type limitRows;
    std::tie(std::ignore, std::ignore, limitRows) = ral::operators::get_sort_vars(this->expression);
    rows_limit = limitRows;

    if(this->context->getTotalNodes() > 1 && rows_limit >= 0) {
        this->context->incrementQuerySubstep();

        int self_node_idx = context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode());
        auto nodes_to_send = context->getAllOtherNodes(self_node_idx);

        std::vector<std::string> limit_messages_to_wait_for;
        std::vector<std::string> target_ids;
        for (auto & node_to_send : nodes_to_send) {
            target_ids.push_back(node_to_send.id());
            limit_messages_to_wait_for.push_back(
                std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + node_to_send.id());
        }
        ral::cache::MetadataDictionary extra_metadata;
        extra_metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, total_batch_rows);
        send_message(nullptr, //empty table
            false, //specific_cache
            "", //cache_id
            target_ids, //target_ids
            "", //message_id_prefix
            true, //always_add
            false, //wait_for
            0, //message_tracker_idx
            extra_metadata);

        int64_t prev_total_rows = 0;
        for (std::size_t i = 0; i < limit_messages_to_wait_for.size(); i++) {
            auto meta_message = this->query_graph->get_input_message_cache()->pullCacheData(limit_messages_to_wait_for[i]);
            if(static_cast<int>(i) < context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode())){
                prev_total_rows += std::stoi(static_cast<ral::cache::CPUCacheData*>(meta_message.get())->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]);
            }
        }
        rows_limit = std::min(std::max(rows_limit - prev_total_rows, int64_t{0}), total_batch_rows);
    }

    int batch_count = 0;
    for (auto &&cache_data : cache_vector) {
        try {
            std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
            inputs.push_back(std::move(cache_data));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this);

            if (rows_limit == 0){
                //break;
            }
            batch_count++;
        } catch(const std::exception& e) {
            // TODO add retry here
            if(logger){
                logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="In Limit kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                                    "duration"_a="");
            }
            throw;
        }
    }

    if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Limit Kernel tasks created",
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
                                    "info"_a="Limit Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
    }

    return kstatus::proceed;
}

// END LimitKernel

} // namespace batch
} // namespace ral
