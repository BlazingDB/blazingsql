#include "BatchOrderByProcessing.h"
#include "CodeTimer.h"
#include <src/utilities/CommonOperations.h>
#include "taskflow/executor.h"

namespace ral {
namespace batch {

// BEGIN PartitionSingleNodeKernel

PartitionSingleNodeKernel::PartitionSingleNodeKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::PartitionSingleNodeKernel} {
    this->query_graph = query_graph;
    this->input_.add_port("input_a", "input_b");
}

void PartitionSingleNodeKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t stream, const std::map<std::string, std::string>& args) {
    auto & input = inputs[0];

    auto partitions = ral::operators::partition_table(partitionPlan->toBlazingTableView(), input->toBlazingTableView(), this->expression);

    for (auto i = 0; i < partitions.size(); i++) {
        std::string cache_id = "output_" + std::to_string(i);
        this->add_to_output_cache(
            std::make_unique<ral::frame::BlazingTable>(std::make_unique<cudf::table>(partitions[i]), input->names()),
            cache_id
            );
    }
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

    if(logger != nullptr) {
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
        return this->tasks.empty();
    });

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="PartitionSingleNode Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

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
}

void SortAndSampleKernel::compute_partition_plan(std::vector<ral::frame::BlazingTableView> sampledTableViews, std::size_t avg_bytes_per_row, std::size_t local_total_num_rows) {

    if (this->context->getAllNodes().size() == 1){ // single node mode
        auto partitionPlan = ral::operators::generate_partition_plan(sampledTableViews,
            local_total_num_rows, avg_bytes_per_row, this->expression, this->context.get());
        this->add_to_output_cache(std::move(partitionPlan), "output_b");
    } else { // distributed mode
        if( ral::utilities::checkIfConcatenatingStringsWillOverflow(sampledTableViews)) {
            logger->warn("{query_id}|{step}|{substep}|{info}",
                            "query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
                            "step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
                            "substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
                            "info"_a="In SortAndSampleKernel::compute_partition_plan Concatenating Strings will overflow strings length");
        }

        auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
        if(context->isMasterNode(self_node)) {
            context->incrementQuerySubstep();
            auto nodes = context->getAllNodes();

            std::vector<std::unique_ptr<ral::cache::CacheData> >table_scope_holder;
            std::vector<size_t> total_table_rows;
            std::vector<size_t> total_avg_bytes_per_row;
            std::vector<ral::frame::BlazingTableView> samples;

            for(std::size_t i = 0; i < nodes.size(); ++i) {
                if(!(nodes[i] == self_node)) {
                    std::string message_id = std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + nodes[i].id();

                    table_scope_holder.push_back(this->query_graph->get_input_message_cache()->pullCacheData(message_id));
                    ral::cache::GPUCacheDataMetaData * cache_ptr = static_cast<ral::cache::GPUCacheDataMetaData *> (table_scope_holder[table_scope_holder.size() - 1].get());

                    total_table_rows.push_back(std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]));
                    total_avg_bytes_per_row.push_back(std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::AVG_BYTES_PER_ROW_METADATA_LABEL]));
                    samples.push_back(cache_ptr->getTableView());
                }
            }

            for (std::size_t i = 0; i < sampledTableViews.size(); i++){
                samples.push_back(sampledTableViews[i]);
            }

            total_table_rows.push_back(local_total_num_rows);
            total_avg_bytes_per_row.push_back(avg_bytes_per_row);

            // let's recompute the `avg_bytes_per_row` using info from all the other nodes
            size_t total_bytes = 0;
            size_t total_rows = 0;
            for(std::size_t i = 0; i < nodes.size(); ++i) {
                total_bytes += total_avg_bytes_per_row[i] * total_table_rows[i];
                total_rows += total_table_rows[i];
            }
            // just in case there is no data
            size_t final_avg_bytes_per_row = total_rows <= 0 ? 1 : total_bytes / total_rows;

            std::size_t totalNumRows = std::accumulate(total_table_rows.begin(), total_table_rows.end(), std::size_t(0));
            std::unique_ptr<ral::frame::BlazingTable> partitionPlan = ral::operators::generate_partition_plan(samples, totalNumRows, final_avg_bytes_per_row, this->expression, this->context.get());

            broadcast(std::move(partitionPlan),
                this->output_.get_cache("output_b").get(),
                "", // message_id_prefix
                "output_b", // cache_id
                PARTITION_PLAN_MESSAGE_TRACKER_IDX, //message_tracker_idx (message tracker for the partitionPlan)
                true); // always_add

        } else {
            context->incrementQuerySubstep();

            auto concatSamples = ral::utilities::concatTables(sampledTableViews);
            concatSamples->ensureOwnership();

            ral::cache::MetadataDictionary extra_metadata;
            extra_metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, local_total_num_rows);
            extra_metadata.add_value(ral::cache::AVG_BYTES_PER_ROW_METADATA_LABEL, avg_bytes_per_row);
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

        this->output_cache("output_b")->wait_for_count(1); // waiting for the partition_plan to arrive before continuing
    }
}

kstatus SortAndSampleKernel::run() {
    CodeTimer timer;
    CodeTimer eventTimer(false);

    bool try_num_rows_estimation = true;
    bool get_samples = true;
    bool estimate_samples = false;
    uint64_t num_rows_estimate = 0;
    uint64_t population_to_sample = 0;
    uint64_t population_sampled = 0;
    BlazingThread partition_plan_thread;

    float order_by_samples_ratio = 0.1;
    std::map<std::string, std::string> config_options = context->getConfigOptions();
    auto it = config_options.find("ORDER_BY_SAMPLES_RATIO");
    if (it != config_options.end()){
        order_by_samples_ratio = std::stof(config_options["ORDER_BY_SAMPLES_RATIO"]);
    }
    int max_order_by_samples = 10000;
    it = config_options.find("MAX_ORDER_BY_SAMPLES_PER_NODE");
    if (it != config_options.end()){
        max_order_by_samples = std::stoi(config_options["MAX_ORDER_BY_SAMPLES_PER_NODE"]);
    }

    bool ordered = false;
    BatchSequence input(this->input_cache(), this, ordered);
    std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
    std::vector<ral::frame::BlazingTableView> sampledTableViews;
    std::size_t localTotalNumRows = 0;
    std::size_t localTotalBytes = 0;
    int batch_count = 0;
    while (input.wait_for_next()) {
        try {
            auto batch = input.next();

            eventTimer.start();
            auto log_input_num_rows = batch ? batch->num_rows() : 0;
            auto log_input_num_bytes = batch ? batch->sizeInBytes() : 0;

            auto sortedTable = ral::operators::sort(batch->toBlazingTableView(), this->expression);
            if (get_samples) {
                auto sampledTable = ral::operators::sample(batch->toBlazingTableView(), this->expression, order_by_samples_ratio);
                sampledTableViews.push_back(sampledTable->toBlazingTableView());
                sampledTables.push_back(std::move(sampledTable));
            }
            localTotalNumRows += batch->view().num_rows();
            localTotalBytes += batch->sizeInBytes();

            // Try samples estimation
            if(try_num_rows_estimation) {
                std::tie(estimate_samples, num_rows_estimate) = this->query_graph->get_estimated_input_rows_to_cache(this->get_id(), std::to_string(this->get_id()));
                population_to_sample = static_cast<uint64_t>(num_rows_estimate * order_by_samples_ratio);
                population_to_sample = (population_to_sample > max_order_by_samples) ? (max_order_by_samples) : population_to_sample;
                try_num_rows_estimation = false;
            }
            population_sampled += batch->num_rows();
            if (estimate_samples && population_to_sample > 0 && population_sampled > population_to_sample)	{

                size_t avg_bytes_per_row = localTotalNumRows == 0 ? 1 : localTotalBytes/localTotalNumRows;
                partition_plan_thread = BlazingThread(&SortAndSampleKernel::compute_partition_plan, this, sampledTableViews, avg_bytes_per_row, num_rows_estimate);
                estimate_samples = false;
                get_samples = false;
            }
            // End estimation

            eventTimer.stop();

            if(sortedTable){
                auto log_output_num_rows = sortedTable->num_rows();
                auto log_output_num_bytes = sortedTable->sizeInBytes();

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

            if(this->add_to_output_cache(std::move(sortedTable), "output_a")){
                batch_count++;
            }

        } catch(const std::exception& e) {
            // TODO add retry here
            // Note that we have to handle the collected samples in a special way. We need to compare to the current batch_count and perhaps evict one set of samples
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="In SortAndSample kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                                        "duration"_a="");
            throw;
        }
    }

    if (partition_plan_thread.joinable()){
        partition_plan_thread.join();
    } else {
        size_t avg_bytes_per_row = localTotalNumRows == 0 ? 1 : localTotalBytes/localTotalNumRows;
        compute_partition_plan(sampledTableViews, avg_bytes_per_row, localTotalNumRows);
    }

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="SortAndSample Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

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
}

void PartitionKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t stream, const std::map<std::string, std::string>& args) {
    auto & input = inputs[0];

    std::vector<ral::distribution::NodeColumnView> partitions = ral::distribution::partitionData(this->context.get(), input->toBlazingTableView(), partitionPlan->toBlazingTableView(), sortColIndices, sortOrderTypes);
    std::vector<int32_t> part_ids(partitions.size());
    std::generate(part_ids.begin(), part_ids.end(), [count=0, num_partitions_per_node = num_partitions_per_node] () mutable { return (count++) % (num_partitions_per_node); });

    scatterParts(partitions,
        "", //message_id_prefix
        part_ids
    );
}

kstatus PartitionKernel::run() {
    CodeTimer timer;

    BatchSequence input_partitionPlan(this->input_.get_cache("input_b"), this);
    partitionPlan = input_partitionPlan.next();
    assert(partitionPlan != nullptr);

    context->incrementQuerySubstep();

    std::map<std::string, std::map<int32_t, int> > node_count;

    std::tie(sortColIndices, sortOrderTypes, std::ignore) =	ral::operators::get_sort_vars(this->expression);
    auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
    auto nodes = context->getAllNodes();

    // If we have no partitionPlan, its because we have no data, therefore its one partition per node
    num_partitions_per_node = partitionPlan->num_rows() == 0 ? 1 : (partitionPlan->num_rows() + 1) / this->context->getTotalNodes();

    std::map<int32_t, int> temp_partitions_map;
    for (size_t i = 0; i < num_partitions_per_node; i++) {
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

    if(logger != nullptr) {
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
        return this->tasks.empty();
    });

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

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Partition Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

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

void MergeStreamKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t stream, const std::map<std::string, std::string>& args) {

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
}

kstatus MergeStreamKernel::run() {
    CodeTimer timer;

    int batch_count = 0;
    for (auto idx = 0; idx < this->input_.count(); idx++)
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
                                "info"_a="In MergeStream kernel batch {} for {}. What: {} . max_memory_used: {}"_format(batch_count, expression, e.what(), blazing_device_memory_resource::getInstance().get_full_memory_summary()),
                                "duration"_a="");
            throw;
        }
    }

    if(logger != nullptr) {
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
        return this->tasks.empty();
    });

    if(logger != nullptr) {
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

void LimitKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t stream, const std::map<std::string, std::string>& args) {
    CodeTimer eventTimer(false);
    auto & input = inputs[0];

    if (rows_limit<0) {
        output->addToCache(std::move(input));
    } else {
        auto log_input_num_rows = input->num_rows();
        auto log_input_num_bytes = input->sizeInBytes();

        eventTimer.start();
        std::tie(input, rows_limit) = ral::operators::limit_table(std::move(input), rows_limit);
        eventTimer.stop();

        auto log_output_num_rows = input->num_rows();
        auto log_output_num_bytes = input->sizeInBytes();

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

        output->addToCache(std::move(input));
    }
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

        auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
        int self_node_idx = context->getNodeIndex(self_node);
        auto nodes_to_send = context->getAllOtherNodes(self_node_idx);

        std::vector<std::string> limit_messages_to_wait_for;
        std::vector<std::string> target_ids;
        for (auto i = 0; i < nodes_to_send.size(); i++) {
            target_ids.push_back(nodes_to_send[i].id());
            limit_messages_to_wait_for.push_back(
                std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + nodes_to_send[i].id());
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
        for (auto i = 0; i < limit_messages_to_wait_for.size(); i++)	{
            auto meta_message = this->query_graph->get_input_message_cache()->pullCacheData(limit_messages_to_wait_for[i]);
            if(i < context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode())){
                prev_total_rows += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData*>(meta_message.get())->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]);
            }
        }
        rows_limit = std::min(std::max(rows_limit - prev_total_rows, int64_t{0}), total_batch_rows);
    }

    int batch_count = 0;
    for (auto &&cache_data : cache_vector)
    {
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
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="In Limit kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                                "duration"_a="");
            throw;
        }
    }

    if(logger != nullptr) {
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
        return this->tasks.empty();
    });

    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="Limit Kernel Completed",
                                "duration"_a=timer.elapsed_time(),
                                "kernel_id"_a=this->get_id());

    return kstatus::proceed;
}

// END LimitKernel

} // namespace batch
} // namespace ral
