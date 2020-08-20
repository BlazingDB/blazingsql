#include "distributing_kernel.h"
#include "utilities/CommonOperations.h"

namespace ral {
namespace cache {

distributing_kernel::distributing_kernel(std::size_t kernel_id,
        std::string expr,
        std::shared_ptr<Context> context,
        kernel_type kernel_type_id)
        : kernel{kernel_id, expr, context, kernel_type::DistributeAggregateKernel},
          node_id{ral::communication::CommunicationData::getInstance().getSelfNode().id()},
          node{ral::communication::CommunicationData::getInstance().getSelfNode()} {
}

void distributing_kernel::set_number_of_message_trackers(std::size_t num_message_trackers) {
    node_count.resize(num_message_trackers);
    messages_to_wait_for.resize(num_message_trackers);
}

void distributing_kernel::send_message(std::unique_ptr<ral::frame::BlazingTable> table,
        std::string specific_cache,
        std::string cache_id,
        std::string target_id,
        std::string total_rows,
        std::string message_id,
        bool always_add,
        bool wait_for,
        std::size_t message_tracker_id,
        ral::cache::MetadataDictionary extra_metadata) {
    ral::cache::MetadataDictionary metadata;
    metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(kernel_id));
    metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(context->getContextToken()));
    metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, specific_cache);
    metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);
    metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, node.id());
    metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, target_id);

    if (total_rows!="") {
        metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, total_rows);
    }

    if (message_id!="") {
        metadata.add_value(
            ral::cache::MESSAGE_ID, message_id +
            metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
            metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] + "_" +
            metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
    } else {
        metadata.add_value(
            ral::cache::MESSAGE_ID, metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
            metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] + "_" +
            metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
    }

    for(auto meta_value : extra_metadata.get_values()) {
        metadata.add_value(meta_value.first, meta_value.second);
    }

    ral::cache::CacheMachine* output_cache = query_graph->get_output_message_cache();

    if(table==nullptr) {
        output_cache->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata), "", always_add);
    } else {
        output_cache->addCacheData(std::unique_ptr<ral::cache::GPUCacheData>(new ral::cache::GPUCacheDataMetaData(std::move(table), metadata)), "", always_add);
    }

    if(wait_for) {
        messages_to_wait_for[message_tracker_id].push_back(message_id +
            metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
            metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] + "_" +
            metadata.get_values()[ral::cache::WORKER_IDS_METADATA_LABEL]);
    }
}

int distributing_kernel::get_total_partition_counts(std::size_t message_tracker_id) {
    int total_count = node_count[message_tracker_id][node_id];
    for (auto message : messages_to_wait_for[message_tracker_id]){
        auto meta_message = query_graph->get_input_message_cache()->pullCacheData(message);
        total_count += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
    }
    return total_count;
}

void distributing_kernel::send_total_partition_counts(ral::cache::CacheMachine* graph_output,
        std::string message_prefix,
        std::string cache_id,
        std::size_t message_tracker_id) {
    auto nodes = context->getAllNodes();

    for(std::size_t i = 0; i < nodes.size(); ++i) {
        if(!(nodes[i] == node)) {
            ral::cache::MetadataDictionary extra_metadata;
            extra_metadata.add_value(ral::cache::PARTITION_COUNT, std::to_string(node_count[message_tracker_id][nodes[i].id()]));

            send_message(nullptr,
                "false", //specific_cache
                cache_id, //cache_id
                nodes[i].id(), //target_id
                "", //total_rows
                message_prefix, //message_id
                true, //always_add
                true, //wait_for
                message_tracker_id,
                extra_metadata);
        }
    }
}

void distributing_kernel::scatter(std::vector<ral::frame::BlazingTableView> partitions,
        ral::cache::CacheMachine* output,
        ral::cache::CacheMachine* graph_output,
        std::string message_id,
        std::string cache_id,
        std::size_t message_tracker_id) {
    auto nodes = context->getAllNodes();
    assert(nodes.size() == partitions.size());

    for(std::size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i] == node) {
            // hash_partition followed by split does not create a partition that we can own, so we need to clone it.
            // if we dont clone it, hashed_data will go out of scope before we get to use the partition
            // also we need a BlazingTable to put into the cache, we cant cache views.
            output->addToCache(std::move(partitions[i].clone()), message_id, true);
            node_count[message_tracker_id][node.id()]++;
        } else {
            send_message(std::move(partitions[i].clone()),
                "true", //specific_cache
                cache_id, //cache_id
                nodes[i].id(), //target_id
                "", //total_rows
                "", //message_id
                true //always_add
            );

            node_count[message_tracker_id][nodes[i].id()]++;
        }
    }
}

void distributing_kernel::increment_node_count(std::string node_id, std::size_t message_tracker_id) {
    node_count[message_tracker_id][node_id]++;
}

}  // namespace cache
}  // namespace ral
