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
        std::string metadata_label,
        std::string cache_id,
        std::string target_id) {
    ral::cache::MetadataDictionary metadata;
    metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(kernel_id));
    metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(context->getContextToken()));
    metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, metadata_label);
    metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);
    metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, node.id());
    metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, target_id);

    ral::cache::CacheMachine* output_cache = query_graph->get_output_message_cache();
    output_cache->addCacheData(std::unique_ptr<ral::cache::GPUCacheData>(new ral::cache::GPUCacheDataMetaData(std::move(table), metadata)));
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
            ral::cache::MetadataDictionary metadata;
            metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, kernel_id);
            metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(context->getContextToken()));
            metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
            metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);
            metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, node.id());
            metadata.add_value(ral::cache::MESSAGE_ID, message_prefix +
                                            metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
                                            metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
                                            metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
            metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
            metadata.add_value(ral::cache::PARTITION_COUNT, std::to_string(node_count[message_tracker_id][nodes[i].id()]));
            messages_to_wait_for[message_tracker_id].push_back(message_prefix +
                                            metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
                                            metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
                                            metadata.get_values()[ral::cache::WORKER_IDS_METADATA_LABEL]);
            graph_output->addCacheData(
                    std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata), "", true);
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

    ral::cache::MetadataDictionary metadata;
    metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, kernel_id);
    metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(context->getContextToken()));
    metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "true");
    metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, node.id());

    for(std::size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i] == node) {
            // hash_partition followed by split does not create a partition that we can own, so we need to clone it.
            // if we dont clone it, hashed_data will go out of scope before we get to use the partition
            // also we need a BlazingTable to put into the cache, we cant cache views.
            output->addToCache(std::move(partitions[i].clone()), message_id, true);
            node_count[message_tracker_id][node.id()]++;
        } else {
            metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
            metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);

            node_count[message_tracker_id][nodes[i].id()]++;
            graph_output->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(partitions[i].clone(), metadata), "", true);
        }
    }
}

void distributing_kernel::increment_node_count(std::string node_id, std::size_t message_tracker_id) {
    node_count[message_tracker_id][node_id]++;
}

}  // namespace cache
}  // namespace ral
