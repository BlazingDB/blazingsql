#include "distributing_kernel.h"
#include "utilities/CommonOperations.h"

namespace ral {
namespace cache {

distributing_kernel::distributing_kernel(std::size_t kernel_id,
        std::string expr,
        std::shared_ptr<Context> context,
        kernel_type kernel_type_id,
        std::string node_id,
        const blazingdb::transport::Node& node,
        ral::cache::CacheMachine* output_message_cache)
    : kernel{kernel_id, expr, context, kernel_type::DistributeAggregateKernel},
      node_id{node_id},
      node{node},
      output_message_cache{output_message_cache} {

}

int distributing_kernel::get_total_partition_counts() {
    int total_count = node_count[node_id];
    for (auto message : messages_to_wait_for){
        auto meta_message = input_message_cache->pullCacheData(message);
        total_count += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
    }
    return total_count;
}

void distributing_kernel::send_total_partition_counts(ral::cache::CacheMachine* graph_output,
        std::string message_prefix,
        std::string metadata_label,
        std::string cache_id) {
    auto nodes = context->getAllNodes();

    for(std::size_t i = 0; i < nodes.size(); ++i) {
        if(!(nodes[i] == node)) {
            ral::cache::MetadataDictionary metadata;
            metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, kernel_id);
            metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(context->getContextToken()));
            metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, metadata_label);
            metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);
            metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, node.id());
            metadata.add_value(ral::cache::MESSAGE_ID, message_prefix +
                                            metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
                                            metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
                                            metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
            metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
            metadata.add_value(ral::cache::PARTITION_COUNT, std::to_string(node_count[nodes[i].id()]));
            messages_to_wait_for.push_back(message_prefix +
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
        std::string metadata_label,
        std::string cache_id) {
    auto nodes = context->getAllNodes();

    ral::cache::MetadataDictionary metadata;
    metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, kernel_id);
    metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(context->getContextToken()));
    metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, metadata_label);
    metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, node.id());

    for(std::size_t i = 0; i < nodes.size(); ++i) {
        if (nodes[i] == node) {
            // hash_partition followed by split does not create a partition that we can own, so we need to clone it.
            // if we dont clone it, hashed_data will go out of scope before we get to use the partition
            // also we need a BlazingTable to put into the cache, we cant cache views.
            output->addToCache(std::move(partitions[i].clone()), message_id, true);
            node_count[node.id()]++;
        } else {
            metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
            metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);

            node_count[nodes[i].id()]++;
            graph_output->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(partitions[i].clone(), metadata), "", true);
        }
    }
}

void distributing_kernel::increment_node_count(std::string node_id) {
    node_count[node_id]++;
}

}  // namespace cache
}  // namespace ral
