#include "MessageManager.h"
#include "utilities/CommonOperations.h"

namespace ral {
namespace distribution {

int MessageManager::get_total_partition_counts(std::map<std::string, int>& node_count) {
    int total_count = node_count[node_id];
    for (auto message : messages_to_wait_for){
        auto meta_message = input_message_cache->pullCacheData(message);
        total_count += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
    }
    return total_count;
}

void MessageManager::send_total_partition_counts(ral::cache::CacheMachine* graph_output,
        std::string message_prefix,
        std::string metadata_label,
        std::string cache_id,
        std::map<std::string, int>& node_count) {
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
                                            nodes[i].id());
            graph_output->addCacheData(
                    std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata),"",true);
        }
    }
}

}  // namespace distribution
}  // namespace ral
