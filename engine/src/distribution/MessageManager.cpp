#include "MessageManager.h"

namespace ral {
namespace distribution {

int MessageManager::get_total_partition_counts() {
    int total_count = node_count[node_id];
    for (auto message : messages_to_wait_for){
        auto meta_message = input_message_cache->pullCacheData(message);
        total_count += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
    }
    return total_count;
}

}  // namespace distribution
}  // namespace ral
