#pragma once

#include <vector>
#include <map>
#include "execution_graph/logic_controllers/CacheMachine.h"
#include <blazingdb/manager/Context.h>
#include "kernel.h"

namespace ral {
namespace cache {

using Context = blazingdb::manager::Context;

class distributing_kernel : public kernel {
    public:
    distributing_kernel(std::size_t kernel_id,
        std::string expr,
        std::shared_ptr<Context> context,
        kernel_type kernel_type_id,
        std::string node_id,
        const blazingdb::transport::Node& node,
        ral::cache::CacheMachine* output_message_cache);
    
    void scatter(std::vector<ral::frame::BlazingTableView> partitions,
        ral::cache::CacheMachine* output,
        ral::cache::CacheMachine* graph_output,
        std::string message_id,
        std::string metadata_label,
        std::string cache_id);

    void send_total_partition_counts(ral::cache::CacheMachine* graph_output,
        std::string message_prefix,
        std::string metadata_label,
        std::string cache_id);

    int get_total_partition_counts();

    void increment_node_count(std::string node_id);

    private:
        std::string node_id;
        const blazingdb::transport::Node& node;
        ral::cache::CacheMachine* input_message_cache;
        ral::cache::CacheMachine* output_message_cache;
        std::map<std::string, int> node_count; //must be thread-safe
        std::vector<std::string> messages_to_wait_for; //must be thread-safe
};

}  // namespace cache
}  // namespace ral
