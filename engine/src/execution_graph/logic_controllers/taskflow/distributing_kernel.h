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
        kernel_type kernel_type_id);
    
    void set_number_of_message_trackers(std::size_t num_message_trackers);

    void send_message(std::unique_ptr<ral::frame::BlazingTable> table,
        std::string specific_cache,
        std::string cache_id,
        std::string target_id,
        std::string total_rows = "",
        std::string message_id = "",
        bool always_add = false);

    void scatter(std::vector<ral::frame::BlazingTableView> partitions,
        ral::cache::CacheMachine* output,
        ral::cache::CacheMachine* graph_output,
        std::string message_id,
        std::string cache_id,
        std::size_t message_tracker_id = 0);

    void send_total_partition_counts(ral::cache::CacheMachine* graph_output,
        std::string message_prefix,
        std::string cache_id,
        std::size_t message_tracker_id = 0);

    int get_total_partition_counts(std::size_t message_tracker_id = 0);

    void increment_node_count(std::string node_id, std::size_t message_tracker_id = 0);

    private:
        std::string node_id;
        const blazingdb::transport::Node& node;
        std::vector<std::map<std::string, int>> node_count; //must be thread-safe
        std::vector<std::vector<std::string>> messages_to_wait_for; //must be thread-safe
};

}  // namespace cache
}  // namespace ral
