#include <vector>
#include <map>
#include "execution_graph/logic_controllers/CacheMachine.h"
#include <blazingdb/manager/Context.h>

namespace ral {
namespace distribution {

using Context = blazingdb::manager::Context;

class MessageManager {

    public:
        MessageManager(std::size_t kernel_id,
            std::shared_ptr<Context> context,
            std::string node_id,
            ral::cache::CacheMachine* output_message_cache)
            : kernel_id{kernel_id}, context{context}, node_id{node_id},
              output_message_cache{output_message_cache} {
        }

    void scatter(std::vector<ral::frame::BlazingTableView> partitions);
    void send_total_partition_counts(void);
    int get_total_partition_counts();

    private:
        int32_t kernel_id;
        std::shared_ptr<Context> context;
        std::string node_id;
        ral::cache::CacheMachine* input_message_cache;
        ral::cache::CacheMachine* output_message_cache;
        std::map<std::string, int> node_count; //must be thread-safe
        std::vector<std::string> messages_to_wait_for; //must be thread-safe
};

}  // namespace distribution
}  // namespace ral
