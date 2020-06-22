#pragma once
#include <condition_variable>
#include <mutex>
#include <chrono>

#include "execution_graph/logic_controllers/PhysicalPlanGenerator.h"

namespace ral {

    class MemoryMonitor {

        public:
            MemoryMonitor(ral::batch::tree_processor* tree) : tree(tree), finished(false){
                resource = &blazing_device_memory_resource::getInstance();
                period = std::chrono::milliseconds(50); // WSM make this configurable
            }

            void start();

            void finalize(){
                std::unique_lock<std::mutex> lock(finished_lock);
                finished = true;
                condition.notify_all();
            }


        private:
            bool finished;
            std::mutex finished_lock;
            std::condition_variable condition;
            // std::shared_ptr<ral::batch::tree_processor> tree;
            ral::batch::tree_processor* tree;
            std::chrono::milliseconds period;
            BlazingMemoryResource* resource;

            bool need_to_free_memory(){
                return resource->get_memory_used() > resource->get_memory_limit();
            }

            void downgradeCaches(ral::batch::node* starting_node);
    };

}  // namespace ral
