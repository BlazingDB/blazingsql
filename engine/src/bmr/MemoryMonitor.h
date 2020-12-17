#pragma once

#include <condition_variable>
#include <mutex>
#include <chrono>
#include <future>
#include <map>

class BlazingMemoryResource;
namespace ral {
namespace  batch{
    class tree_processor;
    class node;
} //namespace batch

class MemoryMonitor {

    public:
        MemoryMonitor(std::shared_ptr<ral::batch::tree_processor> tree, std::map<std::string, std::string> config_options);
        void start();
        void finalize();
    private:
        bool finished;
        std::mutex finished_lock;
        std::condition_variable condition;
        std::shared_ptr<ral::batch::tree_processor> tree;
        std::chrono::milliseconds period;
        BlazingMemoryResource* resource;
        std::future<void> monitor_future;

        bool need_to_free_memory();
        void downgradeCaches(ral::batch::node* starting_node);
};

}  // namespace ral
