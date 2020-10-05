#include "MemoryMonitor.h"
#include "execution_graph/logic_controllers/CacheMachine.h"


namespace ral {

    void MemoryMonitor::start(){
        
        this->monitor_thread = BlazingThread([this](){
            std::unique_lock<std::mutex> lock(finished_lock);
            while(!condition.wait_for(lock, period, [this] { return this->finished; })){
                if (need_to_free_memory()){
                    downgradeCaches(&tree->root);
                }
            }
        });        
    }

    void MemoryMonitor::downgradeCaches(ral::batch::node* starting_node){
        if (starting_node->kernel_unit->get_id() != 0) { // we want to skip the output node
            for (auto iter = starting_node->kernel_unit->output_.cache_machines_.begin(); 
                    iter != starting_node->kernel_unit->output_.cache_machines_.end(); iter++) {
                size_t amount_downgraded = 0;
                do {
                    amount_downgraded = iter->second->downgradeCacheData();
                } while (amount_downgraded > 0 && need_to_free_memory()); // if amount_downgraded is 0 then there is was nothing left to downgrade
            }
        }
        if (need_to_free_memory()){
            if (starting_node->children.size() == 1){
                downgradeCaches(starting_node->children[0].get());
            } else if (starting_node->children.size() > 1){
                std::vector<BlazingThread> threads;
                for (auto node : starting_node->children){
                    threads.push_back(BlazingThread([this, node](){
                        this->downgradeCaches(node.get());
                    }));
                }
                for(auto & thread : threads) {
                    thread.join();
                }
            }            
        } 
    }
}  // namespace ral
