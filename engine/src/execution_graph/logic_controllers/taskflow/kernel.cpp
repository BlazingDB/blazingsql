#include "kernel.h"

namespace ral {
namespace cache {

// this function gets the estimated num_rows for the output
// the default is that its the same as the input (i.e. project, sort, ...)
std::pair<bool, uint64_t> kernel::get_estimated_output_num_rows(){
    return this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
}

void kernel::process(std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream,
        std::string kernel_process_name = ""){
    std::vector< std::unique_ptr<ral::frame::BlazingTable> > input_gpu;

    
    if (this->has_limit_ && output->get_num_rows_added() >= this->limit_rows_) {
  //      return;
    }

    for(auto & input : inputs){
        try{
            //if its in gpu this wont fail
            //if its cpu and it fails the buffers arent deleted
            //if its disk and fails the file isnt deleted
            //so this should be safe
            input_gpu.push_back(std::move(input->decache()));

        }catch(std::exception e){
            throw e;
        }
        

    }

    try{
       do_process(std::move(input_gpu),output,stream, kernel_process_name);
    }catch(std::exception e){
        //remake inputs here
        int i = 0;
        for(auto & input : inputs){
            if (input->get_type() == ral::cache::CacheDataType::GPU || input->get_type() == ral::cache::CacheDataType::GPU_METADATA){
                //this was a gpu cachedata so now its not valid
                static_cast<ral::cache::GPUCacheData *>(input.get())->set_data(std::move(input_gpu[i]));                 
            }
            i++;
        }
        throw;
    }
 
}

void kernel::add_task(size_t task_id){
    std::lock_guard<std::mutex> lock(kernel_mutex);
    this->tasks.insert(task_id);
}

void kernel::notify_complete(size_t task_id){
    std::lock_guard<std::mutex> lock(kernel_mutex);
    this->tasks.erase(task_id);
    kernel_cv.notify_one();
}

}  // end namespace cache
}  // end namespace ral
