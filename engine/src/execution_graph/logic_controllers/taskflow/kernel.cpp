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
       std::size_t bytes = 0;
       for (auto & input : input_gpu) {
           bytes += input->sizeInBytes();
       }
       do_process(std::move(input_gpu),output,stream, kernel_process_name);
       total_input_bytes += bytes; // increment this AFTER its been processed successfully
       
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

// This is only the default estimate of the bytes to be output by a kernel based on the input.
// Each kernel should implement its own version of this, if its possible to obtain a better estimate
std::size_t kernel::estimate_output_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs){
    
    std::size_t input_bytes = 0;
    for (auto & input : inputs) {
        input_bytes += input->sizeInBytes();
    }

    // if we have already processed, then we can estimate based on previous inputs and outputs
    if (total_input_bytes.load() > 0){
        return (std::size_t)((double)input_bytes * ((double)this->output_.total_bytes_added()/(double)total_input_bytes.load()));
    } else { // if we have not already any batches, then lets estimate that the output is the same as the input
        return input_bytes;
    }
}

// This is only the default estimate of the bytes needed by the kernel to perform the operation based on the input.
// Each kernel should implement its own version of this, if its possible to obtain a better estimate
std::size_t kernel::estimate_operating_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs){
    std::size_t bytes_esimate = 0;
    for (auto & input : inputs) {
        bytes_esimate += input->sizeInBytes();
    }
    return bytes_esimate;
}

}  // end namespace cache
}  // end namespace ral
