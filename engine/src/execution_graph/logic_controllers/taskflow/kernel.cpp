#include "kernel.h"
#include "CodeTimer.h"
#include "communication/CommunicationData.h"

namespace ral {
namespace cache {

kernel::kernel(std::size_t kernel_id, std::string expr, std::shared_ptr<Context> context, kernel_type kernel_type_id)
    : total_input_bytes_processed{0}, expression{expr}, kernel_id(kernel_id), kernel_type_id{kernel_type_id}, context{context} {
    parent_id_ = -1;
    has_limit_ = false;
    limit_rows_ = -1;

    logger = spdlog::get("batch_logger");
    events_logger = spdlog::get("events_logger");
    cache_events_logger = spdlog::get("cache_events_logger");

    std::shared_ptr<spdlog::logger> kernels_logger;
    kernels_logger = spdlog::get("kernels_logger");

    if(kernels_logger != nullptr) {
        kernels_logger->info("{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}",
                            "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                            "query_id"_a=(this->context ? std::to_string(this->context->getContextToken()) : "null"),
                            "kernel_id"_a=this->get_id(),
                            "is_kernel"_a=1, //true
                            "kernel_type"_a=get_kernel_type_name(this->get_type_id()));
    }
}

std::shared_ptr<ral::cache::CacheMachine> kernel::output_cache(std::string cache_id) {
    cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
    return this->output_.get_cache(cache_id);
}

std::shared_ptr<ral::cache::CacheMachine> kernel::input_cache() {
    auto kernel_id = std::to_string(this->get_id());
    return this->input_.get_cache(kernel_id);
}

bool kernel::add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string cache_id,bool always_add) {
    CodeTimer cacheEventTimer(false);

    auto num_rows = table->num_rows();
    auto num_bytes = table->sizeInBytes();

    cacheEventTimer.start();

    std::string message_id = get_message_id();
    message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
    cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
    bool added = this->output_.get_cache(cache_id)->addToCache(std::move(table), message_id,always_add);

    cacheEventTimer.stop();

    if(cache_events_logger != nullptr) {
        cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                    "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                    "query_id"_a=context->getContextToken(),
                    "source"_a=this->get_id(),
                    "sink"_a=this->output_.get_cache(cache_id)->get_id(),
                    "num_rows"_a=num_rows,
                    "num_bytes"_a=num_bytes,
                    "event_type"_a="addCache",
                    "timestamp_begin"_a=cacheEventTimer.start_time(),
                    "timestamp_end"_a=cacheEventTimer.end_time());
    }

    return added;
}

bool kernel::add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string cache_id, bool always_add) {
    CodeTimer cacheEventTimer(false);

    auto num_rows = cache_data->num_rows();
    auto num_bytes = cache_data->sizeInBytes();

    cacheEventTimer.start();

    std::string message_id = get_message_id();
    message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
    cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
    bool added = this->output_.get_cache(cache_id)->addCacheData(std::move(cache_data), message_id, always_add);

    cacheEventTimer.stop();

    if(cache_events_logger != nullptr) {
        cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                    "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                    "query_id"_a=context->getContextToken(),
                    "source"_a=this->get_id(),
                    "sink"_a=this->output_.get_cache(cache_id)->get_id(),
                    "num_rows"_a=num_rows,
                    "num_bytes"_a=num_bytes,
                    "event_type"_a="addCache",
                    "timestamp_begin"_a=cacheEventTimer.start_time(),
                    "timestamp_end"_a=cacheEventTimer.end_time());
    }

    return added;
}

bool kernel::add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string cache_id) {
    CodeTimer cacheEventTimer(false);

    auto num_rows = host_table->num_rows();
    auto num_bytes = host_table->sizeInBytes();

    cacheEventTimer.start();

    std::string message_id = get_message_id();
    message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
    cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
    bool added = this->output_.get_cache(cache_id)->addHostFrameToCache(std::move(host_table), message_id);

    cacheEventTimer.stop();

    if(cache_events_logger != nullptr) {
        cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                    "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                    "query_id"_a=context->getContextToken(),
                    "source"_a=this->get_id(),
                    "sink"_a=this->output_.get_cache(cache_id)->get_id(),
                    "num_rows"_a=num_rows,
                    "num_bytes"_a=num_bytes,
                    "event_type"_a="addCache",
                    "timestamp_begin"_a=cacheEventTimer.start_time(),
                    "timestamp_end"_a=cacheEventTimer.end_time());
    }

    return added;
}

// this function gets the estimated num_rows for the output
// the default is that its the same as the input (i.e. project, sort, ...)
std::pair<bool, uint64_t> kernel::get_estimated_output_num_rows(){
    return this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
}

void kernel::process(std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream,
        const std::map<std::string, std::string>& args){
    std::vector< std::unique_ptr<ral::frame::BlazingTable> > input_gpu;

    /*if (this->has_limit_ && output->get_num_rows_added() >= this->limit_rows_) {
  //      return;
    }*/

    for(auto & input : inputs){
        try{
            //if its in gpu this wont fail
            //if its cpu and it fails the buffers arent deleted
            //if its disk and fails the file isnt deleted
            //so this should be safe
            input_gpu.push_back(std::move(input->decache()));

        }catch(std::exception & e){
            auto logger = spdlog::get("batch_logger");
            if (logger){
                logger->error("|||{info}|||||",
                        "info"_a="ERROR in kernel::process trying to decache. What: {}"_format(e.what()));
            }
            exit(-1); // WSM DEBUG REMOVE THIS
            throw;
        }
    }

    try{
       std::size_t bytes = 0;
       for (auto & input : input_gpu) {
           bytes += input->sizeInBytes();
       }
       do_process(std::move(input_gpu),output,stream, args);
       total_input_bytes_processed += bytes; // increment this AFTER its been processed successfully

    }catch(std::exception & e){
        auto logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in kernel::process trying to do do_process. Kernel name is: " + this->kernel_name() + " Kernel id is: " + std::to_string(this->kernel_id) + " What: {}"_format(e.what()));
        }
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
    if (total_input_bytes_processed.load() > 0){
        return (std::size_t)((double)input_bytes * ((double)this->output_.total_bytes_added()/(double)total_input_bytes_processed.load()));
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
