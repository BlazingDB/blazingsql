#include "executor.h"
#include "execution_graph/logic_controllers/GPUCacheData.h"

using namespace fmt::literals;

namespace ral {
namespace execution{

size_t executor::add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    ral::cache::kernel * kernel, const std::map<std::string, std::string>& args) {

    auto task_id = task_id_counter.fetch_add(1, std::memory_order_relaxed);

    kernel->add_task(task_id);

    auto task_added = std::make_unique<task>(
        std::move(inputs),output,task_id, kernel, attempts_limit, args
    );

    task_queue.put(std::move(task_added));
    return task_id;
}


void executor::add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,
		size_t attempts,
		size_t task_id, const std::map<std::string, std::string>& args){

    auto task_added = std::make_unique<task>(
        std::move(inputs),output,task_id, kernel, attempts_limit, args, attempts
    );
    task_queue.put(std::move(task_added));
}

void executor::add_task(std::unique_ptr<task> task) {
    task_queue.put(std::move(task));
}

std::unique_ptr<task> executor::remove_task_from_back(){
    return std::move(task_queue.pop_back());
}

task::task(
    std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    size_t task_id,
    ral::cache::kernel * kernel, size_t attempts_limit,
    const std::map<std::string, std::string>& args,
    size_t attempts) :
    inputs(std::move(inputs)),
    output(output), task_id(task_id),
    kernel(kernel),attempts(attempts),
    attempts_limit(attempts_limit), args(args) {
    
    task_logger = spdlog::get("task_logger");
}

std::size_t task::task_memory_needed() {
    std::size_t bytes_to_decache = 0; // space needed to deache inputs which are currently not in GPU

    for (auto & input : inputs) {
        if (input->get_type() == ral::cache::CacheDataType::CPU || input->get_type() == ral::cache::CacheDataType::LOCAL_FILE){
            bytes_to_decache += input->sizeInBytes();
        } else if (input->get_type() == ral::cache::CacheDataType::IO_FILE){
            // TODO! Need to figure out what we want to do to try to estimate consumption for this
        }
    }
    return bytes_to_decache + kernel->estimate_output_bytes(inputs) + kernel->estimate_operating_bytes(inputs);
}


void task::run(cudaStream_t stream, executor * executor){
    std::vector< std::unique_ptr<ral::frame::BlazingTable> > input_gpu;
    CodeTimer decachingEventTimer;

    int last_input_decached = 0;
    ///////////////////////////////
    // Decaching inputs
    ///////////////////////////////
    try{
        for(auto & input : inputs){
                    //if its in gpu this wont fail
                    //if its cpu and it fails the buffers arent deleted
                    //if its disk and fails the file isnt deleted
                    //so this should be safe
                    last_input_decached++;
                    input_gpu.push_back(std::move(input->decache()));
            }
    }catch(const rmm::bad_alloc& e){
        int i = 0;
        for(auto & input : inputs){
            if (i < last_input_decached && input->get_type() == ral::cache::CacheDataType::GPU ){
                //this was a gpu cachedata so now its not valid
                static_cast<ral::cache::GPUCacheData *>(input.get())->set_data(std::move(input_gpu[i]));
            }
            i++;
        }

        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR of type rmm::bad_alloc in task::run. What: {}"_format(e.what()));
        }

        this->attempts++;
        if(this->attempts < this->attempts_limit){
            executor->add_task(std::move(inputs), output, kernel, attempts, task_id, args);
            return;
        }else{
            throw;
        }
    }catch(const std::exception& e){
        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in task::run. What: {}"_format(e.what()));
        }
        
        throw;
    }
    auto decaching_elapsed = decachingEventTimer.elapsed_time();

    std::size_t log_input_rows = 0;
    std::size_t log_input_bytes = 0;
    for (std::size_t i = 0; i < input_gpu.size(); ++i) {
        log_input_rows += input_gpu.at(i)->num_rows();
        log_input_bytes += input_gpu.at(i)->sizeInBytes();
    }
    
    CodeTimer executionEventTimer;
    auto task_result = kernel->process(std::move(input_gpu),output,stream, args);

    if(task_logger) {
        task_logger->info("{time_started}|{ral_id}|{query_id}|{kernel_id}|{duration_decaching}|{duration_execution}|{input_num_rows}|{input_num_bytes}",
                        "time_started"_a=decachingEventTimer.start_time(),
                        "ral_id"_a=kernel->get_context()->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                        "query_id"_a=kernel->get_context()->getContextToken(),
                        "kernel_id"_a=kernel->get_id(),
                        "duration_decaching"_a=decaching_elapsed,
                        "duration_execution"_a=executionEventTimer.elapsed_time(),
                        "input_num_rows"_a=log_input_rows,
                        "input_num_bytes"_a=log_input_bytes);
    }

    if(task_result.status == ral::execution::task_status::SUCCESS){
        complete();
    }else if(task_result.status == ral::execution::task_status::RETRY){
        std::size_t i = 0;
        for(auto & input : inputs){
            if(input != nullptr){
                if  (input->get_type() == ral::cache::CacheDataType::GPU){
                    //this was a gpu cachedata so now its not valid
                    if(task_result.inputs.size() > 0 && i <= task_result.inputs.size() && task_result.inputs[i] != nullptr && task_result.inputs[i]->is_valid()){ 
                        static_cast<ral::cache::GPUCacheData *>(input.get())->set_data(std::move(task_result.inputs[i]));
                    }else{
                        //the input was lost and it was a gpu dataframe which is not recoverable
                        throw rmm::bad_alloc(task_result.what.c_str());
                    }
                }
            } else {
                throw std::runtime_error("Input is null, cannot recover");
            }
            i++;
        }
        this->attempts++;
        if(this->attempts < this->attempts_limit){
            executor->add_task(std::move(inputs), output, kernel, attempts, task_id, args);
        }else{
            throw rmm::bad_alloc("Ran out of memory processing");
        }
    }else{
        throw std::runtime_error(task_result.what.c_str());
    }
}

void task::complete(){
    kernel->notify_complete(task_id);
}

void task::fail(){
    kernel->notify_fail(task_id);
}

std::vector<std::unique_ptr<ral::cache::CacheData > > task::release_inputs(){
    return std::move(this->inputs);
}

void task::set_inputs(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs){
    this->inputs = std::move(inputs);
}



executor * executor::_instance;

executor::executor(int num_threads, double processing_memory_limit_threshold) :
 pool(num_threads), task_id_counter(0), resource(&blazing_device_memory_resource::getInstance()), task_queue("executor_task_queue") {
     processing_memory_limit = resource->get_total_memory() * processing_memory_limit_threshold;
     for( int i = 0; i < num_threads; i++){
         cudaStream_t stream;
         cudaStreamCreate(&stream);
         streams.push_back(stream);
     }
}

void executor::execute(){
    while(shutdown == 0 && exception_holder.empty()){
        //consider using get_all and calling in a loop.
        auto cur_task = this->task_queue.pop_or_wait();
        pool.push([cur_task{std::move(cur_task)}, this](int thread_id){
            std::size_t memory_needed = cur_task->task_memory_needed();

            // Here we want to wait until we make sure we have enough memory to operate, or if there are no tasks currently running, then we want to go ahead and run
            std::unique_lock<std::mutex> lock(memory_safety_mutex);
            memory_safety_cv.wait(lock, [this, memory_needed] { 
                if (memory_needed < (processing_memory_limit - resource->get_memory_used())){
                    return true;
                } else if (active_tasks_counter.load() == 0){
                    std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
                    if (logger){
                        logger->warn("|||{info}|||||",
                                "info"_a="WARNING: launching task even though over limit, because there are no tasks running. Memory used: {}"_format(std::to_string(resource->get_memory_used())));
                    }
                    return true;
                } else {
                    return false;
                }
                });

            active_tasks_counter++;

            try {
                cur_task->run(this->streams[thread_id],this);
            } catch(...) {
                std::unique_lock<std::mutex> lock(exception_holder_mutex);
                exception_holder.push(std::current_exception());
                cur_task->fail();
            }

            active_tasks_counter--;
            memory_safety_cv.notify_all();
        });
    }
}

std::exception_ptr executor::last_exception(){
    std::unique_lock<std::mutex> lock(exception_holder_mutex);
    std::exception_ptr e;
    if (!exception_holder.empty()) {
        e = exception_holder.front();
        exception_holder.pop();
    }
    return e;
}

bool executor::has_exception(){
    std::unique_lock<std::mutex> lock(exception_holder_mutex);
    return !exception_holder.empty();
}

} // namespace execution
} // namespace ral
