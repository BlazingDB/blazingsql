#include "executor.h"

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
    try{
        kernel->process(inputs,output,stream,args);
        complete();
    }catch(rmm::bad_alloc e){

        auto logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR of type rmm::bad_alloc in task::run. What: {}"_format(e.what()));
        }
        exit(-1); // WSM DEBUG REMOVE THIS
        this->attempts++;
        if(this->attempts < this->attempts_limit){
            executor->add_task(std::move(inputs), output, kernel, attempts, task_id, args);
        }else{
            throw;
        }
    }catch(std::exception & e){
        auto logger = spdlog::get("batch_logger");
        if (logger){
            logger->error("|||{info}|||||",
                    "info"_a="ERROR in task::run. What: {}"_format(e.what()));
        }
        exit(-1); // WSM DEBUG REMOVE THIS
        throw;
    }
}

void task::complete(){
    kernel->notify_complete(task_id);
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

    while(shutdown == 0){
        //consider using get_all and calling in a loop.
        auto cur_task = this->task_queue.pop_or_wait();
        pool.push([cur_task{std::move(cur_task)},this](int thread_id){
            std::size_t memory_needed = cur_task->task_memory_needed();

            // Here we want to wait until we make sure we have enough memory to operate, or if there are no tasks currently running, then we want to go ahead and run
            std::unique_lock<std::mutex> lock(memory_safety_mutex);
            memory_safety_cv.wait(lock, [this, memory_needed] { 
                if (memory_needed < (processing_memory_limit - resource->get_memory_used())){
                    return true;
                } else if (active_tasks_counter.load() == 0){
                    auto logger = spdlog::get("batch_logger");
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
            cur_task->run(this->streams[thread_id],this);
            active_tasks_counter--;
            memory_safety_cv.notify_all();
        });
    }
}

} // namespace execution
} // namespace ral
