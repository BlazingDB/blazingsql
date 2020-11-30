#include "executor.h"

namespace ral {
namespace execution{

size_t executor::add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    ral::cache::kernel * kernel,std::string kernel_process_name) {

    auto task_id = task_id_counter.fetch_add(1, std::memory_order_relaxed);

    kernel->add_task(task_id);

    auto task_added = std::make_unique<task>(
        std::move(inputs),output,task_id, kernel, attempts_limit,kernel_process_name
    );

    task_queue.put(std::move(task_added));
    return task_id;
}


void executor::add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,
		size_t attempts,
		size_t task_id,std::string kernel_process_name){

    auto task_added = std::make_unique<task>(
        std::move(inputs),output,task_id, kernel, attempts_limit,kernel_process_name, attempts
    );
    task_queue.put(std::move(task_added));
}

task::task(
    std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    size_t task_id,
    ral::cache::kernel * kernel, size_t attempts_limit,
    std::string kernel_process_name,
    size_t attempts) :
    inputs(std::move(inputs)),
    task_id(task_id), output(output),
    kernel(kernel), attempts_limit(attempts_limit),
    kernel_process_name(kernel_process_name), attempts(attempts) {
}


void task::run(cudaStream_t stream, executor * executor){
    try{
        kernel->process(inputs,output,stream,kernel_process_name);
        complete();
    }catch(rmm::bad_alloc e){
        this->attempts++;
        if(this->attempts < this->attempts_limit){
            executor->add_task(std::move(inputs), output, kernel, attempts, task_id, kernel_process_name);
        }else{
            throw;
        }
    }catch(std::exception e){
        throw;
    }
}

void task::complete(){
    kernel->notify_complete(task_id);
}

executor * executor::_instance;

executor::executor(int num_threads) :
 pool(num_threads), task_id_counter(0) {
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
            cur_task->run(this->streams[thread_id],this);
        });
    }
}

} // namespace execution
} // namespace ral
