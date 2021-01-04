#pragma once

#include "kernel.h"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "ExceptionHandling/BlazingThread.h"
#include "utilities/ctpl_stl.h"

namespace ral {
namespace execution{

class priority {
public:

private:
	size_t priority_num_query; //can be used to prioritize one query over another
	size_t priority_num_kernel; //can be
};

class executor;

class task {
public:

	task(
	std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
	std::shared_ptr<ral::cache::CacheMachine> output,
	size_t task_id,
	ral::cache::kernel * kernel, size_t attempts_limit,
	const std::map<std::string, std::string>& args, size_t attempts = 0);

	/**
	* Function which runs the kernel process on the inputs and puts results into output.
	* This function does not modify the inputs and can throw an exception. In the case it throws an exception it
	* gets placed back in the executor if it was a memory exception.
	*/
	void run(cudaStream_t stream, executor * executor);
	void complete();
	std::size_t task_memory_needed();

	/**
	 * This function releases the inputs of a task so that they can be manipulated. They then need to be set again with set_inputs
	 */
	std::vector<std::unique_ptr<ral::cache::CacheData > > release_inputs();

	/**
	 * This function set the inputs of a task. It is meant to be used in conjunction with release_inputs
	 */
	void set_inputs(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs);

protected:
	std::vector<std::unique_ptr<ral::cache::CacheData > > inputs;
	std::shared_ptr<ral::cache::CacheMachine> output;
	size_t task_id;
	ral::cache::kernel * kernel;
	size_t attempts = 0;
	size_t attempts_limit;
	std::map<std::string, std::string> args;
};


class executor{
public:
	static executor * get_instance(){
		if(_instance == nullptr){
			throw std::runtime_error("Executor not initialized.");
		}
		return _instance;
	}

	static void init_executor(int num_threads, double processing_memory_limit_threshold){
		if(!_instance){
			_instance = new executor(num_threads, processing_memory_limit_threshold);
			_instance->task_id_counter = 0;
			_instance->active_tasks_counter = 0;
			auto thread = std::thread([/*_instance*/]{
				_instance->execute();
			});
			thread.detach();
		}
	}

	void execute();
	size_t add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel, const std::map<std::string, std::string>& args = {});

	void add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,
		size_t attempts,
		size_t task_id, const std::map<std::string, std::string>& args = {});

	void add_task(std::unique_ptr<task> task);

	std::unique_ptr<task> remove_task_from_back();

	void notify_memory_safety_cv(){
		memory_safety_cv.notify_all();
	}

private:
	executor(int num_threads, double processing_memory_limit_threshold);
	ctpl::thread_pool<BlazingThread> pool;
	std::vector<cudaStream_t> streams; //one stream per thread
	ral::cache::WaitingQueue< std::unique_ptr<task> > task_queue;
	int shutdown = 0;
	static executor * _instance;
	std::atomic<int> task_id_counter;
	size_t attempts_limit = 10;

	BlazingMemoryResource* resource;
	std::size_t processing_memory_limit;
	std::atomic<int> active_tasks_counter;
	std::mutex memory_safety_mutex;
	std::condition_variable memory_safety_cv;
};


} // namespace execution
} // namespace ral
