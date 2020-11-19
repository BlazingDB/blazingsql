#pragma once

#include "kernel_type.h"
#include "port.h"
#include "graph.h"
#include "communication/CommunicationData.h"
#include "CodeTimer.h"
#include "utilities/ctpl_stl.h"
#include "blazingdb/concurrency/BlazingThread.h"

namespace ral {
namespace cache {
class kernel;
class graph;
using kernel_pair = std::pair<kernel *, std::string>;

/**
 * @brief This interface represents a computation unit in the execution graph.
 * Each kernel has basically and input and output ports and the expression asocciated to the computation unit.
 * Each class that implements this interface should define how the computation is executed. See `run()` method.
 */
class kernel {
public:
	/**
	 * Constructor for the kernel
	 * @param kernel_id Current kernel identifier.
	 * @param expr Original logical expression that the kernel will execute.
	 * @param context Shared context associated to the running query.
	 * @param kernel_type_id Identifier representing the kernel type.
	 */
	kernel(std::size_t kernel_id, std::string expr, std::shared_ptr<Context> context, kernel_type kernel_type_id) : expression{expr}, kernel_id(kernel_id), context{context}, kernel_type_id{kernel_type_id} {

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

	/**
	 * @brief Sets its parent kernel.
	 *
	 * @param id The identifier of its parent.
	 */
	void set_parent(size_t id) { parent_id_ = id; }

	/**
	 * @brief Indicates if the kernel has a parent.
	 *
	 * @return true If the kernel has a parent, false otherwise.
	 */
	bool has_parent() const { return parent_id_ != -1; }

	/**
	 * Destructor
	 */
	virtual ~kernel() {}

	/**
	 * @brief Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run() = 0;


	kernel_pair operator[](const std::string & portname) { return std::make_pair(this, portname); }

	/**
	 * @brief Returns the kernel identifier.
	 *
	 * @return int32_t The kernel identifier.
	 */
	std::int32_t get_id() const { return (kernel_id); }

	/**
	 * @brief Returns the kernel type identifier.
	 *
	 * @return kernel_type The kernel type identifier.
	 */
	kernel_type get_type_id() const { return kernel_type_id; }

	/**
	 * @brief Sets the kernel type identifier.
	 *
	 * @param kernel_type The new kernel type identifier.
	 */
	void set_type_id(kernel_type kernel_type_id_) { kernel_type_id = kernel_type_id_; }

	
	/**
	 * @brief Returns the input cache.
	 */
	std::shared_ptr<ral::cache::CacheMachine> input_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->input_.get_cache(kernel_id);
	}

	/**
	 * @brief Returns the output cache associated to an identifier.
	 *
	 * @return cache_id The identifier of the output cache.
	 */
	std::shared_ptr<ral::cache::CacheMachine> output_cache(std::string cache_id = "") {
		cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
		return this->output_.get_cache(cache_id);
	}

	/**
	 * @brief Adds a BlazingTable into the output cache.
	 *
	 * @param table The table that will be added to the output cache.
	 * @param cache_id The cache identifier.
	 */
	bool add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string cache_id = "",bool always_add = false) {
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

	/**
	 * @brief Adds a CacheData into the output cache.
	 *
	 * @param cache_data The cache_data that will be added to the output cache.
	 * @param cache_id The cache identifier.
	 */
	bool add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string cache_id = "", bool always_add = false) {
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

	/**
	 * @brief Adds a BlazingHostTable into the output cache.
	 *
	 * @param host_table The host table that will be added to the output cache.
	 * @param cache_id The cache identifier.
	 */
	bool add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string cache_id = "") {
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

	/**
	 * @brief Returns the current context.
	 */
	Context * get_context() const {
		return context.get();
	}

	/**
	 * @brief Returns the id message as a string.
	 */
	std::string get_message_id(){
		return std::to_string((int)this->get_type_id()) + "_" + std::to_string(this->get_id());
	}

	/**
	 * @brief Returns true if all the caches of an input are finished.
	 */
	bool input_all_finished() {
		return this->input_.all_finished();
	}

	/**
	 * @brief Returns sum of all the rows added to all caches of the input port.
	 */
	uint64_t total_input_rows_added() {
		return this->input_.total_rows_added();
	}

	/**
	 * @brief Returns true if a specific input cache is finished.
	 *
	 * @param port_name Name of the port.
	 */
	bool input_cache_finished(const std::string & port_name) {
		return this->input_.is_finished(port_name);
	}

	/**
	 * @brief Returns the number of rows added to a specific input cache.
	 *
	 * @param port_name Name of the port.
	 */
	uint64_t input_cache_num_rows_added(const std::string & port_name) {
		return this->input_.get_num_rows_added(port_name);
	}

	/**
	 * @brief Returns the estimated num_rows for the output, the default
	 * is that its the same as the input (i.e. project, sort, ...).
	 */
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows();
	

	void process(std::vector<std::unique_ptr<ral::cache::CacheData > > * inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, std::string kernel_process_name);

	virtual void do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream,std::string kernel_process_name){
			
		}
	void notify_complete(size_t task_id);
	void add_task(size_t task_id);
	bool finished_tasks(){
		return tasks.empty();
	}
protected:
	std::set<size_t> tasks;
	std::mutex kernel_mutex;
	std::condition_variable kernel_cv;

public:
	std::string expression; /**< Stores the logical expression being processed. */
	port input_{this}; /**< Represents the input cache machines and their names. */
	port output_{this}; /**< Represents the output cache machine and their name. */
	const std::size_t kernel_id; /**< Stores the current kernel identifier. */
	std::int32_t parent_id_; /**< Stores the parent kernel identifier if any. */
	bool execution_done = false; /**< Indicates whether the execution is complete. */
	kernel_type kernel_type_id; /**< Stores the id of the kernel type. */
	std::shared_ptr<graph> query_graph; /**< Stores a pointer to the current execution graph. */
	std::shared_ptr<Context> context; /**< Shared context of the running query. */

	bool has_limit_; /**< Indicates if the Logical plan only contains a LogicalTableScan (or BindableTableScan) and LogicalLimit. */
	int64_t limit_rows_; /**< Specifies the maximum number of rows to return. */

	std::shared_ptr<spdlog::logger> logger;
	std::shared_ptr<spdlog::logger> events_logger;
	std::shared_ptr<spdlog::logger> cache_events_logger;
};


}  // namespace cache


namespace execution{

class prioirity {
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
    std::string kernel_process_name);
	/**
	* Function which runs the kernel process on the inputs and puts results into output.
	* This function does not modify the inputs and can throw an exception. In the case it throws an exception it 
	* gets placed back in the executor if it was a memory exception.
	*/
	void run(cudaStream_t stream, executor * executor);
	void complete();
protected:
	std::vector<std::unique_ptr<ral::cache::CacheData > > inputs;	
	std::shared_ptr<ral::cache::CacheMachine> output;
	size_t task_id;
	ral::cache::kernel * kernel;
	size_t attempts = 0;
	size_t attempts_limit;
	std::string kernel_process_name = "";
};



class executor{
public:
	static executor * get_instance(){
		if(_instance == nullptr){
			throw std::runtime_error("Executor not initialized.");
		}
		return _instance;
	}

	static void init_executor(int num_threads){
		if(!_instance){
			_instance = new executor(num_threads);
			std::thread([]{
				_instance->execute();
			});
		}

	}

	void execute();
	size_t add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,std::string kernel_process_name){

		}
	void add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,
		size_t attempts,
		size_t task_id,std::string kernel_process_name){
			
		}

/*	
	void add_task(ral::batch::DataSourceSequence * inputs,
				std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,std::string kernel_process_name); 
	
	void add_task(ral::batch::DataSourceSe@rodquence * inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		ral::cache::kernel * kernel,
		size_t attempts,
		size_t task_id,std::string kernel_process_name);
*/

private:
	executor(int num_threads);
	ctpl::thread_pool<BlazingThread> pool;
	std::vector<cudaStream_t> streams; //one stream per thread
	ral::cache::WaitingQueue< std::unique_ptr<task> > task_queue;
	int shutdown = 0;
	static executor * _instance;

};


} // namespace execution

}  // namespace ral
