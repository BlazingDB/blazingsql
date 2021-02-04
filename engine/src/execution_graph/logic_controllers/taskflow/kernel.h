#pragma once

#include "kernel_type.h"
#include "port.h"
#include "graph.h"

namespace ral {
namespace execution{

enum task_status{
	SUCCESS,
	RETRY,
	FAIL
};

struct task_result{
	task_status status;
	std::string what;
	std::vector<std::unique_ptr<ral::frame::BlazingTable> > inputs;
};

} 
}

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
	kernel(std::size_t kernel_id, std::string expr, std::shared_ptr<Context> context, kernel_type kernel_type_id);

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
	std::shared_ptr<ral::cache::CacheMachine> input_cache();

	/**
	 * @brief Returns the output cache associated to an identifier.
	 *
	 * @return cache_id The identifier of the output cache.
	 */
	std::shared_ptr<ral::cache::CacheMachine> output_cache(std::string cache_id = "");

	/**
	 * @brief Adds a BlazingTable into the output cache.
	 *
	 * @param table The table that will be added to the output cache.
	 * @param cache_id The cache identifier.
	 */
	bool add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string cache_id = "",bool always_add = false);

	/**
	 * @brief Adds a CacheData into the output cache.
	 * @param cache_data The cache_data that will be added to the output cache.
	 * @param cache_id The cache identifier.
	 */
	bool add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string cache_id = "", bool always_add = false);

	/**
	 * @brief Adds a BlazingHostTable into the output cache.
	 *
	 * @param host_table The host table that will be added to the output cache.
	 * @param cache_id The cache identifier.
	 */
	bool add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string cache_id = "");

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

	ral::execution::task_result process(std::vector<std::unique_ptr<ral::frame::BlazingTable > >  inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args);

	virtual ral::execution::task_result do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable> > /*inputs*/,
		std::shared_ptr<ral::cache::CacheMachine> /*output*/,
		cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/){
			return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
	}

	std::size_t estimate_output_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs);
	std::size_t estimate_operating_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs);

	virtual std::string kernel_name() { return "base_kernel"; }

	void notify_complete(size_t task_id);
	void notify_fail(size_t task_id);
	void add_task(size_t task_id);
	bool finished_tasks(){
		return tasks.empty();
	}
protected:
	std::set<size_t> tasks;
	std::mutex kernel_mutex;
	std::condition_variable kernel_cv;
	std::atomic<std::size_t> total_input_bytes_processed;
	
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
	// TODO DFR
        // we dont need cache_events_logger here any more
	std::shared_ptr<spdlog::logger> cache_events_logger;
};


}  // namespace cache
}  // namespace ral
