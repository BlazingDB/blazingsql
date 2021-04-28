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

/**
 * @brief This interface represents a computation unit in the execution graph.
 * Each kernel has basically and input and output ports and the expression asocciated to the computation unit.
 * Each class that implements this interface should define how the computation is executed. See `do_process()` method.
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
	 * @brief Returns the input cache associated to an identifier. If none is provided it returns the default
	 * 
	 * @param cache_id The cache identifier.
	 *
	 * @return A shared pointer to the desired CacheMachine
	 */
	std::shared_ptr<ral::cache::CacheMachine> input_cache(std::string cache_id = "");

	/**
	 * @brief Returns the output cache associated to an identifier. If none is provided it returns the default
	 * 
	 * @param cache_id The cache identifier.
	 *
	 * @return A shared pointer to the desired CacheMachine
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

	/**
	* @brief Invokes the do_process function.
	*/
	ral::execution::task_result process(std::vector<std::unique_ptr<ral::frame::BlazingTable > >  inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args);

	/**
	* @brief Implemented by all derived classes and is the function which actually performs transformations on dataframes.
	* @param inputs The data being operated on
	* @param output the output cache to write the output to
	* @param stream the cudastream to to use
	* @param args any additional arguments the kernel may need to perform its execution that may not be available to the kernel at instantiation.
	*/
	virtual ral::execution::task_result do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable> > /*inputs*/,
		std::shared_ptr<ral::cache::CacheMachine> /*output*/,
		cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/){
			return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
	}

	/**
	* @brief given the inputs, estimates the number of bytes that will be necessary for holding the output after performing a transformation. For many kernels this is not an estimate but rather a certainty. For operations whose outputs are of indeterminate size it provides an estimate.
	* @param inputs the data that would be transformed
	* @returns the number of bytes that we expect to be needed to hold the output after performing this kernels transformations on the given inputs.
	*/
	std::size_t estimate_output_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs);

	/**
	* @brief given the inputs, estimates the number of bytes that will be necessary for performing the transformation. This can be thought of as the memory overhead of the actual transformations being performed. For many kernels this is not an estimate but rather a certainty. For operations that perform indeterminately sized allocations based on the contents of inputs it provides an estimate.
	* @param inputs the data that would be transformed
	* @returns the number of bytes that we expect to be needed to hold the output after performing this kernels transformations on the given inputs.
	*/
	std::size_t estimate_operating_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs);

	virtual std::string kernel_name() { return "base_kernel"; }

	/**
	* @brief notify the kernel that a task it dispatched was completed successfully.
	*/
	void notify_complete(size_t task_id);
	/**
	* @brief notify the kernel that a task it dispatched failed.
	*/
	void notify_fail(size_t task_id);
	/**
	* @brief add a task to the list of tasks the kernel is waiting to complete.
	*/
	void add_task(size_t task_id);
	/**
	* @brief check and see if all the tasks were completed.
	*/
	bool finished_tasks(){
		return tasks.empty();
	}
protected:
	std::set<size_t> tasks;
	std::mutex kernel_mutex;
	std::condition_variable kernel_cv;
	std::atomic<std::size_t> total_input_bytes_processed;
	std::atomic<std::size_t> total_input_rows_processed;
	

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
};

}  // namespace cache
}  // namespace ral
