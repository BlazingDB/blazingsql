#pragma once

#include <mutex>
#include "CacheMachine.h"
#include "taskflow/graph.h"
#include "io/Schema.h"
#include "io/DataLoader.h"

#include <execution_graph/logic_controllers/LogicPrimitives.h>

namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using namespace fmt::literals;

using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;
using frame_type = std::vector<std::unique_ptr<ral::frame::BlazingTable>>;
using Context = blazingdb::manager::Context;

/**
 * @brief This is the standard data sequencer that just pulls data from an input cache one batch at a time.
 */
class BatchSequence {
public:
	/**
	 * Constructor for the BatchSequence
	 * @param cache The input cache from where the data will be pulled.
	 * @param kernel The kernel that will actually receive the pulled data.
	 * @param ordered Indicates whether the order should be kept at data pulling.
	 */
	BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr, bool ordered = true);

	/**
	 * Updates the input cache machine.
	 * @param cache The pointer to the new input cache.
	 */
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache);

	/**
	 * Get the next message as a unique pointer to a BlazingTable.
	 * If there are no more messages on the queue we get a nullptr.
	 * @return Unique pointer to a BlazingTable containing the next decached message.
	 */
	RecordBatch next();

	/**
	 * Blocks executing thread until a new message is ready or when the message queue is empty.
	 * @return true A new message is ready.
	 * @return false There are no more messages on the cache.
	 */
	bool wait_for_next();

	/**
	 * Indicates if the message queue is not empty at this point on time.
	 * @return true There is at least one message in the queue.
	 * @return false Message queue is empty.
	 */
	bool has_next_now();

private:
	std::shared_ptr<ral::cache::CacheMachine> cache; /**< Cache machine from which the data will be pulled. */
	const ral::cache::kernel * kernel; /**< Pointer to the kernel that will receive the cache data. */
	bool ordered; /**< Indicates whether the order should be kept when pulling data from the cache. */
};

/**
 * @brief This data sequencer works as a bypass to take data from one input to an output without decacheing.
 */
class BatchSequenceBypass {
public:
	/**
	 * Constructor for the BatchSequenceBypass
	 * @param cache The input cache from where the data will be pulled.
	 * @param kernel The kernel that will actually receive the pulled data.
	 */
	BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr);

	/**
	 * Updates the input cache machine.
	 * @param cache The pointer to the new input cache.
	 */
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache);

	/**
	 * Get the next message as a CacheData object.
	 * @return CacheData containing the next message without decacheing.
	 */
	std::unique_ptr<ral::cache::CacheData> next();

	/**
	 * Blocks executing thread until a new message is ready or when the message queue is empty.
	 * @return true A new message is ready.
	 * @return false There are no more messages on the cache.
	 */
	bool wait_for_next();

	/**
	 * Indicates if the message queue is not empty at this point on time.
	 * @return true There is at least one message in the queue.
	 * @return false Message queue is empty.
	 */
	bool has_next_now();

private:
	std::shared_ptr<ral::cache::CacheMachine> cache; /**< Cache machine from which the data will be pulled. */
	const ral::cache::kernel * kernel; /**< Pointer to the kernel that will receive the cache data. */
};


/**
 * @brief Gets data from a data source, such as a set of files or from a DataFrame.
 * These data sequencers are used by the TableScan's.
 */
class DataSourceSequence {
public:
	/**
	 * Constructor for the DataSourceSequence
	 * @param loader Data loader responsible for executing the batching load.
	 * @param schema Table schema associated to the data to be loaded.
	 * @param context Shared context associated to the running query.
	 */
	DataSourceSequence(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context);

	/**
	 * Get the next batch as a unique pointer to a BlazingTable.
	 * If there are no more batches we get a nullptr.
	 * @return Unique pointer to a BlazingTable containing the next batch read.
	 */
	RecordBatch next();

	/**
	 * Indicates if there are more batches to process.
	 * @return true There is at least one batch to be processed.
	 * @return false The data source is empty or all batches have already been processed.
	 */
	bool has_next();

	/**
	 * Updates the set of columns to be projected at the time of reading the data source.
	 * @param projections The set of column ids to be selected.
	 */
	void set_projections(std::vector<int> projections);

	/**
	 * Get the batch index.
	 * @note This function can be called from a parallel thread, so we want it to be thread safe.
	 * @return The current batch index.
	 */
	size_t get_batch_index();

	/**
	 * Get the number of batches identified on the data source.
	 * @return The number of batches.
	 */
	size_t get_num_batches();

private:
	std::shared_ptr<ral::io::data_provider> provider; /**< Data provider associated to the data loader. */
	std::shared_ptr<ral::io::data_parser> parser; /**< Data parser associated to the data loader. */

	std::shared_ptr<Context> context; /**< Pointer to the shared query context. */
	std::vector<int> projections; /**< List of columns that will be selected if they were previously settled. */
	ral::io::data_loader loader; /**< Data loader responsible for executing the batching load. */
	ral::io::Schema  schema; /**< Table schema associated to the data to be loaded. */
	std::vector<std::vector<int>> all_row_groups;
	std::atomic<size_t> batch_index; /**< Current global batch index. */
	size_t n_batches; /**< Number of batches. */
	size_t n_files; /**< Number of files. */
	bool is_empty_data_source; /**< Indicates whether the data source is empty. */
	bool is_gdf_parser; /**< Indicates whether the parser is a gdf one. */
	int cur_file_index{}; /**< Current file index. */
	int file_batch_index{};  /**< Current file batch index. */
	ral::io::data_handle current_data_handle{};

	std::mutex mutex_; /**< Mutex for making the loading batch thread-safe. */
};

/**
 * @brief This kernel loads the data from the specified data source.
 */
class TableScan : public kernel {
public:
	/**
	 * Constructor for TableScan
	 * @param kernel_id Kernel identifier.
	 * @param queryString Original logical expression that the kernel will execute.
	 * @param loader Data loader responsible for executing the batching load.
	 * @param schema Table schema associated to the data to be loaded.
	 * @param context Shared context associated to the running query.
	 * @param query_graph Shared pointer of the current execution graph.
	 */
	TableScan(std::size_t kernel_id, const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	
	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run();

	/**
	 * Returns the estimated num_rows for the output at one point.
	 * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
	 */
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows();

private:
	DataSourceSequence input; /**< Input data source sequence. */
};

/**
 * @brief This kernel loads the data and delivers only columns that are requested.
 * It also filters the data if there are one or more filters, and sets their column aliases
 * accordingly.
 */
class BindableTableScan : public kernel {
public:
	/**
	 * Constructor for BindableTableScan
	 * @param kernel_id Kernel identifier.
	 * @param queryString Original logical expression that the kernel will execute.
	 * @param loader Data loader responsible for executing the batching load.
	 * @param schema Table schema associated to the data to be loaded.
	 * @param context Shared context associated to the running query.
	 * @param query_graph Shared pointer of the current execution graph.
	 */
	BindableTableScan(std::size_t kernel_id, const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run();

	/**
	 * Returns the estimated num_rows for the output at one point.
	 * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
	 */
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows();

private:
	DataSourceSequence input; /**< Input data source sequence. */
};

/**
 * @brief This kernel only returns the subset columns contained in the logical projection expression.
 */
class Projection : public kernel {
public:
	/**
	 * Constructor for Projection
	 * @param kernel_id Kernel identifier.
	 * @param queryString Original logical expression that the kernel will execute.
	 * @param context Shared context associated to the running query.
	 * @param query_graph Shared pointer of the current execution graph.
	 */
	Projection(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run();
};

/**
 * @brief This kernel filters the data according to the specified conditions.
 */
class Filter : public kernel {
public:
	/**
	 * Constructor for TableScan
	 * @param kernel_id Kernel identifier.
	 * @param queryString Original logical expression that the kernel will execute.
	 * @param context Shared context associated to the running query.
	 * @param query_graph Shared pointer of the current execution graph.
	 */
	Filter(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run();

	/**
	 * Returns the estimated num_rows for the output at one point.
	 * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
	 */
	std::pair<bool, uint64_t> get_estimated_output_num_rows();
};

/**
 * @brief This kernel allows printing the preceding input caches to the standard output.
 */
class Print : public kernel {
public:
	/**
	 * Constructor
	 */
	Print() : kernel(0,"Print", nullptr, kernel_type::PrintKernel) { ofs = &(std::cout); }
	Print(std::ostream & stream) : kernel(0,"Print", nullptr, kernel_type::PrintKernel) { ofs = &stream; }

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run();

protected:
	std::ostream * ofs = nullptr; /**< Target output stream object. */
	std::mutex print_lock; /**< Mutex for making the printing thread-safe. */
};


/**
 * @brief This kernel represents the last step of the execution graph.
 * Basically it allows to extract the result of the different levels of
 * memory abstractions in the form of a concrete table.
 */
class OutputKernel : public kernel {
public:
	/**
	 * Constructor for OutputKernel
	 * @param kernel_id Kernel identifier.
	 * @param context Shared context associated to the running query.
	 */
	OutputKernel(std::size_t kernel_id, std::shared_ptr<Context> context) : kernel(kernel_id,"OutputKernel", context, kernel_type::OutputKernel) { }

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run();

	/**
	 * Returns the vector containing the final processed output.
	 * @return frame_type A vector of unique_ptr of BlazingTables.
	 */
	frame_type release();

protected:
	frame_type output; /**< Vector of tables with the final output. */
};

} // namespace batch
} // namespace ral
