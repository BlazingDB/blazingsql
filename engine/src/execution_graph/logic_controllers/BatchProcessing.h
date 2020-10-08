#pragma once

#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/network/Server.h"
#include <src/communication/network/Client.h>
#include "parser/expression_utils.hpp"

#include <cudf/types.hpp>
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>

#include <src/operators/OrderBy.h>
#include <src/operators/GroupBy.h>
#include <src/utilities/DebuggingUtils.h>
#include <stack>
#include <mutex>
#include "io/DataLoader.h"
#include "io/Schema.h"
#include <Util/StringUtil.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include "parser/expression_utils.hpp"

#include <cudf/copying.hpp>
#include <cudf/merge.hpp>
#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include <src/CalciteInterpreter.h>
#include <src/utilities/CommonOperations.h>

#include "distribution/primitives.h"
#include "config/GPUManager.cuh"
#include "CacheMachine.h"
#include "blazingdb/concurrency/BlazingThread.h"

#include "taskflow/graph.h"
#include "communication/CommunicationData.h"

#include "CodeTimer.h"

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
	BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr, bool ordered = true)
	: cache{cache}, kernel{kernel}, ordered{ordered}
	{}

	/**
	 * Updates the input cache machine.
	 * @param cache The pointer to the new input cache.
	 */
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
		this->cache = cache;
	}

	/**
	 * Get the next message as a unique pointer to a BlazingTable.
	 * If there are no more messages on the queue we get a nullptr.
	 * @return Unique pointer to a BlazingTable containing the next decached message.
	 */
	RecordBatch next() {
		std::shared_ptr<spdlog::logger> cache_events_logger;
		cache_events_logger = spdlog::get("cache_events_logger");

		CodeTimer cacheEventTimer(false);

		cacheEventTimer.start();
		std::unique_ptr<ral::frame::BlazingTable> output;
		if (ordered) {
			output = cache->pullFromCache();
		} else {
			output = cache->pullUnorderedFromCache();
		}
		cacheEventTimer.stop();

		if(output){
			auto num_rows = output->num_rows();
			auto num_bytes = output->sizeInBytes();

			if(cache_events_logger != nullptr) {
				cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
							"ral_id"_a=cache->get_context()->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
							"query_id"_a=cache->get_context()->getContextToken(),
							"source"_a=cache->get_id(),
							"sink"_a=kernel->get_id(),
							"num_rows"_a=num_rows,
							"num_bytes"_a=num_bytes,
							"event_type"_a="removeCache",
							"timestamp_begin"_a=cacheEventTimer.start_time(),
							"timestamp_end"_a=cacheEventTimer.end_time());
			}
		}

		return output;
	}

	/**
	 * Blocks executing thread until a new message is ready or when the message queue is empty.
	 * @return true A new message is ready.
	 * @return false There are no more messages on the cache.
	 */
	bool wait_for_next() {
		if (kernel) {
			std::string message_id = std::to_string((int)kernel->get_type_id()) + "_" + std::to_string(kernel->get_id());
		}

		return cache->wait_for_next();
	}

	/**
	 * Indicates if the message queue is not empty at this point on time.
	 * @return true There is at least one message in the queue.
	 * @return false Message queue is empty.
	 */
	bool has_next_now() {
		return cache->has_next_now();
	}
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
	BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr)
	: cache{cache}, kernel{kernel}
	{}

	/**
	 * Updates the input cache machine.
	 * @param cache The pointer to the new input cache.
	 */
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
		this->cache = cache;
	}

	/**
	 * Get the next message as a CacheData object.
	 * @return CacheData containing the next message without decacheing.
	 */
	std::unique_ptr<ral::cache::CacheData> next() {
		std::shared_ptr<spdlog::logger> cache_events_logger;
		cache_events_logger = spdlog::get("cache_events_logger");

		CodeTimer cacheEventTimer(false);

		cacheEventTimer.start();
		auto output = cache->pullCacheData();
		cacheEventTimer.stop();

		if (output) {
			auto num_rows = output->num_rows();
			auto num_bytes = output->sizeInBytes();

			cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
							"ral_id"_a=cache->get_context()->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
							"query_id"_a=cache->get_context()->getContextToken(),
							"source"_a=cache->get_id(),
							"sink"_a=kernel->get_id(),
							"num_rows"_a=num_rows,
							"num_bytes"_a=num_bytes,
							"event_type"_a="removeCache",
							"timestamp_begin"_a=cacheEventTimer.start_time(),
							"timestamp_end"_a=cacheEventTimer.end_time());
		}

		return output;
	}

	/**
	 * Blocks executing thread until a new message is ready or when the message queue is empty.
	 * @return true A new message is ready.
	 * @return false There are no more messages on the cache.
	 */
	bool wait_for_next() {
		return cache->wait_for_next();
	}

	/**
	 * Indicates if the message queue is not empty at this point on time.
	 * @return true There is at least one message in the queue.
	 * @return false Message queue is empty.
	 */
	bool has_next_now() {
		return cache->has_next_now();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache; /**< Cache machine from which the data will be pulled. */
	const ral::cache::kernel * kernel; /**< Pointer to the kernel that will receive the cache data. */
};

using ral::communication::network::Server;
using ral::communication::network::Client;
using ral::communication::messages::ReceivedHostMessage;

/**
 * @brief Connects a HostCacheMachine to a server receiving certain types of messages,
 * so that basically the data sequencer is effectively iterating through batches
 * received from another node via out communication layer.
 */
template<class MessageType>
class ExternalBatchColumnDataSequence {
public:
	/**
	 * Constructor for the ExternalBatchColumnDataSequence
	 * @param context Shared context associated to the running query.
	 * @param message_id Message identifier which will be associated with the underlying cache.
	 * @param kernel The kernel that will actually receive the pulled data.
	 */
	ExternalBatchColumnDataSequence(std::shared_ptr<Context> context, const std::string & message_id, const ral::cache::kernel * kernel = nullptr)
		: context{context}, last_message_counter{context->getTotalNodes() - 1}, kernel{kernel}
	{
		host_cache = std::make_shared<ral::cache::HostCacheMachine>(context, 0); //todo assing right id
		std::string context_comm_token = context->getContextCommunicationToken();
		const uint32_t context_token = context->getContextToken();
		std::string comms_message_token = MessageType::MessageID() + "_" + context_comm_token;

		BlazingMutableThread t([this, comms_message_token, context_token, message_id](){
			while(true){
					auto message = Server::getInstance().getHostMessage(context_token, comms_message_token);
					if(!message) {
						--last_message_counter;
						if (last_message_counter == 0 ){
							this->host_cache->finish();
							break;
						}
					}	else{
						auto concreteMessage = std::static_pointer_cast<ReceivedHostMessage>(message);
						assert(concreteMessage != nullptr);
						auto host_table = concreteMessage->releaseBlazingHostTable();
						host_table->setPartitionId(concreteMessage->getPartitionId());
						this->host_cache->addToCache(std::move(host_table), message_id);
					}
			}
		});
		t.detach();
	}

	/**
	 * Blocks executing thread until a new message is ready or when the message queue is empty.
	 * @return true A new message is ready.
	 * @return false There are no more messages on the host cache.
	 */
	bool wait_for_next() {
		return host_cache->wait_for_next();
	}

	/**
	 * Get the next message as a BlazingHostTable.
	 * @return BlazingHostTable containing the next released message.
	 */
	std::unique_ptr<ral::frame::BlazingHostTable> next() {
		std::shared_ptr<spdlog::logger> cache_events_logger;
		cache_events_logger = spdlog::get("cache_events_logger");

		CodeTimer cacheEventTimer(false);

		cacheEventTimer.start();
		auto output = host_cache->pullFromCache(context.get());
		cacheEventTimer.stop();

		if(output){
			auto num_rows = output->num_rows();
			auto num_bytes = output->sizeInBytes();

			cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
							"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
							"query_id"_a=context->getContextToken(),
							"source"_a=host_cache->get_id(),
							"sink"_a=kernel->get_id(),
							"num_rows"_a=num_rows,
							"num_bytes"_a=num_bytes,
							"event_type"_a="removeCache",
							"timestamp_begin"_a=cacheEventTimer.start_time(),
							"timestamp_end"_a=cacheEventTimer.end_time());
		}

		return output;
	}
private:
	std::shared_ptr<Context> context; /**< Pointer to the shared query context. */
	std::shared_ptr<ral::cache::HostCacheMachine> host_cache; /**< Host cache machine from which the data will be pulled. */
	const ral::cache::kernel * kernel; /**< Pointer to the kernel that will receive the cache data. */
	int last_message_counter; /**< Allows to stop waiting for messages keeping track of the last message received from each other node. */
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
	DataSourceSequence(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
		: context(context), loader(loader), schema(schema), batch_index{0}, cur_file_index{0}, cur_row_group_index{0}, n_batches{0}
	{
		// n_partitions{n_partitions}: TODO Update n_batches using data_loader
		this->provider = loader.get_provider();
		this->parser = loader.get_parser();

		n_files = schema.get_files().size();
		for (size_t index = 0; index < n_files; index++) {
			all_row_groups.push_back(schema.get_rowgroup_ids(index));
		}

		is_empty_data_source = (n_files == 0 && parser->get_num_partitions() == 0);
		is_gdf_parser = parser->get_num_partitions() > 0;
		if(is_gdf_parser){
			n_batches = parser->get_num_partitions();
		} else {
			n_batches = n_files;
		}
	}

	/**
	 * Get the next batch as a unique pointer to a BlazingTable.
	 * If there are no more batches we get a nullptr.
	 * @return Unique pointer to a BlazingTable containing the next batch read.
	 */
	RecordBatch next() {
		std::unique_lock<std::mutex> lock(mutex_);

		if (!has_next()) {
			return nullptr;
		}

		if (is_empty_data_source) {
			batch_index++;
			return schema.makeEmptyBlazingTable(projections);
		}

		if(is_gdf_parser){
			auto ret = loader.load_batch(context.get(), projections, schema, ral::io::data_handle(), 0, std::vector<cudf::size_type>(1, cur_row_group_index));
			batch_index++;
			cur_row_group_index++;

			return std::move(ret);
		}

		// a file handle that we can use in case errors occur to tell the user which file had parsing issues
		assert(this->provider->has_next());

		auto local_cur_data_handle = this->provider->get_next();
		auto local_cur_file_index = cur_file_index;
		auto local_all_row_groups = this->all_row_groups[cur_file_index];

		batch_index++;
		cur_file_index++;

		lock.unlock();

		auto ret = loader.load_batch(context.get(), projections, schema, local_cur_data_handle, local_cur_file_index, local_all_row_groups);
		return std::move(ret);
	}

	/**
	 * Indicates if there are more batches to process.
	 * @return true There is at least one batch to be processed.
	 * @return false The data source is empty or all batches have already been processed.
	 */
	bool has_next() {
		return (is_empty_data_source && batch_index < 1) || (is_gdf_parser && batch_index.load() < n_batches) || (cur_file_index < n_files);
	}

	/**
	 * Updates the set of columns to be projected at the time of reading the data source.
	 * @param projections The set of column ids to be selected.
	 */
	void set_projections(std::vector<int> projections) {
		this->projections = projections;
	}

	/**
	 * Get the batch index.
	 * @note This function can be called from a parallel thread, so we want it to be thread safe.
	 * @return The current batch index.
	 */
	size_t get_batch_index() {
		return batch_index.load();
	}

	/**
	 * Get the number of batches identified on the data source.
	 * @return The number of batches.
	 */
	size_t get_num_batches() {
		return n_batches;
	}

private:
	std::shared_ptr<ral::io::data_provider> provider; /**< Data provider associated to the data loader. */
	std::shared_ptr<ral::io::data_parser> parser; /**< Data parser associated to the data loader. */

	std::shared_ptr<Context> context; /**< Pointer to the shared query context. */
	std::vector<int> projections; /**< List of columns that will be selected if they were previously settled. */
	ral::io::data_loader loader; /**< Data loader responsible for executing the batching load. */
	ral::io::Schema  schema; /**< Table schema associated to the data to be loaded. */
	size_t cur_file_index; /**< Current file index. */
	size_t cur_row_group_index; /**< Current rowgroup index. */
	std::vector<std::vector<int>> all_row_groups;
	std::atomic<size_t> batch_index; /**< Current batch index. */
	size_t n_batches; /**< Number of batches. */
	size_t n_files; /**< Number of files. */
	bool is_empty_data_source; /**< Indicates whether the data source is empty. */
	bool is_gdf_parser; /**< Indicates whether the parser is a gdf one. */

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
	TableScan(std::size_t kernel_id, const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(kernel_id, queryString, context, kernel_type::TableScanKernel), input(loader, schema, context)
	{
		this->query_graph = query_graph;
	}

	/**
	 * Indicates whether the cache load can be throttling.
	 * @return true If the pace of cache loading may be throttled.
	 * @return false If cache should be loaded according to the default pace.
	 */
	bool can_you_throttle_my_input() {
		return false;
	}

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run() {
		CodeTimer timer;

		int table_scan_kernel_num_threads = 4;
		std::map<std::string, std::string> config_options = context->getConfigOptions();
		auto it = config_options.find("TABLE_SCAN_KERNEL_NUM_THREADS");
		if (it != config_options.end()){
			table_scan_kernel_num_threads = std::stoi(config_options["TABLE_SCAN_KERNEL_NUM_THREADS"]);
		}
		bool has_limit = this->has_limit_;
		size_t limit_ = this->limit_rows_;

		// want to read only one file at a time to avoid OOM when `select * from table limit N`
		if (has_limit) {
			table_scan_kernel_num_threads = 1;
		}

		cudf::size_type current_rows = 0;
		std::vector<BlazingThread> threads;
		for (int i = 0; i < table_scan_kernel_num_threads; i++) {
			threads.push_back(BlazingThread([this, &has_limit, &limit_, &current_rows]() {
				CodeTimer eventTimer(false);

				this->output_cache()->wait_if_cache_is_saturated();

				std::unique_ptr<ral::frame::BlazingTable> batch;
				while(batch = input.next()) {
					eventTimer.start();
					eventTimer.stop();
					current_rows += batch->num_rows();

					events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
									"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
									"query_id"_a=context->getContextToken(),
									"kernel_id"_a=this->get_id(),
									"input_num_rows"_a=batch->num_rows(),
									"input_num_bytes"_a=batch->sizeInBytes(),
									"output_num_rows"_a=batch->num_rows(),
									"output_num_bytes"_a=batch->sizeInBytes(),
									"event_type"_a="compute",
									"timestamp_begin"_a=eventTimer.start_time(),
									"timestamp_end"_a=eventTimer.end_time());

					this->add_to_output_cache(std::move(batch));
					this->output_cache()->wait_if_cache_is_saturated();

					if (has_limit && current_rows >= limit_) {
						break;
					}
				}
			}));
		}
		for (auto &&t : threads) {
			t.join();
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="TableScan Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

	/**
	 * Returns the estimated num_rows for the output at one point.
	 * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
	 */
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		double rows_so_far = (double)this->output_.total_rows_added();
		double num_batches = (double)this->input.get_num_batches();
		double current_batch = (double)this->input.get_batch_index();
		if (current_batch == 0 || num_batches == 0){
			return std::make_pair(false, 0);
		} else {
			return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
		}
	}

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
		std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(kernel_id, queryString, context, kernel_type::BindableTableScanKernel), input(loader, schema, context)
	{
		this->query_graph = query_graph;
	}

	/**
	 * Indicates whether the cache load can be throttling.
	 * @return true If the pace of cache loading may be throttled.
	 * @return false If cache should be loaded according to the default pace.
	 */
	bool can_you_throttle_my_input() {
		return false;
	}

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run() {
		CodeTimer timer;

		input.set_projections(get_projections(expression));

		int table_scan_kernel_num_threads = 4;
		std::map<std::string, std::string> config_options = context->getConfigOptions();
		auto it = config_options.find("TABLE_SCAN_KERNEL_NUM_THREADS");
		if (it != config_options.end()){
			table_scan_kernel_num_threads = std::stoi(config_options["TABLE_SCAN_KERNEL_NUM_THREADS"]);
		}

		bool has_limit = this->has_limit_;
		size_t limit_ = this->limit_rows_;
		cudf::size_type current_rows = 0;
		std::vector<BlazingThread> threads;
		for (int i = 0; i < table_scan_kernel_num_threads; i++) {
			threads.push_back(BlazingThread([expression = this->expression, &limit_, &has_limit, &current_rows, this]() {

				CodeTimer eventTimer(false);

				this->output_cache()->wait_if_cache_is_saturated();
				std::unique_ptr<ral::frame::BlazingTable> batch;

				while(batch = input.next()) {
					try {
						eventTimer.start();
						auto log_input_num_rows = batch->num_rows();
						auto log_input_num_bytes = batch->sizeInBytes();

						if(is_filtered_bindable_scan(expression)) {
							auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, this->context.get());
							current_rows += columns->num_rows();
							columns->setNames(fix_column_aliases(columns->names(), expression));
							eventTimer.stop();

							if( columns ) {
								auto log_output_num_rows = columns->num_rows();
								auto log_output_num_bytes = columns->sizeInBytes();

								events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
												"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
												"query_id"_a=context->getContextToken(),
												"kernel_id"_a=this->get_id(),
												"input_num_rows"_a=log_input_num_rows,
												"input_num_bytes"_a=log_input_num_bytes,
												"output_num_rows"_a=log_output_num_rows,
												"output_num_bytes"_a=log_output_num_bytes,
												"event_type"_a="compute",
												"timestamp_begin"_a=eventTimer.start_time(),
												"timestamp_end"_a=eventTimer.end_time());
							}

							this->add_to_output_cache(std::move(columns));
						}
						else{
							current_rows += batch->num_rows();
							batch->setNames(fix_column_aliases(batch->names(), expression));

							auto log_output_num_rows = batch->num_rows();
							auto log_output_num_bytes = batch->sizeInBytes();
							eventTimer.stop();

							events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
											"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
											"query_id"_a=context->getContextToken(),
											"kernel_id"_a=this->get_id(),
											"input_num_rows"_a=log_input_num_rows,
											"input_num_bytes"_a=log_input_num_bytes,
											"output_num_rows"_a=log_output_num_rows,
											"output_num_bytes"_a=log_output_num_bytes,
											"event_type"_a="compute",
											"timestamp_begin"_a=eventTimer.start_time(),
											"timestamp_end"_a=eventTimer.end_time());

							this->add_to_output_cache(std::move(batch));
						}

						this->output_cache()->wait_if_cache_is_saturated();

						// useful when the Algebra Relacional only contains: BindableTableScan and LogicalLimit
						if (has_limit && current_rows >= limit_) {
							break;
						}

					} catch(const std::exception& e) {
						// TODO add retry here
						logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
														"query_id"_a=context->getContextToken(),
														"step"_a=context->getQueryStep(),
														"substep"_a=context->getQuerySubstep(),
														"info"_a="In BindableTableScan kernel batch for {}. What: {}"_format(expression, e.what()),
														"duration"_a="");
						throw;
					}
				}
			}));
		}
		for (auto &&t : threads) {
			t.join();
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="BindableTableScan Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

	/**
	 * Returns the estimated num_rows for the output at one point.
	 * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
	 */
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		double rows_so_far = (double)this->output_.total_rows_added();
		double num_batches = (double)this->input.get_num_batches();
		double current_batch = (double)this->input.get_batch_index();
		if (current_batch == 0 || num_batches == 0){
			return std::make_pair(false, 0);
		} else {
			return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
		}
	}

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
	Projection(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(kernel_id, queryString, context, kernel_type::ProjectKernel)
	{
		this->query_graph = query_graph;
	}

	/**
	 * Indicates whether the cache load can be throttling.
	 * @return true If the pace of cache loading may be throttled.
	 * @return false If cache should be loaded according to the default pace.
	 */
	bool can_you_throttle_my_input() {
		return true;
	}

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run() {
		CodeTimer timer;
		CodeTimer eventTimer(false);

		BatchSequence input(this->input_cache(), this);
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				this->output_cache()->wait_if_cache_is_saturated();

				auto batch = input.next();

				auto log_input_num_rows = batch ? batch->num_rows() : 0;
				auto log_input_num_bytes = batch ? batch->sizeInBytes() : 0;

				eventTimer.start();
				auto columns = ral::processor::process_project(std::move(batch), expression, context.get());
				eventTimer.stop();

				if(columns){
					auto log_output_num_rows = columns->num_rows();
					auto log_output_num_bytes = columns->sizeInBytes();
					if(events_logger != nullptr) {
						events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
									"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
									"query_id"_a=context->getContextToken(),
									"kernel_id"_a=this->get_id(),
									"input_num_rows"_a=log_input_num_rows,
									"input_num_bytes"_a=log_input_num_bytes,
									"output_num_rows"_a=log_output_num_rows,
									"output_num_bytes"_a=log_output_num_bytes,
									"event_type"_a="compute",
									"timestamp_begin"_a=eventTimer.start_time(),
									"timestamp_end"_a=eventTimer.end_time());
					}
				}

				this->add_to_output_cache(std::move(columns));
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				if(logger != nullptr) {
					logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In Projection kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
				}
				throw;
			}
		}

		if(logger != nullptr) {
			logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="Projection Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());
		}
		return kstatus::proceed;
	}

private:

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
	Filter(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(kernel_id, queryString, context, kernel_type::FilterKernel)
	{
		this->query_graph = query_graph;
	}

	/**
	 * Indicates whether the cache load can be throttling.
	 * @return true If the pace of cache loading may be throttled.
	 * @return false If cache should be loaded according to the default pace.
	 */
	bool can_you_throttle_my_input() {
		return true;
	}

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run() {
		CodeTimer timer;
		CodeTimer eventTimer(false);

		BatchSequence input(this->input_cache(), this);
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				this->output_cache()->wait_if_cache_is_saturated();

				auto batch = input.next();

				auto log_input_num_rows = batch->num_rows();
				auto log_input_num_bytes = batch->sizeInBytes();

				eventTimer.start();
				auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
				eventTimer.stop();

				auto log_output_num_rows = columns->num_rows();
				auto log_output_num_bytes = columns->sizeInBytes();

				events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
								"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
								"query_id"_a=context->getContextToken(),
								"kernel_id"_a=this->get_id(),
								"input_num_rows"_a=log_input_num_rows,
								"input_num_bytes"_a=log_input_num_bytes,
								"output_num_rows"_a=log_output_num_rows,
								"output_num_bytes"_a=log_output_num_bytes,
								"event_type"_a="compute",
								"timestamp_begin"_a=eventTimer.start_time(),
								"timestamp_end"_a=eventTimer.end_time());

				this->add_to_output_cache(std::move(columns));
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In Filter kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
				throw;
			}
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="Filter Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

	/**
	 * Returns the estimated num_rows for the output at one point.
	 * @return A pair representing that there is no data to be processed, or the estimated number of output rows.
	 */
	std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
		if (total_in.first){
			double out_so_far = (double)this->output_.total_rows_added();
			double in_so_far = (double)this->input_.total_rows_added();
			if (in_so_far == 0){
				return std::make_pair(false, 0);
			} else {
				return std::make_pair(true, (uint64_t)( ((double)total_in.second) *out_so_far/in_so_far) );
			}
		} else {
			return std::make_pair(false, 0);
		}
    }

private:

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
	 * Indicates whether the cache load can be throttling.
	 * @return true If the pace of cache loading may be throttled.
	 * @return false If cache should be loaded according to the default pace.
	 */
	bool can_you_throttle_my_input() {
		return false;
	}

	/**
	 * Executes the batch processing.
	 * Loads the data from their input port, and after processing it,
	 * the results are stored in their output port.
	 * @return kstatus 'stop' to halt processing, or 'proceed' to continue processing.
	 */
	virtual kstatus run() {
		std::lock_guard<std::mutex> lg(print_lock);
		BatchSequence input(this->input_cache(), this);
		while (input.wait_for_next() ) {
			auto batch = input.next();
			ral::utilities::print_blazing_table_view(batch->toBlazingTableView());
		}
		return kstatus::stop;
	}

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
	virtual kstatus run() {
		while (this->input_.get_cache()->wait_for_next()) {
			CodeTimer cacheEventTimer(false);

			cacheEventTimer.start();
			auto temp_output = std::move(this->input_.get_cache()->pullFromCache());
			cacheEventTimer.stop();

			if(temp_output){
				auto num_rows = temp_output->num_rows();
				auto num_bytes = temp_output->sizeInBytes();

				cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
								"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
								"query_id"_a=context->getContextToken(),
								"source"_a=this->input_.get_cache()->get_id(),
								"sink"_a=this->get_id(),
								"num_rows"_a=num_rows,
								"num_bytes"_a=num_bytes,
								"event_type"_a="removeCache",
								"timestamp_begin"_a=cacheEventTimer.start_time(),
								"timestamp_end"_a=cacheEventTimer.end_time());

				output.emplace_back(std::move(temp_output));
			}
		}

		return kstatus::stop;
	}

	/**
	 * Indicates whether the cache load can be throttling.
	 * @return true If the pace of cache loading may be throttled.
	 * @return false If cache should be loaded according to the default pace.
	 */
	bool can_you_throttle_my_input() {
		return false;
	}

	/**
	 * Returns the vector containing the final processed output.
	 * @return frame_type A vector of unique_ptr of BlazingTables.
	 */
	frame_type release() {
		return std::move(output);
	}

protected:
	frame_type output; /**< Vector of tables with the final output. */
};

} // namespace batch
} // namespace ral
