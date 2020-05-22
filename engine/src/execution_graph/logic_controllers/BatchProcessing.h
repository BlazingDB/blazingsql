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


#include "utilities/random_generator.cuh"
//#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/from_cudf/cpp_tests/utilities/column_wrapper.hpp>

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
using frame_type = std::unique_ptr<ral::frame::BlazingTable>;
using Context = blazingdb::manager::Context;
class BatchSequence {
public:
	BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr)
	: cache{cache}, kernel{kernel}
	{}
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
		this->cache = cache;
	}
	RecordBatch next() {
		std::shared_ptr<spdlog::logger> cache_events_logger;
		cache_events_logger = spdlog::get("cache_events_logger");

		CodeTimer cacheEventTimer(false);

		cacheEventTimer.start();
		auto output = cache->pullFromCache();
		cacheEventTimer.stop();

		if(output){
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

	bool wait_for_next() {
		if (kernel) {
			std::string message_id = std::to_string((int)kernel->get_type_id()) + "_" + std::to_string(kernel->get_id());
			// std::cout<<">>>>> WAIT_FOR_NEXT id : " <<  message_id <<std::endl;
		}

		return cache->wait_for_next();
	}

	bool has_next_now() {
		return cache->has_next_now();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache;
	const ral::cache::kernel * kernel;
};

class BatchSequenceBypass {
public:
	BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr)
	: cache{cache}, kernel{kernel}
	{}
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
		this->cache = cache;
	}
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
	// cache->addToRawCache(cache->pullFromRawCache())
	bool wait_for_next() {
		return cache->wait_for_next();
	}

	bool has_next_now() {
		return cache->has_next_now();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache;
	const ral::cache::kernel * kernel;
};

using ral::communication::network::Server;
using ral::communication::network::Client;
using ral::communication::messages::ReceivedHostMessage;

template<class MessageType>
class ExternalBatchColumnDataSequence {
public:
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

	bool wait_for_next() {
		return host_cache->wait_for_next();
	}

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
	std::shared_ptr<Context> context;
	std::shared_ptr<ral::cache::HostCacheMachine> host_cache;
	const ral::cache::kernel * kernel;
	int last_message_counter;
};


class DataSourceSequence {
public:
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

	bool has_next() {
		return (is_empty_data_source && batch_index < 1) || (is_gdf_parser && batch_index.load() < n_batches) || (cur_file_index < n_files);
	}

	void set_projections(std::vector<size_t> projections) {
		this->projections = projections;
	}

	// this function can be called from a parallel thread, so we want it to be thread safe
	size_t get_batch_index() {
		return batch_index.load();
	}

	size_t get_num_batches() {
		return n_batches;
	}

private:
	std::shared_ptr<ral::io::data_provider> provider;
	std::shared_ptr<ral::io::data_parser> parser;

	std::shared_ptr<Context> context;
	std::vector<size_t> projections;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	size_t cur_file_index;
	size_t cur_row_group_index;
	std::vector<std::vector<int>> all_row_groups;
	std::atomic<size_t> batch_index;
	size_t n_batches;
	size_t n_files;
	bool is_empty_data_source;
	bool is_gdf_parser;

	std::mutex mutex_;
};

class TableScan : public kernel {
public:
	TableScan(const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(queryString, context, kernel_type::TableScanKernel), input(loader, schema, context)
	{
		this->query_graph = query_graph;
	}

	bool can_you_throttle_my_input() {
		return false;
	}
	
	virtual kstatus run() {
		CodeTimer timer;

		int table_scan_kernel_num_threads = 4;
		std::map<std::string, std::string> config_options = context->getConfigOptions();
		auto it = config_options.find("TABLE_SCAN_KERNEL_NUM_THREADS");
		if (it != config_options.end()){
			table_scan_kernel_num_threads = std::stoi(config_options["TABLE_SCAN_KERNEL_NUM_THREADS"]);
		}

		std::vector<BlazingThread> threads;
		for (int i = 0; i < table_scan_kernel_num_threads; i++) {
			threads.push_back(BlazingThread([this]() {
				CodeTimer eventTimer(false);
				this->output_cache()->wait_if_cache_is_saturated();

				std::unique_ptr<ral::frame::BlazingTable> batch;
				while(batch = input.next()) {
					eventTimer.start();
					eventTimer.stop();

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

	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		double rows_so_far = (double)this->output_.total_rows_added();
		double num_batches = (double)this->input.get_num_batches();
		double current_batch = (double)this->input.get_batch_index();
		if (current_batch == 0 || num_batches == 0){
			return std::make_pair(false,0);
		} else {
			return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
		}
	}

private:
	DataSourceSequence input;
};

class BindableTableScan : public kernel {
public:
	BindableTableScan(const std::string & queryString, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(queryString, context, kernel_type::BindableTableScanKernel), input(loader, schema, context)
	{
		this->query_graph = query_graph;
	}

	bool can_you_throttle_my_input() {
		return false;
	}

	virtual kstatus run() {
		CodeTimer timer;

		input.set_projections(get_projections(expression));

		int table_scan_kernel_num_threads = 4;
		std::map<std::string, std::string> config_options = context->getConfigOptions();
		auto it = config_options.find("TABLE_SCAN_KERNEL_NUM_THREADS");
		if (it != config_options.end()){
			table_scan_kernel_num_threads = std::stoi(config_options["TABLE_SCAN_KERNEL_NUM_THREADS"]);
		}

		std::vector<BlazingThread> threads;
		for (int i = 0; i < table_scan_kernel_num_threads; i++) {
			threads.push_back(BlazingThread([expression = this->expression, this]() {

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

					} catch(const std::exception& e) {
						// TODO add retry here
						logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
														"query_id"_a=context->getContextToken(),
														"step"_a=context->getQueryStep(),
														"substep"_a=context->getQuerySubstep(),
														"info"_a="In BindableTableScan kernel batch for {}. What: {}"_format(expression, e.what()),
														"duration"_a="");
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

	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		double rows_so_far = (double)this->output_.total_rows_added();
		double num_batches = (double)this->input.get_num_batches();
		double current_batch = (double)this->input.get_batch_index();
		if (current_batch == 0 || num_batches == 0){
			return std::make_pair(false,0);
		} else {
			return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
		}
	}

private:
	DataSourceSequence input;
};

class Projection : public kernel {
public:
	Projection(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(queryString, context, kernel_type::ProjectKernel)
	{
		this->query_graph = query_graph;
	}

	bool can_you_throttle_my_input() {
		return true;
	}

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
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In Projection kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
			}
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="Projection Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};

class Filter : public kernel {
public:
	Filter(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(queryString, context, kernel_type::FilterKernel)
	{
		this->query_graph = query_graph;
	}

	bool can_you_throttle_my_input() {
		return true;
	}

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

class Print : public kernel {
public:
	Print() : kernel("Print", nullptr, kernel_type::PrintKernel) { ofs = &(std::cout); }
	Print(std::ostream & stream) : kernel("Print", nullptr, kernel_type::PrintKernel) { ofs = &stream; }

	bool can_you_throttle_my_input() {
		return false;
	}

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
	std::ostream * ofs = nullptr;
	std::mutex print_lock;
};


class OutputKernel : public kernel {
public:
	OutputKernel(std::shared_ptr<Context> context) : kernel("OutputKernel", context, kernel_type::OutputKernel) { }

	virtual kstatus run() {
		CodeTimer cacheEventTimer(false);

		cacheEventTimer.start();
		output = std::move(this->input_.get_cache()->pullFromCache());
		cacheEventTimer.stop();

		if(output){
			auto num_rows = output->num_rows();
			auto num_bytes = output->sizeInBytes();

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
		}

		return kstatus::stop;
	}

	bool can_you_throttle_my_input() {
		return false;
	}

	frame_type	release() {
		return std::move(output);
	}

protected:
	frame_type output;
};


namespace test {
class generate : public kernel {
public:
	generate(std::int64_t count = 1000) : kernel("", nullptr, kernel_type::GenerateKernel), count(count) {}
	virtual kstatus run() {

		cudf::test::fixed_width_column_wrapper<int32_t> column1{{0, 1, 2, 3, 4, 5}, {1, 1, 1, 1, 1, 1}};

		CudfTableView cudfTableView{{column1} };

		const std::vector<std::string> columnNames{"column1"};
		ral::frame::BlazingTableView blazingTableView{cudfTableView, columnNames};

		std::unique_ptr<ral::frame::BlazingTable> table = ral::generator::generate_sample(blazingTableView, 4);

		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	std::int64_t count;
};
}
using GeneratorKernel = test::generate;


} // namespace batch
} // namespace ral
