#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <typeindex>
#include <vector>
#include <limits>
#include <map>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <cudf/io/functions.hpp>

#include "error.hpp"
#include "CodeTimer.h"
#include <blazingdb/manager/Context.h>
#include <communication/messages/GPUComponentMessage.h>
#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "execution_graph/logic_controllers/BlazingColumnOwner.h"
#include "execution_graph/logic_controllers/BlazingColumnView.h"
#include <bmr/BlazingMemoryResource.h>
#include "communication/CommunicationData.h"
#include <exception>


using namespace std::chrono_literals;

namespace ral {
namespace cache {

using Context = blazingdb::manager::Context;
using namespace fmt::literals;

/// \brief An enum type to represent the cache level ID
enum class CacheDataType { GPU, CPU, LOCAL_FILE, GPU_METADATA };

const std::string KERNEL_ID_METADATA_LABEL = "kernel_id";
const std::string QUERY_ID_METADATA_LABEL = "query_id";
const std::string CACHE_ID_METADATA_LABEL = "cache_id";
const std::string ADD_TO_SPECIFIC_CACHE_METADATA_LABEL = "add_to_specific_cache";
const std::string SENDER_WORKER_ID_METADATA_LABEL = "sender_worker_id";
const std::string WORKER_IDS_METADATA_LABEL = "worker_ids";
const std::string TOTAL_TABLE_ROWS_METADATA_LABEL = "total_table_rows";
const std::string JOIN_LEFT_BYTES_METADATA_LABEL = "join_left_bytes_metadata_label";
const std::string JOIN_RIGHT_BYTES_METADATA_LABEL = "join_right_bytes_metadata_label";
const std::string MESSAGE_ID = "message_id";
const std::string PARTITION_COUNT = "partition_count";

/// \brief An interface which represent a CacheData
class CacheData {
public:
	CacheData(CacheDataType cache_type, std::vector<std::string> col_names, std::vector<cudf::data_type> schema, size_t n_rows)
		: cache_type(cache_type), col_names(col_names), schema(schema), n_rows(n_rows)
	{
	}
	virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;

	virtual size_t sizeInBytes() const = 0;

	virtual ~CacheData() {}

	std::vector<std::string> names() const {
		return col_names;
	}
	std::vector<cudf::data_type> get_schema() const {
		return schema;
	}
	size_t num_rows() const {
		return n_rows;
	}
	CacheDataType get_type() const {
		return cache_type;
	}

protected:
	CacheDataType cache_type;
	std::vector<std::string> col_names;
	std::vector<cudf::data_type> schema;
	size_t n_rows;
};

class MetadataDictionary{
public:
	void add_value(std::string key, std::string value){
		this->values[key] = value;
	}

	void add_value(std::string key, int value){
		this->values[key] = std::to_string(value);
	}


	int get_kernel_id(){
		if( this->values.find(KERNEL_ID_METADATA_LABEL) == this->values.end()){
			throw BlazingMissingMetadataException(KERNEL_ID_METADATA_LABEL);
		}
		return std::stoi(values[KERNEL_ID_METADATA_LABEL]);
	}

	
	std::map<std::string,std::string> get_values(){
		return this->values;
	}

	void set_values(std::map<std::string,std::string> new_values){
		this->values= new_values;
	}
private:
	std::map<std::string,std::string> values;
};

/// \brief A specific class for a CacheData on GPU Memory
class GPUCacheData : public CacheData {
public:
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table)
		: CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}

		GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, CacheDataType cache_type_override)
			: CacheData(cache_type_override,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}


	std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }

	size_t sizeInBytes() const override { return data->sizeInBytes(); }

	virtual ~GPUCacheData() {}

	ral::frame::BlazingTableView getTableView(){
		return this->data->toBlazingTableView();
	}

protected:
	std::unique_ptr<ral::frame::BlazingTable> data;
};

class GPUCacheDataMetaData : public GPUCacheData {
public:
	GPUCacheDataMetaData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
		: GPUCacheData(std::move(table), CacheDataType::GPU_METADATA),
		metadata(metadata)
		 { }

	std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }

	std::pair<std::unique_ptr<ral::frame::BlazingTable>,MetadataDictionary > decacheWithMetaData(){
		 return std::make_pair(std::move(data),this->metadata); }

	size_t sizeInBytes() const override { return data->sizeInBytes(); }

	MetadataDictionary getMetadata(){
		return this->metadata;
	}

	virtual ~GPUCacheDataMetaData() {}

private:
	MetadataDictionary metadata;
};


/// \brief A specific class for a CacheData on CPU Memory
 class CPUCacheData : public CacheData {
 public:
 	CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table)
		: CacheData(CacheDataType::CPU, gpu_table->names(), gpu_table->get_schema(), gpu_table->num_rows())
	{
		this->host_table = ral::communication::messages::serialize_gpu_message_to_host_table(gpu_table->toBlazingTableView());
 	}

	CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table)
		: CacheData(CacheDataType::CPU, host_table->names(), host_table->get_schema(), host_table->num_rows()), host_table{std::move(host_table)}
	{
	}
 	std::unique_ptr<ral::frame::BlazingTable> decache() override {
 		return ral::communication::messages::deserialize_from_cpu(host_table.get());
 	}

	std::unique_ptr<ral::frame::BlazingHostTable> releaseHostTable() {
 		return std::move(host_table);
 	}
 	size_t sizeInBytes() const override { return host_table->sizeInBytes(); }

 	virtual ~CPUCacheData() {}

protected:
	 std::unique_ptr<ral::frame::BlazingHostTable> host_table;
 };

/// \brief A specific class for a CacheData on Disk Memory
class CacheDataLocalFile : public CacheData {
public:
	CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path);

	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	size_t sizeInBytes() const override;
	size_t fileSizeInBytes() const;
	virtual ~CacheDataLocalFile() {}
	std::string filePath() const { return filePath_; }

private:
	std::string filePath_;
	size_t size_in_bytes;
};

using frame_type = std::unique_ptr<ral::frame::BlazingTable>;

/// \brief This class represents a  messsage into que WaitingQueue.
/// We use this class to represent not only the CacheData
/// but also the message_id and the cache level (`cache_index`) where the data is stored.
class message {
public:
	message(std::unique_ptr<CacheData> content, std::string message_id = "")
		: data(std::move(content)), message_id(message_id)
	{
		assert(data != nullptr);
	}

	~message() = default;

	std::string get_message_id() const { return (message_id); }

	CacheData& get_data() const { return *data; }

	std::unique_ptr<CacheData> release_data() { return std::move(data); }

protected:
	const std::string message_id;
	std::unique_ptr<CacheData> data;
};

/**
	@brief A class that represents a Waiting Queue which is used
	into the multi-tier cache system to stores data (GPUCacheData, CPUCacheData, CacheDataLocalFile).
	This class brings concurrency support the all cache machines into the  execution graph.
	A blocking messaging system for `pop_or_wait` method is implemeted by using a condition variable.
	Note: WaitingQueue class is based on communication MessageQueue.
*/
class WaitingQueue {
public:
	using message_ptr = std::unique_ptr<message>;

	WaitingQueue() : finished{false} {}
	~WaitingQueue() = default;

	WaitingQueue(WaitingQueue &&) = delete;
	WaitingQueue(const WaitingQueue &) = delete;
	WaitingQueue & operator=(WaitingQueue &&) = delete;
	WaitingQueue & operator=(const WaitingQueue &) = delete;

	void put(message_ptr item) {
		std::unique_lock<std::mutex> lock(mutex_);
		putWaitingQueue(std::move(item));
		processed++;
		condition_variable_.notify_all();
		
	}
	int processed_parts(){
		return processed;
	}
	void finish() {
		std::unique_lock<std::mutex> lock(mutex_);
		this->finished = true;
		condition_variable_.notify_all();
	}

	bool is_finished() {
		return this->finished.load(std::memory_order_seq_cst);
	}



	void wait_for_count(int count){

		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] () {
			if (count < this->processed){
				throw std::runtime_error("WaitingQueue::wait_for_count encountered " + std::to_string(this->processed) + " when expecting " + std::to_string(count));
			}
			return count == this->processed;
		});
	}


	message_ptr pop_or_wait() {		

		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, 60000ms, [&, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst) or !this->empty();
				if (!done_waiting && blazing_timer.elapsed_time() > 59000){
					auto logger = spdlog::get("batch_logger");
					logger->warn("|||{info}|{duration}||||",
										"info"_a="WaitingQueue pop_or_wait timed out",
										"duration"_a=blazing_timer.elapsed_time());
				}
				return done_waiting;
			})){}

		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	bool wait_for_next() {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, 60000ms, [&, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst) or !this->empty();
				if (!done_waiting && blazing_timer.elapsed_time() > 59000){
					auto logger = spdlog::get("batch_logger");
					logger->warn("|||{info}|{duration}||||",
										"info"_a="WaitingQueue wait_for_next timed out",
										"duration"_a=blazing_timer.elapsed_time());
				}
				return done_waiting;
			})){}

		if(this->empty()) {
			return false;
		}
		return true;
	}

	bool has_next_now() {
		std::unique_lock<std::mutex> lock(mutex_);
		return !this->empty();
	}

	void wait_until_finished() {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, 60000ms, [&blazing_timer, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst);
				if (!done_waiting && blazing_timer.elapsed_time() > 59000){
					auto logger = spdlog::get("batch_logger");
					logger->warn("|||{info}|{duration}||||",
										"info"_a="WaitingQueue wait_until_finished timed out",
										"duration"_a=blazing_timer.elapsed_time());
				}
				return done_waiting;
			})){}
	}

	void wait_until_num_bytes(size_t num_bytes) {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, 60000ms, [&blazing_timer, num_bytes, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst);
				if (!done_waiting) {
					size_t total_bytes = 0;
					for (int i = 0; i < message_queue_.size(); i++){
						total_bytes += message_queue_[i]->get_data().sizeInBytes();
					}
					done_waiting = total_bytes > num_bytes;
				}
				if (!done_waiting && blazing_timer.elapsed_time() > 59000){
					auto logger = spdlog::get("batch_logger");
					logger->warn("|||{info}|{duration}||||",
										"info"_a="WaitingQueue wait_until_finished timed out",
										"duration"_a=blazing_timer.elapsed_time());
				}
				return done_waiting;
			})){}
	}

	size_t get_next_size_in_bytes(){
		std::unique_lock<std::mutex> lock(mutex_);
		if (message_queue_.size() > 0){
			message_queue_[0]->get_data().sizeInBytes();
		} else {
			return 0;
		}
	}

	message_ptr get_or_wait(std::string message_id) {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [message_id, &blazing_timer, this] {
				auto result = std::any_of(this->message_queue_.cbegin(),
							this->message_queue_.cend(), [&](auto &e) {
								return e->get_message_id() == message_id;
							});
				bool done_waiting = this->finished.load(std::memory_order_seq_cst) or result;
				if (!done_waiting && blazing_timer.elapsed_time() > 59000){
					auto logger = spdlog::get("batch_logger");
					logger->warn("|||{info}|{duration}|message_id|{message_id}||",
										"info"_a="WaitingQueue get_or_wait timed out",
										"duration"_a=blazing_timer.elapsed_time(),
										"message_id"_a=message_id);
				}
				return done_waiting;
			});
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		while (true){
			auto data = this->pop_unsafe();
			if (data->get_message_id() == message_id){
				return std::move(data);
			} else {
				putWaitingQueue(std::move(data));
			}
		}
	}

	message_ptr pop_unsafe() {
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	std::vector<message_ptr> get_all_or_wait() {
		CodeTimer blazing_timer;
		std::unique_lock<std::mutex> lock(mutex_);
		while(!condition_variable_.wait_for(lock, 60000ms,  [&blazing_timer, this] {
				bool done_waiting = this->finished.load(std::memory_order_seq_cst);
				if (!done_waiting && blazing_timer.elapsed_time() > 59000){
					auto logger = spdlog::get("batch_logger");
					logger->warn("|||{info}|{duration}||||",
										"info"_a="WaitingQueue get_all_or_wait timed out",
										"duration"_a=blazing_timer.elapsed_time());
				}
				return done_waiting;
			})){}
		return get_all_unsafe();
	}

	std::unique_lock<std::mutex> lock(){
		std::unique_lock<std::mutex> lock(mutex_);
		return std::move(lock);
	}

	std::vector<message_ptr> get_all_unsafe() {
		std::vector<message_ptr> messages;
		for(message_ptr & it : message_queue_) {
			messages.emplace_back(std::move(it));
		}
		message_queue_.clear();
		return messages;
	}

	void put_all_unsafe(std::vector<message_ptr> messages) {
		for(size_t i = 0; i < messages.size(); i++) {
			putWaitingQueue(std::move(messages[i]));
		}		
	}


private:
	bool empty() { 
		return this->message_queue_.size() == 0; 
	}
	
	void putWaitingQueue(message_ptr item) { message_queue_.emplace_back(std::move(item)); }

private:
	std::mutex mutex_;
	std::deque<message_ptr> message_queue_;
	std::atomic<bool> finished;
	std::condition_variable condition_variable_;
	int processed = 0;
};



std::unique_ptr<GPUCacheDataMetaData> cast_cache_data_to_gpu_with_meta(std::unique_ptr<CacheData>  base_pointer);
/**
	@brief A class that represents a Cache Machine on a
	multi-tier (GPU memory, CPU memory, Disk memory) cache system.
*/
class CacheMachine {
public:
	CacheMachine(std::shared_ptr<Context> context);

	CacheMachine(std::shared_ptr<Context> context, std::size_t flow_control_bytes_threshold);

	~CacheMachine();

	virtual void put(size_t message_id, std::unique_ptr<ral::frame::BlazingTable> table);

	virtual std::unique_ptr<ral::frame::BlazingTable> get_or_wait(size_t index);

	virtual void clear();

	virtual void addToCache(std::unique_ptr<ral::frame::BlazingTable> table, const std::string & message_id = "", bool always_add = false);

	virtual void addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, const std::string & message_id = "", bool always_add = false);

	virtual void addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> table, const std::string & message_id = "");

	virtual void finish();

	virtual bool is_finished();

	uint64_t get_num_bytes_added();

	uint64_t get_num_rows_added();

	void wait_until_finished();

	std::int32_t get_id() const;

	Context * get_context() const;

	bool wait_for_next() {
		return this->waitingCache->wait_for_next();
	}

	bool has_next_now() {
		return this->waitingCache->has_next_now();
	}
	virtual std::unique_ptr<ral::frame::BlazingTable> pullFromCache();


	virtual std::unique_ptr<ral::cache::CacheData> pullCacheData(std::string message_id);

	virtual std::unique_ptr<ral::frame::BlazingTable> pullUnorderedFromCache();


	virtual std::unique_ptr<ral::cache::CacheData> pullCacheData();


	bool thresholds_are_met(std::size_t bytes_count);

	virtual void wait_if_cache_is_saturated();

	void wait_for_count(int count){
		return this->waitingCache->wait_for_count(count);
	}
	// take the first cacheData in this CacheMachine that it can find (looking in reverse order) that is in the GPU put it in RAM or Disk as oppropriate
	// this function does not change the order of the caches
	virtual size_t downgradeCacheData();


protected:
	static std::size_t cache_count;

	/// This property represents a waiting queue object which stores all CacheData Objects
	std::unique_ptr<WaitingQueue> waitingCache;

	/// References to the properties of the multi-tier cache system
	std::vector<BlazingMemoryResource*> memory_resources;
	std::atomic<std::size_t> num_bytes_added;
	std::atomic<uint64_t> num_rows_added;
	/// This variable is to keep track of if anything has been added to the cache. Its useful to keep from adding empty tables to the cache, where we might want an empty table at least to know the schema
	bool something_added;
	std::shared_ptr<Context> ctx;
	std::shared_ptr<spdlog::logger> logger;
	std::shared_ptr<spdlog::logger> cache_events_logger;
	const std::size_t cache_id;

	std::size_t flow_control_bytes_threshold;
	std::size_t flow_control_bytes_count;
	std::mutex flow_control_mutex;
	std::condition_variable flow_control_condition_variable;

};

/**
	@brief A class that represents a Host Cache Machine on a
	multi-tier cache system, however this cache machine only stores CacheData of type CPUCacheData.
	This class is used to by pass BatchSequences.
*/
class HostCacheMachine {
public:
	HostCacheMachine(std::shared_ptr<Context> context, const std::size_t id) : ctx(context), cache_id(id) {
		waitingCache = std::make_unique<WaitingQueue>();
		logger = spdlog::get("batch_logger");
		something_added = false;

		std::shared_ptr<spdlog::logger> kernels_logger;
		kernels_logger = spdlog::get("kernels_logger");

		kernels_logger->info("{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}",
								"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
								"query_id"_a=(context ? std::to_string(context->getContextToken()) : "null"),
								"kernel_id"_a=id,
								"is_kernel"_a=0, //false
								"kernel_type"_a="host_cache");
	}

	~HostCacheMachine() {}

	virtual void addToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, const std::string & message_id = "") {
		// we dont want to add empty tables to a cache, unless we have never added anything
		if (!this->something_added || host_table->num_rows() > 0){
			logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
										"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
										"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
										"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
										"info"_a="Add to HostCacheMachine",
										"duration"_a="",
										"kernel_id"_a=message_id,
										"rows"_a=host_table->num_rows());

			auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
			auto item = std::make_unique<message>(std::move(cache_data), message_id);
			this->waitingCache->put(std::move(item));
			this->something_added = true;
		}
	}

	std::int32_t get_id() const { return cache_id; }

	virtual void finish() {
		this->waitingCache->finish();
	}

	void wait_until_finished() {
		waitingCache->wait_until_finished();
	}

	bool wait_for_next() {
		return this->waitingCache->wait_for_next();
	}

	bool has_next_now() {
		return this->waitingCache->has_next_now();
	}

	virtual std::unique_ptr<ral::frame::BlazingHostTable> pullFromCache(Context * ctx = nullptr) {
		std::unique_ptr<message> message_data = waitingCache->pop_or_wait();
		if (message_data == nullptr) {
			return nullptr;
		}

		assert(message_data->get_data().get_type() == CacheDataType::CPU);

		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
									"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
									"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
									"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
									"info"_a="Pull from HostCacheMachine",
									"duration"_a="",
									"kernel_id"_a=message_data->get_message_id(),
									"rows"_a=message_data->get_data().num_rows());

		return static_cast<CPUCacheData&>(message_data->get_data()).releaseHostTable();
	}

protected:
	std::unique_ptr<WaitingQueue> waitingCache;
	std::shared_ptr<Context> ctx;
	std::shared_ptr<spdlog::logger> logger;
	bool something_added;
	const std::size_t cache_id;
};

/**
	@brief A class that represents a Cache Machine on a
	multi-tier cache system. Moreover, it only returns a single BlazingTable by concatenating all batches.
	This Cache Machine is used in the last Kernel (OutputKernel) in the ExecutionGraph.

	This ConcatenatingCacheMachine::pullFromCache method does not guarantee the relative order
	of the messages to be preserved
*/
class ConcatenatingCacheMachine : public CacheMachine {
public:
	ConcatenatingCacheMachine(std::shared_ptr<Context> context);

	ConcatenatingCacheMachine(std::shared_ptr<Context> context, std::size_t flow_control_bytes_threshold, bool concat_all);

	~ConcatenatingCacheMachine() = default;

	std::unique_ptr<ral::frame::BlazingTable> pullFromCache() override;

	std::unique_ptr<ral::frame::BlazingTable> pullUnorderedFromCache() override {
		return pullFromCache();
	}

	size_t downgradeCacheData() override { // dont want to be able to downgrage concatenating caches
		return 0;
	}

  private:
	bool concat_all;

};




}  // namespace cache
} // namespace ral
