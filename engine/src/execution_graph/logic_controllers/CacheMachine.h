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


/**
* Indicates a type of CacheData
* CacheData are type erased so we need to know if they reside in GPU,
* CPU, or a file. We can also have GPU messages that contain metadata
* which are used for sending CacheData from node to node
*/
enum class CacheDataType { GPU, CPU, LOCAL_FILE, GPU_METADATA };

const std::string KERNEL_ID_METADATA_LABEL = "kernel_id"; /**< A message metadata field that indicates which kernel owns this message. */
const std::string QUERY_ID_METADATA_LABEL = "query_id"; /**< A message metadata field that indicates which query owns this message. */
const std::string CACHE_ID_METADATA_LABEL = "cache_id";  /**< A message metadata field that indicates what cache a message should be routed to. Can be empty string if
 																															and only if add_to_specific_cache == false. */
const std::string ADD_TO_SPECIFIC_CACHE_METADATA_LABEL = "add_to_specific_cache";  /**< A message metadata field that indicates if a message should be routed to a specific cache or to the global input cache. */
const std::string SENDER_WORKER_ID_METADATA_LABEL = "sender_worker_id"; /**< A message metadata field that indicates what worker is sending this message. */
const std::string WORKER_IDS_METADATA_LABEL = "worker_ids"; /**< A message metadata field that indicates what worker is sending this message. */
const std::string TOTAL_TABLE_ROWS_METADATA_LABEL = "total_table_rows"; /**< A message metadata field that indicates how many rows are in this message. */
const std::string JOIN_LEFT_BYTES_METADATA_LABEL = "join_left_bytes_metadata_label"; /**< A message metadata field that indicates how many bytes were found in a left table for join scheduling.  */
const std::string JOIN_RIGHT_BYTES_METADATA_LABEL = "join_right_bytes_metadata_label"; /**< A message metadata field that indicates how many bytes were found in a right table for join scheduling.  */
const std::string MESSAGE_ID = "message_id"; /**< A message metadata field that indicates the id of a message. Not all messages have an id. Any message that has add_to_specific_cache == false MUST have a message id. */
const std::string PARTITION_COUNT = "partition_count"; /**< A message metadata field that indicates the number of partitions a kernel processed.  */

/**
* Base Class for all CacheData
* A CacheData represents a combination of a schema along with a container for
* a dataframe. This gives us one type that can be sent around and whose only
* purpose is to hold data until it is ready to be operated on by calling
* the decache method.
*/
class CacheData {
public:

	/**
	* Constructor for CacheData
	* This is only invoked by the derived classes when constructing.
	* @param cache_type The CacheDataType of this cache letting us know where the data is stored.
	* @param col_names The names of the columns in the dataframe.
	* @param schema The types of the columns in the dataframe.
	* @param n_rows The number of rows in the dataframe.
	*/
	CacheData(CacheDataType cache_type, std::vector<std::string> col_names, std::vector<cudf::data_type> schema, size_t n_rows)
		: cache_type(cache_type), col_names(col_names), schema(schema), n_rows(n_rows)
	{
	}
	/**
	* Remove the payload from this CacheData. A pure virtual function.
	* This removes the payload for the CacheData. After this the CacheData will
	* almost always go out of scope and be destroyed.
	* @return a BlazingTable generated from the source of data for this CacheData
	*/
	virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;

	/**
	* . A pure virtual function.
	* This removes the payload for the CacheData. After this the CacheData will
	* almost always go out of scope and be destroyed.
	* @return the number of bytes our dataframe occupies in whatever format it is being stored
	*/
	virtual size_t sizeInBytes() const = 0;

	/**
	* Destructor
	*/
	virtual ~CacheData() {}

	/**
	* Get the names of the columns.
	* @return a vector of the column names
	*/
	std::vector<std::string> names() const {
		return col_names;
	}

	/**
	* Get the cudf::data_type of each column.
	* @return a vector of the cudf::data_type of each column.
	*/
	std::vector<cudf::data_type> get_schema() const {
		return schema;
	}

	/**
	* Get the number of rows this CacheData will generate with decache.
	*/
	size_t num_rows() const {
		return n_rows;
	}

	/**
	* Gets the type of CacheData that was used to construct this CacheData
	* @return The CacheDataType that is used to store the dataframe representation.
	*/
	CacheDataType get_type() const {
		return cache_type;
	}

protected:
	CacheDataType cache_type; /**< The CacheDataType that is used to store the dataframe representation. */
	std::vector<std::string> col_names; /**< A vector storing the names of the columns in the dataframe representation. */
	std::vector<cudf::data_type> schema; /**< A vector storing the cudf::data_type of the columns in the dataframe representation. */
	size_t n_rows; /**< Stores the number of rows in the dataframe representation. */
};

/**
* Lightweight wrapper for a map that will one day be used for compile time checks.
* Currently it is just a wrapper for map but in the future the intention is for
* us to manage the Metadata in a struct as opposed to a map of string to string.
*/
class MetadataDictionary{
public:

	/**
	* Gets the type of CacheData that was used to construct this CacheData
	* @param key The key in the map that we will be modifying.
	* @param value The value that we will set the key to.
	*/
	void add_value(std::string key, std::string value){
		this->values[key] = value;
	}

	/**
	* Gets the type of CacheData that was used to construct this CacheData
	* @param key The key in the map that we will be modifying.
	* @param value The value that we will set the key to.
	*/
	void add_value(std::string key, int value){
		this->values[key] = std::to_string(value);
	}

	/**
	* Gets id of creating kernel.
	* @return Get the id of the kernel that created this message.
	*/
	int get_kernel_id(){
		if( this->values.find(KERNEL_ID_METADATA_LABEL) == this->values.end()){
			throw BlazingMissingMetadataException(KERNEL_ID_METADATA_LABEL);
		}
		return std::stoi(values[KERNEL_ID_METADATA_LABEL]);
	}

	/**
	* Print every key => value pair in the map.
	* Only used for debugging purposes.
	*/
	void print(){
		for(auto elem : this->values)
		{
		   std::cout << elem.first << " " << elem.second<< "\n";
		}
	}
	/**
	* Gets the map storing the metadata.
	* @return the map storing all of the metadata.
	*/

	std::map<std::string,std::string> get_values(){
		return this->values;
	}

	/**
	* Erases all current metadata and sets new values.
	* @param new_values A map to copy into this->values .
	*/
	void set_values(std::map<std::string,std::string> new_values){
		this->values= new_values;
	}
private:
	std::map<std::string,std::string> values; /**< Stores the mapping of metdata label to metadata value */
};

/**
* A CacheData that keeps its dataframe in GPU memory.
* This is a CacheData representation that wraps a ral::frame::BlazingTable. It
* is the most performant since its construction and decaching are basically
* no ops.
*/
class GPUCacheData : public CacheData {
public:
	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table)
		: CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}

	/**
	* Constructor with CacheDataType override.
	* This constructor is called by GPUCacheDataMetaData constructor.
	* @param table The BlazingTable that is moved into the CacheData.
	* @param cache_type_override The type we want to set for this CacheData.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, CacheDataType cache_type_override)
		: CacheData(cache_type_override,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}

	/**
	* Move the BlazingTable out of this Cache
	* This function only exists so that we can interact with all cache data by
	* calling decache on them.
	* @return The BlazingTable that was used to construct this CacheData.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }

	/**
	* Get the amount of GPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingTable consumes.
	*/
	size_t sizeInBytes() const override { return data->sizeInBytes(); }
	/**
	* Destructor
	*/
	virtual ~GPUCacheData() {}

	/**
	* Get a ral::frame::BlazingTableView of the underlying data.
	* This allows you to read the data while it remains in cache.
	* @return a view of the data in this instance.
	*/
	ral::frame::BlazingTableView getTableView(){
		return this->data->toBlazingTableView();
	}

protected:
	std::unique_ptr<ral::frame::BlazingTable> data; /**< Stores the data to be returned in decache */
};

/**
* A GPUCacheData that also has a MetadataDictionary
* These are messages that have metadata attached to them which is used
* in planning and routing.
*/
class GPUCacheDataMetaData : public GPUCacheData {
public:
	/**
	* Constructor
	* These are messages that have metadata attached to them which is used
	* in planning and routing.
	* @param table The BlazingTable that is moved into the CacheData.
	* @param metadata The metadata that will be used in transport and planning.
	*/
	GPUCacheDataMetaData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
		: GPUCacheData(std::move(table), CacheDataType::GPU_METADATA),
		metadata(metadata)
		 { }

	 /**
 	* Move the BlazingTable out of this Cache
 	* This function only exists so that we can interact with all cache data by
 	* calling decache on them.
 	* @return The BlazingTable that was used to construct this CacheData.
 	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }
	/**
	* Move the BlazingTable and MetadataDictionary out of this Cache
	* This function only exists so that we can interact with all cache data by
	* calling decache on them.
	* @return A pair with the BlazingTable and MetadataDictionary that was used to
	* construct this CacheData.
	*/
	std::pair<std::unique_ptr<ral::frame::BlazingTable>,MetadataDictionary > decacheWithMetaData(){
		 return std::make_pair(std::move(data),this->metadata); }

	/**
 	* Get the amount of GPU memory consumed by this CacheData
 	* Having this function allows us to have one api for seeing the consumption
 	* of all the CacheData objects that are currently in Caches.
 	* @return The number of bytes the BlazingTable consumes.
 	*/
	size_t sizeInBytes() const override { return data->sizeInBytes(); }

	/**
	* Get the MetadataDictionary
	* @return The MetadataDictionary which is used in routing and planning.
	*/
	MetadataDictionary getMetadata(){
		return this->metadata;
	}

	/**
	* Destructor
	*/
	virtual ~GPUCacheDataMetaData() {}

private:
	MetadataDictionary metadata; /**< The metadata used for routing and planning. */
};


/**
* A CacheData that keeps its dataframe in CPU memory.
* This is a CacheData representation that wraps a ral::frame::BlazingHostTable.
* It is the more performant than most file based caching strategies but less
* efficient than a GPUCacheData.
*/
 class CPUCacheData : public CacheData {
 public:
	/**
 	* Constructor
	* Takes a GPU based ral::frame::BlazingTable and converts it CPU version
	* that is stored in a ral::frame::BlazingHostTable.
	* @param table The BlazingTable that is converted to a BlazingHostTable and
	* stored.
 	*/
 	CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table)
		: CacheData(CacheDataType::CPU, gpu_table->names(), gpu_table->get_schema(), gpu_table->num_rows())
	{
		this->host_table = ral::communication::messages::serialize_gpu_message_to_host_table(gpu_table->toBlazingTableView());
 	}

	/**
 	* Constructor
 	* Takes a GPU based ral::frame::BlazingHostTable and stores it in this
 	* CacheData instance.
	* @param table The BlazingHostTable that is moved into the CacheData.
 	*/
	CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table)
		: CacheData(CacheDataType::CPU, host_table->names(), host_table->get_schema(), host_table->num_rows()), host_table{std::move(host_table)}
	{
	}
	/**
	* Decache from a BlazingHostTable to BlazingTable and return the BlazingTable.
	* @return A unique_ptr to a BlazingTable
 	*/
 	std::unique_ptr<ral::frame::BlazingTable> decache() override {
 		return ral::communication::messages::deserialize_from_cpu(host_table.get());
 	}

	/**
 	* Release this BlazingHostTable from this CacheData
	* If you want to allow this CacheData to be destroyed but want to keep the
	* memory in CPU this allows you to pull it out as a BlazingHostTable.
	* @return a unique_ptr to the BlazingHostTable that this was either
	* constructed with or which was generated during construction from a
	* BlazingTable.
 	*/
	std::unique_ptr<ral::frame::BlazingHostTable> releaseHostTable() {
 		return std::move(host_table);
 	}

	/**
	* Get the amount of CPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingHostTable consumes.
	*/
 	size_t sizeInBytes() const override { return host_table->sizeInBytes(); }
	/**
	* Destructor
	*/
 	virtual ~CPUCacheData() {}

protected:
	 std::unique_ptr<ral::frame::BlazingHostTable> host_table; /**< The CPU representation of a DataFrame  */
 };

/**
* A CacheData that stores is data in an ORC file.
* This allows us to cache onto filesystems to allow larger queries to run on
* limited resources. This is the least performant cache in most instances.
*/
class CacheDataLocalFile : public CacheData {
public:

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path);

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	/**
 	* Get the amount of GPU memory that the decached BlazingTable WOULD consume.
 	* Having this function allows us to have one api for seeing how much GPU
	* memory is necessary to decache the file from disk.
 	* @return The number of bytes needed for the BlazingTable decache would
	* generate.
 	*/
	size_t sizeInBytes() const override;
	/**
	* Get the amount of disk space consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the ORC file consumes.
	*/
	size_t fileSizeInBytes() const;

	/**
	* Destructor
	*/
	virtual ~CacheDataLocalFile() {}

	/**
	* Get the file path of the ORC file.
	* @return The path to the ORC file.
	*/
	std::string filePath() const { return filePath_; }

private:
	std::string filePath_; /**< The path to the ORC file. Is usually generated randomly. */
	size_t size_in_bytes; /**< The size of the file being stored. */
};

using frame_type = std::unique_ptr<ral::frame::BlazingTable>;


/**
* A wrapper for a CacheData object with a message_id.
* Bundles together a String mesage_id with a CacheData object. Really not very
* necessary as we could just as easily store this in the CacheData object.
*/
class message { //TODO: the cache_data object can store its id. This is not needed.
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
* A Queue that has built in methods for waiting operations.
* The purpose of WaitingQueue is to provide apis that get things out of the
* queue when they exist and wait for things when they don't without consuming
* many compute resources.This is accomplished through the use of a
* condition_variable and mutex locks.
*/
class WaitingQueue {
public:
	using message_ptr = std::unique_ptr<message>;

	/**
	* Constructor
	*/
	WaitingQueue() : finished{false} {}

	/**
	* Destructor
	*/
	~WaitingQueue() = default;

	WaitingQueue(WaitingQueue &&) = delete;
	WaitingQueue(const WaitingQueue &) = delete;
	WaitingQueue & operator=(WaitingQueue &&) = delete;
	WaitingQueue & operator=(const WaitingQueue &) = delete;

	/**
	* Put a message onto the WaitingQueue using unique_lock.
	* This message aquires a unique_lock and then pushes a message onto the
	* WaitingQueue. It then increments the processed count and notifies the
	* WaitingQueue's condition variable.
	* @param item the message_ptr being added to the WaitingQueue
	*/
	void put(message_ptr item) {
		std::unique_lock<std::mutex> lock(mutex_);
		putWaitingQueue(std::move(item));
		processed++;
		condition_variable_.notify_all();
	}

	/**
	* Get number of partitions processed.
	* @return number of partitions that have been inserted into this WaitingQueue.
	*/
	int processed_parts(){
		return processed;
	}

	/**
	* Finish lets us know that know more messages will come in.
	* This exists so that if anyone is trying to pull from a queue that is already
	* completed that operation will return nullptr so it will not block
	* indefinitely.
	*/
	void finish() {
		std::unique_lock<std::mutex> lock(mutex_);
		this->finished = true;
		condition_variable_.notify_all();
	}

	/**
	* Lets us know if a WaitingQueue has finished running messages.
	* @return A bool indicating whether or not this WaitingQueue is finished.
	* receiving messages.
	*/
	bool is_finished() {
		return this->finished.load(std::memory_order_seq_cst);
	}


	/**
	* Blocks executing thread until a certain number messages are reached.
	* We often want to block a thread from proceeding until a certain number ouf
	* messages exist in the WaitingQueue. It also alerts us if we ever receive
	* more messages than we expected.
	*/
	void wait_for_count(int count){

		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] () {
			if (count < this->processed){
				throw std::runtime_error("WaitingQueue::wait_for_count encountered " + std::to_string(this->processed) + " when expecting " + std::to_string(count));
			}
			return count == this->processed;
		});
	}

	/**
	* Get a message_ptr if it exists in the WaitingQueue else wait.
	* This function allows kernels to pull from the cache before a cache has
	* CacheData in it. If finish is called on the WaitingQueue and no messages
	* are left this returns nullptr.
	* @return A message_ptr that was pushed into the WaitingQueue nullptr if
	* the WaitingQueue is empty and finished.
	*/
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
	/**
	* Wait for the next message to be ready.
	* @return Waits for the next CacheData to be available. Returns true when this
	* is the case. Returns false if the WaitingQueue is both finished and empty.
	*/
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
	/**
	* Indicates if the WaitingQueue has messages at this point in time.
	* @return A bool which is true if the WaitingQueue is not empty.
	*/
	bool has_next_now() {
		std::unique_lock<std::mutex> lock(mutex_);
		return !this->empty();
	}

	/**
	* Pauses a threads execution until this WaitingQueue has finished processing.
	* Sometimes, like in the case of Joins, we might be waiting for a
	* WaitingQueue to have finished before the next kernel can use the data it
	* contains.
	*/
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

	/**
	* Waits until a certain number of bytes exist in the WaitingQueue
	* During some execution kernels it is better to wait for a certain amount
	* of the total anticipated data to be available before processing the next
	* batch.
	* @param num_bytes The number of bytes that we will wait to exist in the
	* WaitingQueue unless the WaitingQueue has already had finished() called.
	*/
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

	/**
	* Let's us know the size of the next CacheData to be pulled.
	* Sometimes it is useful to know how much data we will be pulling in each
	* CacheData that we have accumulated for making estimates on how much more
	* is going to be coming or seeing how far along we are.
	* @return The number of bytes consumed by the next message.
	*/
	size_t get_next_size_in_bytes(){
		std::unique_lock<std::mutex> lock(mutex_);
		if (message_queue_.size() > 0){
			message_queue_[0]->get_data().sizeInBytes();
		} else {
			return 0;
		}
	}

	/**
	* Get a specific message from the WaitingQueue.
	* Messages are always accompanied by a message_id though in some cases that
	* id is empty string. This allows us to get a specific message from the
	* WaitingQueue and wait around for it to exist or for this WaitingQueue to be
	* finished. If WaitingQueue is finished and there are no messages we get
	* a nullptr.
	* @param message_id The id of the message that we want to get or wait for.
	* @return The message that has this id or nullptr if that message will  never
	* be able to arrive because the WaitingQueue is finished.
	*/
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

	/**
	* Pop the front element WITHOUT thread safety.
	* Allos us to pop from the front in situations where we have already acquired
	* a unique_lock on this WaitingQueue's mutex.
	* @return The first message in the WaitingQueue.
	*/
	message_ptr pop_unsafe() {
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	/**
	* Waits until all messages are ready then returns all of them.
	* You should never call this function more than once on a WaitingQueue else
	* race conditions can occur.
	* @return A vector of all the messages that were inserted into the
	* WaitingQueue.
	*/
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

	/**
	* A function that returns a unique_lock using the WaitingQueue's mutex.
	* @return A unique_lock
	*/
	std::unique_lock<std::mutex> lock(){
		std::unique_lock<std::mutex> lock(mutex_);
		return std::move(lock);
	}

	/**
	* Get all messagse in the WaitingQueue without locking.
	* @return A vector with all the messages in the WaitingQueue.
	*/
	std::vector<message_ptr> get_all_unsafe() {
		std::vector<message_ptr> messages;
		for(message_ptr & it : message_queue_) {
			messages.emplace_back(std::move(it));
		}
		message_queue_.clear();
		return messages;
	}

	/**
	* Put a vector of messages onto the WaitingQueue without locking.
	* @param messages A vector of messages that will be pushed into the WaitingQueue.
	*/
	void put_all_unsafe(std::vector<message_ptr> messages) {
		for(size_t i = 0; i < messages.size(); i++) {
			putWaitingQueue(std::move(messages[i]));
		}
	}


private:
	/**
	* Checks if the WaitingQueue is empty.
	* @return A bool indicating if the WaitingQueue is empty.
	*/
	bool empty() {
		return this->message_queue_.size() == 0;
	}
	/**
	* Adds a message to the WaitingQueue
	* @param item The message to add to the WaitingQueue.
	*/
	void putWaitingQueue(message_ptr item) { message_queue_.emplace_back(std::move(item)); }

private:
	std::mutex mutex_; /**< This mutex is used for making access to the
											WaitingQueue thread-safe. */
	std::deque<message_ptr> message_queue_; /**< */
	std::atomic<bool> finished; /**< Indicates if this WaitingQueue is finished. */
	std::condition_variable condition_variable_; /**< Used to notify waiting
																								functions*/
	int processed = 0; /**< Count of messages added to the WaitingQueue. */
};


/**
* Helper function to convert a std::unique_ptr<CacheData>  to a std::unique_ptr<GPUCacheDataMetaData>.
* @param base_pointer The unique_ptr we will convert.
* @return The converted pointer.
*/
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
