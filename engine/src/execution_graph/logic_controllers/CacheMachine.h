#pragma once
#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "execution_graph/logic_controllers/BlazingColumnOwner.h"
#include "execution_graph/logic_controllers/BlazingColumnView.h"
#include <atomic>
#include <blazingdb/manager/Context.h>
#include <cudf/io/functions.hpp>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <src/communication/messages/GPUComponentMessage.h>
#include <string>
#include <typeindex>
#include <vector>
#include <bmr/BlazingMemoryResource.h>

namespace ral {
namespace cache {

enum CacheDataType { GPU, CPU, LOCAL_FILE, IO_FILE };

/// \brief An interface which represent a CacheData 
class CacheData {
public:
	CacheData(std::vector<std::string> col_names, std::vector<cudf::data_type> schema, size_t n_rows)
		: col_names(col_names), schema(schema), n_rows(n_rows)
	{
	}
	virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;

	virtual unsigned long long sizeInBytes() = 0;
	
	virtual ~CacheData() {}

	std::vector<std::string> names() const {
		return col_names;
	}
	std::vector<cudf::data_type> get_schema() {
		return schema;
	}
	size_t num_rows() const {
		return n_rows;
	}
	
protected:
	std::vector<std::string> 		col_names;
	std::vector<cudf::data_type> 	schema;
	size_t							n_rows;
};

/// \brief A specific class for a CacheData on GPU Memory 
class GPUCacheData : public CacheData {
public:
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table) : CacheData(table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}

	std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }

	unsigned long long sizeInBytes() override { return data->sizeInBytes(); }

	virtual ~GPUCacheData() {}

private:
	std::unique_ptr<ral::frame::BlazingTable> data;
};

/// \brief A specific class for a CacheData on CPU Memory 
 class CPUCacheData : public CacheData {
 public:
 	CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table) 
		: CacheData(gpu_table->names(), gpu_table->get_schema(), gpu_table->num_rows()) 
	{
		this->host_table = ral::communication::messages::experimental::serialize_gpu_message_to_host_table(gpu_table->toBlazingTableView());
 	}

	CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table)
		: CacheData(host_table->names(), host_table->get_schema(), host_table->num_rows()), host_table{std::move(host_table)}
	{
	}
 	std::unique_ptr<ral::frame::BlazingTable> decache() override {
 		return ral::communication::messages::experimental::deserialize_from_cpu(host_table.get());
 	}

	std::unique_ptr<ral::frame::BlazingHostTable> releaseHostTable() {
 		return std::move(host_table);
 	}
 	unsigned long long sizeInBytes() override { return host_table->sizeInBytes(); }

 	virtual ~CPUCacheData() {}

protected:
	 std::unique_ptr<ral::frame::BlazingHostTable> host_table;
 };

/// \brief A specific class for a CacheData on Disk Memory 
class CacheDataLocalFile : public CacheData {
public:
	CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table);

	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	unsigned long long sizeInBytes() override;
	virtual ~CacheDataLocalFile() {}
	std::string filePath() const { return filePath_; }

private:
	std::string filePath_;
	// ideally would be a writeable file
};

using frame_type = std::unique_ptr<ral::frame::BlazingTable>;

/// \brief This class represents a  messsage into que WaitingQueue. 
/// We use this class to represent not only the CacheData 
/// but also the message_id and the cache level (`cache_index`) where the data is stored.
template <class T>
class message {
public:
	message(std::unique_ptr<T> content, size_t cache_index, std::string message_id = "")
		: data(std::move(content)), cache_index{cache_index}, message_id(message_id) {
	}

	virtual ~message() = default;

	std::string get_message_id() { return (message_id); }

	std::unique_ptr<T> releaseData() { return std::move(data); }
	size_t cacheIndex() { return cache_index; }

protected:
	const std::string message_id;
	size_t cache_index;
	std::unique_ptr<T> data;
};

/**
	@brief A class that represents a Waiting Queue which is used 
	into the multi-tier cache system to stores data (GPUCacheData, CPUCacheData, CacheDataLocalFile). 
	This class brings concurrency support the all cache machines into the  execution graph. 
	A blocking messaging system for `pop_or_wait` method is implemeted by using a condition variable.
	Note: WaitingQueue class is based on communication MessageQueue. 
*/
template <class T>
class WaitingQueue {
public:
	using message_ptr = std::unique_ptr<message<T>>;

	WaitingQueue() : finished{false} {}
	~WaitingQueue() = default;

	WaitingQueue(WaitingQueue &&) = delete;
	WaitingQueue(const WaitingQueue &) = delete;
	WaitingQueue & operator=(WaitingQueue &&) = delete;
	WaitingQueue & operator=(const WaitingQueue &) = delete;

	void put(message_ptr item) {
		std::unique_lock<std::mutex> lock(mutex_);
		putWaitingQueue(std::move(item));
		lock.unlock();
		condition_variable_.notify_all();
	}

	void finish() { 
		std::unique_lock<std::mutex> lock(mutex_);
		this->finished = true;
		condition_variable_.notify_all(); 
	}

	bool is_finished() {
		return this->finished.load(std::memory_order_seq_cst);
	}

	bool empty() const { return this->message_queue_.size() == 0; }

	message_ptr pop_or_wait() {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] { return this->finished.load(std::memory_order_seq_cst) or !this->empty(); });
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	bool wait_for_next() {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] { return this->finished.load(std::memory_order_seq_cst) or !this->empty(); });
		if(this->empty()) {
			return false;	
		}
		return true;
	}

	bool has_next_now() {
		std::unique_lock<std::mutex> lock(mutex_);
		return !this->empty();
	}

	bool ready_to_execute() {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] { return this->finished.load(std::memory_order_seq_cst) or !this->empty(); });
		return not this->finished;
	}

	message_ptr get_or_wait(std::string message_id) {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [message_id, this] { 
				auto result = std::any_of(this->message_queue_.cbegin(),
							this->message_queue_.cend(), [&](auto &e) {
								return e->get_message_id() == message_id;
							});
				return this->finished.load(std::memory_order_seq_cst) or result;
		 });
		if(this->message_queue_.size() == 0) {
			return nullptr;
		}
		while (true){
			auto data = this->pop();
			if (data->get_message_id() == message_id){
				return std::move(data);
			} else {
				putWaitingQueue(std::move(data));				
			}
		}
	}

	message_ptr pop() {
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	std::vector<message_ptr> get_all_or_wait() {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] { return this->finished.load(std::memory_order_seq_cst); });
		std::vector<message_ptr> response;
		for(message_ptr & it : message_queue_) {
			response.emplace_back(std::move(it));
		}
		message_queue_.erase(message_queue_.begin(), message_queue_.end());
		return response;
	} 
	
	void setNumberOfBatches(size_t n_batches) {
		this->n_batches = n_batches;
	}

	size_t getNumberOfBatches() {
		return this->n_batches;
	}

private:
	void putWaitingQueue(message_ptr item) { message_queue_.emplace_back(std::move(item)); }

private:
	std::mutex mutex_;
	std::deque<message_ptr> message_queue_;
	std::atomic<bool> finished;
	std::atomic<size_t> n_batches{1};
	std::condition_variable condition_variable_;
};
/**
	@brief A class that represents a Cache Machine on a
	multi-tier (GPU memory, CPU memory, Disk memory) cache system. 
*/
class CacheMachine {
public:
	CacheMachine();

	~CacheMachine();

	virtual void put(size_t message_id, std::unique_ptr<ral::frame::BlazingTable> table);

	virtual std::unique_ptr<ral::frame::BlazingTable> get_or_wait(size_t index);

	virtual void clear();

	virtual void addToCache(std::unique_ptr<ral::frame::BlazingTable> table, std::string message_id = "");

	virtual void addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, std::string message_id = "");

	virtual void addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> table, std::string message_id = "");

	virtual void finish();

	virtual bool is_finished();

	uint64_t get_num_bytes_added();

	uint64_t get_num_rows_added();

	bool ready_to_execute();

	bool wait_for_next() {
		return this->waitingCache->wait_for_next();
	}
	
	bool has_next_now() {
		return this->waitingCache->has_next_now();
	} 
	virtual std::unique_ptr<ral::frame::BlazingTable> pullFromCache();

	virtual std::unique_ptr<ral::cache::CacheData> pullCacheData();

	void setNumberOfBatches(size_t n_batches) {
		this->waitingCache->setNumberOfBatches(n_batches);
	}

	size_t getNumberOfBatches() {
		return this->waitingCache->getNumberOfBatches();
	}

protected:
	/// This property represents a waiting queue object which stores all CacheData Objects  
	std::unique_ptr<WaitingQueue<CacheData>> waitingCache;

protected:
	/// References to the properties of the multi-tier cache system
	std::vector<BlazingMemoryResource*> memory_resources;
	std::atomic<uint64_t> num_bytes_added;
	std::atomic<uint64_t> num_rows_added;
};

/**
	@brief A class that represents a Host Cache Machine on a
	multi-tier cache system, however this cache machine only stores CacheData of type CPUCacheData.
	This class is used to by pass BatchSequences.
*/ 
class HostCacheMachine {
public:
	HostCacheMachine() {
		waitingCache = std::make_unique<WaitingQueue<CacheData>>();
	}

	~HostCacheMachine() {}

	virtual void addToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table) {
		auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
		std::unique_ptr<message<CacheData>> item = std::make_unique<message<CacheData>>(std::move(cache_data), 0);
		this->waitingCache->put(std::move(item));
	}

	virtual void finish() {
		this->waitingCache->finish();
	}

	bool ready_to_execute() {
		return waitingCache->ready_to_execute();
	}

	bool wait_for_next() {
		return this->waitingCache->wait_for_next();
	}
	
	bool has_next_now() {
		return this->waitingCache->has_next_now();
	} 
	
	virtual std::unique_ptr<ral::frame::BlazingHostTable> pullFromCache() {
		std::unique_ptr<message<CacheData>> message_data = waitingCache->pop_or_wait();
		if (message_data == nullptr) {
			return nullptr;
		}
		std::unique_ptr<CacheData> cache_data = message_data->releaseData();
		auto cpu_data = (CPUCacheData * )(cache_data.get());
		return cpu_data->releaseHostTable();
	}

	void setNumberOfBatches(size_t n_batches) {
		this->waitingCache->setNumberOfBatches(n_batches);
	}

	size_t getNumberOfBatches() {
		return this->waitingCache->getNumberOfBatches();
	}
protected:
	std::unique_ptr<WaitingQueue<CacheData>> waitingCache;
};

/**
	@brief A class that represents a Cache Machine on a
	multi-tier cache system. Moreover, it only returns a single BlazingTable by concatenating all batches.
	This Cache Machine is used in the last Kernel (OutputKernel) in the ExecutionGraph.
*/ 
class ConcatenatingCacheMachine : public CacheMachine {
public:
	ConcatenatingCacheMachine();

	~ConcatenatingCacheMachine() = default;

	std::unique_ptr<ral::frame::BlazingTable> pullFromCache() override;
};


}  // namespace cache
} // namespace ral
