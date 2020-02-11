
#pragma once

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <blazingdb/manager/Context.h>
#include <cudf/io/functions.hpp>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <typeindex>
#include <vector>

typedef cudf::experimental::table CudfTable;
typedef cudf::table_view CudfTableView;
typedef cudf::column CudfColumn;
typedef cudf::column_view CudfColumnView;
namespace cudf_io = cudf::experimental::io;

namespace ral {

namespace frame {

class BlazingTable;
class BlazingTableView;

class BlazingTable {
public:
	BlazingTable(std::unique_ptr<CudfTable> table, std::vector<std::string> columnNames);
	BlazingTable(BlazingTable &&) = default;
	BlazingTable & operator=(BlazingTable const &) = delete;
	BlazingTable & operator=(BlazingTable &&) = delete;

	CudfTableView view() const;
	cudf::size_type num_columns() const { return table->num_columns(); }
	cudf::size_type num_rows() const { return table->num_rows(); }
	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> & names) { this->columnNames = names; }

	BlazingTableView toBlazingTableView() const;

	operator bool() const { return table != nullptr; }

	std::unique_ptr<CudfTable> releaseCudfTable() { return std::move(table); }

	unsigned long long sizeInBytes() {
		unsigned long long total_size = 0UL;
		for(cudf::size_type i = 0; i < this->table->num_columns(); ++i) {
			const cudf::column_view & column = this->table->get_column(i);
			if(column.type().id() == cudf::type_id::STRING) {
				auto num_children = column.num_children();
				assert(num_children == 2);

				auto offsets_column = column.child(0);
				auto chars_column = column.child(1);

				total_size += chars_column.size();
				cudf::data_type offset_dtype(cudf::type_id::INT32);
				total_size += offsets_column.size() * cudf::size_of(offset_dtype);
				if(column.has_nulls()) {
					total_size += cudf::bitmask_allocation_size_bytes(column.size());
				}
			} else {
				total_size += column.size() * cudf::size_of(column.type());
				if(column.has_nulls()) {
					total_size += cudf::bitmask_allocation_size_bytes(column.size());
				}
			}
		}
		return total_size;
	}

private:
	std::vector<std::string> columnNames;
	std::unique_ptr<CudfTable> table;
};


class BlazingTableView {
public:
	BlazingTableView();
	BlazingTableView(CudfTableView table, std::vector<std::string> columnNames);
	BlazingTableView(BlazingTableView const &) = default;
	BlazingTableView(BlazingTableView &&) = default;

	BlazingTableView & operator=(BlazingTableView const &) = default;
	BlazingTableView & operator=(BlazingTableView &&) = default;

	CudfTableView view() const;

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }

	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> & names) { this->columnNames = names; }

	cudf::size_type num_columns() const { return table.num_columns(); }

	cudf::size_type num_rows() const { return table.num_rows(); }

	std::unique_ptr<BlazingTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
};

typedef std::pair<std::unique_ptr<ral::frame::BlazingTable>, ral::frame::BlazingTableView> TableViewPair;

TableViewPair createEmptyTableViewPair(std::vector<cudf::type_id> column_types, std::vector<std::string> column_names);

}  // namespace frame

namespace cache {

enum CacheDataType { GPU, CPU, LOCAL_FILE, IO_FILE };


class CacheData {
public:
	virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;
	virtual unsigned long long sizeInBytes() = 0;
	virtual ~CacheData() {}

protected:
	std::vector<std::string> names;
};

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

class CacheMachine {
public:
	CacheMachine(unsigned long long gpuMemory,
		std::vector<unsigned long long> memoryPerCache,
		std::vector<CacheDataType> cachePolicyTypes);
	virtual ~CacheMachine();

	virtual std::unique_ptr<ral::frame::BlazingTable> pullFromCache();
	virtual void addToCache(std::unique_ptr<ral::frame::BlazingTable> table);

	//  void eject(int partitionNumber);
	//  void reprocess(int partitionNumber);
	//  void reduceCache(int cacheIndex, unsigned long long reductionAmount);
	//  void incrementCache(int cacheIndex, unsigned long long incrementAmount);
	// TODO: figure out how we tell it which cache to prefer
	bool finished();
	void finish();

protected:
	std::vector<CacheDataType> cachePolicyTypes;
	std::vector<unsigned long long> memoryPerCache;
	std::vector<unsigned long long> usedMemory;
	bool _finished;

private:
	std::mutex cacheMutex;
	std::vector<std::unique_ptr<std::queue<std::unique_ptr<CacheData>>>> cache;
	std::queue<std::unique_ptr<ral::frame::BlazingTable>> gpuData;
	//  std::map<int,std::unique_ptr<CacheData> > operatingData;
};


enum kstatus { stop, proceed };
using frame_type = std::unique_ptr<ral::frame::BlazingTable>;
static std::size_t message_count = {0};

template <class T>
class message {
public:
	message(std::unique_ptr<T> content) : data(std::move(content)), message_id(message_count) { message_count++; }

	virtual ~message() = default;

	std::size_t get_id() { return (message_id); }

	std::unique_ptr<T> releaseData() { return std::move(data); }

protected:
	const std::size_t message_id;
	std::unique_ptr<T> data;
};


template <class T>
class WaitingQueue {
public:
	using message_ptr = std::unique_ptr<message<T>>;

	WaitingQueue() = default;
	~WaitingQueue() = default;

	WaitingQueue(WaitingQueue &&) = delete;
	WaitingQueue(const WaitingQueue &) = delete;
	WaitingQueue & operator=(WaitingQueue &&) = delete;
	WaitingQueue & operator=(const WaitingQueue &) = delete;

	message_ptr get(const std::size_t & message_id) {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] {
			return std::any_of(this->message_queue_.cbegin(), this->message_queue_.cend(), [&](const auto & e) {
				return e->get_id() == message_id;
			});
		});
		return getWaitingQueue(message_id);
	}
	message_ptr get() {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_variable_.wait(lock, [&, this] { return this->message_queue_.size() > 0; });
		auto data = std::move(this->message_queue_.front());
		this->message_queue_.pop_front();
		return std::move(data);
	}

	bool empty() const {
		return this->message_queue_.size() == 0;
	}

	void put(message_ptr item) {
		std::unique_lock<std::mutex> lock(mutex_);
		putWaitingQueue(std::move(item));
		lock.unlock();
		condition_variable_.notify_one();
	}

private:
	message_ptr getWaitingQueue(const std::size_t & message_id) {
		auto it = std::partition(message_queue_.begin(), message_queue_.end(), [&message_id](const auto & e) {
			return e->getMessageTokenValue() != message_id;
		});
		assert(it != message_queue_.end());
		message_ptr message = std::move(*it);
		message_queue_.erase(it, it + 1);
		return message;
	}

	void putWaitingQueue(message_ptr item) { message_queue_.emplace_back(std::move(item)); }

private:
	std::mutex mutex_;
	std::deque<message_ptr> message_queue_;
	std::condition_variable condition_variable_;
};

class WaitingCacheMachine : public CacheMachine {
public:
	WaitingCacheMachine(unsigned long long gpuMemory,
		std::vector<unsigned long long> memoryPerCache,
		std::vector<CacheDataType> cachePolicyTypes_); 

	~WaitingCacheMachine();

	void addToCache(std::unique_ptr<ral::frame::BlazingTable> table) override;

	std::unique_ptr<ral::frame::BlazingTable> pullFromCache() override;

private:
	std::vector<std::unique_ptr<WaitingQueue<CacheData>>> waitingCache;
	WaitingQueue<ral::frame::BlazingTable> waitingGpuData;
};

class WorkerThread {
public:
	WorkerThread() {}
	virtual ~WorkerThread() {}
	virtual bool process() = 0;

protected:
	blazingdb::manager::experimental::Context * context;
	std::string queryString;
	bool _paused;
};

template <typename Processor>
class SingleSourceWorkerThread : public WorkerThread {
public:
	SingleSourceWorkerThread(std::shared_ptr<CacheMachine> cacheSource,
		std::shared_ptr<CacheMachine> cacheSink,
		std::string queryString,
		Processor * processor,
		blazingdb::manager::experimental::Context * context)
		: source(cacheSource), sink(cacheSink), WorkerThread() {
		this->context = context;
		this->queryString = queryString;
		this->_paused = false;
		this->processor = processor;
	}

	// returns true when theres nothing left to process
	bool process() override {
		if(_paused || source->finished()) {
			return false;
		}
		auto input = source->pullFromCache();
		if(input == nullptr) {
			return true;
		}
		auto output = this->processor(input->toBlazingTableView(), queryString, nullptr);
		sink->addToCache(std::move(output));
		return process();
	}
	void resume() { _paused = false; }
	void pause() { _paused = true; }
	virtual ~SingleSourceWorkerThread() {}

private:
	std::shared_ptr<CacheMachine> source;
	std::shared_ptr<CacheMachine> sink;
	Processor * processor;
};
//
////Has two sources, waits until at least one is complte before proceeding
template <typename Processor>
class DoubleSourceWaitingWorkerThread : public WorkerThread {
public:
	DoubleSourceWaitingWorkerThread(
		std::shared_ptr<WaitingCacheMachine> cacheSourceOne,
		std::shared_ptr<WaitingCacheMachine> cacheSourceTwo,
		std::shared_ptr<CacheMachine> cacheSink,
		std::string queryString,
		Processor * processor,
		blazingdb::manager::experimental::Context * context)
		: sourceOne(cacheSourceOne), sourceTwo(cacheSourceTwo), sink(cacheSink) {
		this->context = context;
		this->queryString = queryString;
		this->_paused = false;
		this->processor = processor;
	}
	virtual ~DoubleSourceWaitingWorkerThread() {}

	// returns true when theres nothing left to process
	bool process() override {
		if(_paused || sourceOne->finished()) {
			return false;
		}
		auto inputOne = sourceOne->pullFromCache();

		if(_paused || sourceTwo->finished()) {
			return false;
		}
		auto inputTwo = sourceTwo->pullFromCache();

		auto output = this->processor(this->context, inputOne->toBlazingTableView(), inputTwo->toBlazingTableView(), queryString);
		sink->addToCache(std::move(output));
	}

	void resume() { _paused = false; }

	void pause() { _paused = true; }

private:
	std::shared_ptr<WaitingCacheMachine> sourceOne;
	std::shared_ptr<WaitingCacheMachine> sourceTwo;
	std::shared_ptr<CacheMachine> sink;
	Processor * processor;
};

template <typename Processor>
class ProcessMachine {
public:
	ProcessMachine(std::shared_ptr<CacheMachine> cacheSource,
		std::shared_ptr<CacheMachine> cacheSink,
		Processor * processor,
		std::string queryString,
		blazingdb::manager::experimental::Context * context,
		int numWorkers)
		: source(cacheSource), sink(cacheSink), context(context), queryString(queryString), numWorkers(numWorkers) {
		for(int i = 0; i < numWorkers; i++) {
			auto thread = std::make_unique<SingleSourceWorkerThread<Processor>>(
				cacheSource, cacheSink, queryString, processor, context);
			workerThreads.emplace_back(std::move(thread));
		}
	}
	ProcessMachine(std::shared_ptr<WaitingCacheMachine> cacheSourceOne,
		std::shared_ptr<WaitingCacheMachine> cacheSourceTwo,
		std::shared_ptr<CacheMachine> cacheSink,
		Processor * processor,
		std::string queryString,
		blazingdb::manager::experimental::Context * context,
		int numWorkers)
		: sink(cacheSink), context(context), queryString(queryString), numWorkers(numWorkers) {
		for(int i = 0; i < numWorkers; i++) {
			auto thread = std::make_unique<DoubleSourceWaitingWorkerThread<Processor>>(
				cacheSourceOne, cacheSourceTwo, cacheSink, queryString, processor, context);
			workerThreads.emplace_back(std::move(thread));
		}
	}
	void run();
	void adjustWorkerCount(int numWorkers);

private:
	std::shared_ptr<CacheMachine> source;
	std::shared_ptr<CacheMachine> sink;
	std::vector<std::unique_ptr<WorkerThread>> workerThreads;
	int numWorkers;
	blazingdb::manager::experimental::Context * context;
	std::string queryString;
};

template <typename Processor>
void ProcessMachine<Processor>::run() {
	std::vector<std::thread> threads;
	for(int threadIndex = 0; threadIndex < numWorkers; threadIndex++) {
		std::thread t([this, threadIndex] { this->workerThreads[threadIndex]->process(); });
		threads.push_back(std::move(t));
	}
	for(auto & thread : threads) {
		thread.join();
	}
}


}  // namespace cache

}  // namespace ral
