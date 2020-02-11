

#include "LogicPrimitives.h"
#include <sys/stat.h>


#include <random>

#include "cudf/column/column_factories.hpp"

namespace ral {

namespace frame {


BlazingTable::BlazingTable(std::unique_ptr<CudfTable> table, std::vector<std::string> columnNames)
	: table(std::move(table)), columnNames(columnNames) {}

CudfTableView BlazingTable::view() const { return this->table->view(); }

std::vector<std::string> BlazingTable::names() const { return this->columnNames; }

BlazingTableView BlazingTable::toBlazingTableView() const {
	return BlazingTableView(this->table->view(), this->columnNames);
}


BlazingTableView::BlazingTableView() {}

BlazingTableView::BlazingTableView(CudfTableView table, std::vector<std::string> columnNames)
	: table(table), columnNames(columnNames) {}

CudfTableView BlazingTableView::view() const { return this->table; }

std::vector<std::string> BlazingTableView::names() const { return this->columnNames; }

std::unique_ptr<BlazingTable> BlazingTableView::clone() const {
	std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(this->table);
	return std::make_unique<BlazingTable>(std::move(cudfTable), this->columnNames);
}

TableViewPair createEmptyTableViewPair(std::vector<cudf::type_id> column_types, std::vector<std::string> column_names) {
	std::vector<std::unique_ptr<cudf::column>> empty_columns;
	empty_columns.resize(column_types.size());
	for(int i = 0; i < column_types.size(); ++i) {
		cudf::type_id col_type = column_types[i];
		cudf::data_type dtype(col_type);
		std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
		empty_columns[i] = std::move(empty_column);
	}

	std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(std::move(empty_columns));
	std::unique_ptr<BlazingTable> table = std::make_unique<BlazingTable>(std::move(cudf_table), column_names);

	TableViewPair ret;
	ret.first = std::move(table);
	ret.second = ret.first->toBlazingTableView();

	return ret;
}

}  // namespace frame

namespace cache {

std::string randomString(std::size_t length) {
	const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	std::random_device random_device;
	std::mt19937 generator(random_device());
	std::uniform_int_distribution<> distribution(0, characters.size() - 1);

	std::string random_string;

	for(std::size_t i = 0; i < length; ++i) {
		random_string += characters[distribution(generator)];
	}

	return random_string;
}

unsigned long long CacheDataLocalFile::sizeInBytes() {
	struct stat st;

	if(stat(this->filePath_.c_str(), &st) == 0)
		return (st.st_size);
	else
		throw;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataLocalFile::decache() {
	cudf_io::read_orc_args in_args{cudf_io::source_info{this->filePath_}};
	auto result = cudf_io::read_orc(in_args);
	return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), this->names);
}

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table) {
	// TODO: make this configurable
	this->filePath_ = "/tmp/.blazing-temp-" + randomString(64) + ".orc";
	this->names = table->names();
	std::cout << "CacheDataLocalFile: " << this->filePath_ << std::endl;
	cudf_io::table_metadata metadata;
	for(auto name : table->names()) {
		metadata.column_names.emplace_back(name);
	}
	cudf_io::write_orc_args out_args(cudf_io::sink_info{this->filePath_}, table->view(), &metadata);

	cudf_io::write_orc(out_args);
}


bool CacheMachine::finished() {
	if(gpuData.size() > 0) {
		return false;
	}
	for(int cacheIndex = 1; cacheIndex < cache.size(); cacheIndex++) {
		if(cache.size() > 0) {
			return false;
		}
	}
	return this->_finished;
}

void CacheMachine::finish() { this->_finished = true; }

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache() {
	{
		std::lock_guard<std::mutex> lock(cacheMutex);
		if(gpuData.size() > 0) {
			auto data = std::move(gpuData.front());
			gpuData.pop();
			usedMemory[0] -= data->sizeInBytes();
			return std::move(data);	 // blocks  until it can do this, can return nullptr
		}
	}

	// a different option would be to use cv.notify here
	for(int cacheIndex = 1; cacheIndex < memoryPerCache.size(); cacheIndex++) {
		{
			std::lock_guard<std::mutex> lock(cacheMutex);
			if(cache[cacheIndex]->size() > 0) {
				auto data = std::move(cache[cacheIndex]->front());
				cache[cacheIndex]->pop();
				usedMemory[cacheIndex] -= data->sizeInBytes();
				return std::move(data->decache());
			}
		}
	}
	// there are no chunks
	// make condition variable  and Wait
	// for now hack it by calling pullFromCache again
	if(_finished) {
		return nullptr;
	}
	// TODO: Fix or ask @felipe latter. TODO fix next line
	if (gpuData.size() == 0) { _finished = true; }
	return pullFromCache();
}

void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table) {
	for(int cacheIndex = 0; cacheIndex < memoryPerCache.size(); cacheIndex++) {
		if(usedMemory[cacheIndex] <= (memoryPerCache[cacheIndex] + table->sizeInBytes())) {
			usedMemory[cacheIndex] += table->sizeInBytes();
			if(cacheIndex == 0) {
				std::lock_guard<std::mutex> lock(cacheMutex);
				gpuData.push(std::move(table));
			} else {
				std::thread t([table = std::move(table), this, cacheIndex]() mutable {
					if(this->cachePolicyTypes[cacheIndex] == LOCAL_FILE) {
						std::lock_guard<std::mutex> lock(cacheMutex);
						auto item = std::make_unique<CacheDataLocalFile>(std::move(table));
						this->cache[cacheIndex]->push(std::move(item));
						// NOTE: Wait don't kill the main process until the last thread is finished!
					}
				});
				t.detach();
			}
			break;
		}
	}
}

CacheMachine::~CacheMachine() {}

CacheMachine::CacheMachine(unsigned long long gpuMemory,
	std::vector<unsigned long long> memoryPerCache_,
	std::vector<CacheDataType> cachePolicyTypes_)
	: _finished(false) {
	this->cache.resize(cachePolicyTypes_.size() + 1);
	for(size_t i = 0; i < cache.size(); i++) {
		this->cache[i] = std::make_unique<std::queue<std::unique_ptr<CacheData>>>();
	}
	this->memoryPerCache.push_back(gpuMemory);
	for(auto mem : memoryPerCache_) {
		this->memoryPerCache.push_back(mem);
	}

	this->usedMemory.resize(cache.size(), 0UL);
	this->cachePolicyTypes.push_back(GPU);
	for(auto policy : cachePolicyTypes_) {
		this->cachePolicyTypes.push_back(policy);
	}
}


WaitingCacheMachine::WaitingCacheMachine(unsigned long long gpuMemory,
	std::vector<unsigned long long> memoryPerCache,
	std::vector<CacheDataType> cachePolicyTypes_)
	: CacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes_) {
	waitingCache.resize(cachePolicyTypes_.size() + 1);
	for(size_t i = 0; i < waitingCache.size(); i++) {
		waitingCache[i] = std::make_unique<WaitingQueue<CacheData>>();
	}
}

WaitingCacheMachine::~WaitingCacheMachine() {}

void WaitingCacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table) {
	for(int cacheIndex = 0; cacheIndex < memoryPerCache.size(); cacheIndex++) {
		if(usedMemory[cacheIndex] <= (memoryPerCache[cacheIndex] + table->sizeInBytes())) {
			usedMemory[cacheIndex] += table->sizeInBytes();
			if(cacheIndex == 0) {
				auto item = std::make_unique<message<ral::frame::BlazingTable>>(std::move(table));
				waitingGpuData.put(std::move(item));
			} else {
				std::thread t([table = std::move(table), this, cacheIndex]() mutable {
					if(this->cachePolicyTypes[cacheIndex] == LOCAL_FILE) {
						auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table));
						std::unique_ptr<message<CacheData>> item =
							std::make_unique<message<CacheData>>(std::move(cache_data));
						this->waitingCache[cacheIndex]->put(std::move(item));
						// NOTE: Wait don't kill the main process until the last thread is finished!
					}
				});
				t.detach();
			}
			break;
		}
	}
}

std::unique_ptr<ral::frame::BlazingTable> WaitingCacheMachine::pullFromCache() {
	{
		// TODO: @alex: Fix this: We should use here a unique queue. In order to use the latest element in que queue.
		auto data = waitingGpuData.get();
		auto frame = data->releaseData();
		usedMemory[0] -= frame->sizeInBytes();
		return std::move(frame);
	}
	for(int cacheIndex = 1; cacheIndex < memoryPerCache.size(); cacheIndex++) {
		if(not waitingCache[cacheIndex]->empty()) {
			auto data = std::move(waitingCache[cacheIndex]->get());
			auto cache_data = data->releaseData();
			usedMemory[cacheIndex] -= cache_data->sizeInBytes();
			return std::move(cache_data->decache());
		}
	}
}


}  // namespace cache

}  // namespace ral
