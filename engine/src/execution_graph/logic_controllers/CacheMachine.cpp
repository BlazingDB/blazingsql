#include "CacheMachine.h"
#include <sys/stat.h>
#include <random>
#include <src/utilities/CommonOperations.h>
#include "cudf/column/column_factories.hpp"
#include "distribution/primitives.h"

namespace ral {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CacheMachine::CacheMachine(unsigned long long gpuMemory,
						   std::vector<unsigned long long> memoryPerCache_,
						   std::vector<CacheDataType> cachePolicyTypes_)
	{
	waitingCache = std::make_unique<WaitingQueue<CacheData>>();
	this->memoryPerCache.push_back(gpuMemory);
	for(auto mem : memoryPerCache_) {
		this->memoryPerCache.push_back(mem);
	}

	this->usedMemory.resize(cachePolicyTypes_.size() + 1, 0UL);
	this->cachePolicyTypes.push_back(GPU);
	for(auto policy : cachePolicyTypes_) {
		this->cachePolicyTypes.push_back(policy);
	}
}

CacheMachine::~CacheMachine() {}


void CacheMachine::finish() {
	this->waitingCache->notify();
}
void CacheMachine::addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table) {
	auto cacheIndex = 1;
	if(this->cachePolicyTypes[cacheIndex] == CPU) {
		auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
		std::unique_ptr<message<CacheData>> item =
			std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex);
		this->waitingCache->put(std::move(item));
	}else {
		assert(false);
	}
}

void CacheMachine::insert(std::unique_ptr<ral::frame::BlazingTable> table, size_t message_id) {
	this->addToCache(std::move(table), message_id);
}


void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table, size_t message_id) {
	int cacheIndex = 0;
	while(cacheIndex < memoryPerCache.size()) {
		if(usedMemory[cacheIndex] <= (memoryPerCache[cacheIndex] + table->sizeInBytes())) {
			usedMemory[cacheIndex] += table->sizeInBytes();
			if(cacheIndex == 0) {
				// before we put into a cache, we need to make sure we fully own the table
				auto column_names = table->names();
				auto cudf_table = table->releaseCudfTable();
				std::unique_ptr<ral::frame::BlazingTable> fully_owned_table = 
					std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), column_names);

				auto cache_data = std::make_unique<GPUCacheData>(std::move(fully_owned_table));
				std::unique_ptr<message<CacheData>> item =
					std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex);
				this->waitingCache->put(std::move(item));

			} else {
				if(this->cachePolicyTypes[cacheIndex] == CPU) {
					auto cache_data = std::make_unique<CPUCacheData>(std::move(table));
					std::unique_ptr<message<CacheData>> item =
						std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex);
					this->waitingCache->put(std::move(item));
				} else if(this->cachePolicyTypes[cacheIndex] == LOCAL_FILE) {
					std::thread t([table = std::move(table), this, cacheIndex]() mutable {
					  auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table));
					  std::unique_ptr<message<CacheData>> item =
						  std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex);
					  this->waitingCache->put(std::move(item));
					  // NOTE: Wait don't kill the main process until the last thread is finished!
					});
					t.detach();
				}
			}
			break;
		}
		cacheIndex++;
	}
	assert(cacheIndex < memoryPerCache.size());
}

bool CacheMachine::is_finished() {
	if(not waitingCache->empty()) {
		return false;
	}
	return waitingCache->is_finished();
}


std::unique_ptr<ral::frame::BlazingTable> CacheMachine::get_or_wait(size_t index) {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->get_or_wait(index);
	if (message_data == nullptr) {
		return nullptr;
	}
	auto cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	usedMemory[cache_index] -= cache_data->sizeInBytes();
	return std::move(cache_data->decache());
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache() {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->pop_or_wait();
	if (message_data == nullptr) {
		return nullptr;
	}
	auto cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	usedMemory[cache_index] -= cache_data->sizeInBytes();
	return std::move(cache_data->decache());
}


NonWaitingCacheMachine::NonWaitingCacheMachine(unsigned long long gpuMemory,
													 std::vector<unsigned long long> memoryPerCache,
													 std::vector<CacheDataType> cachePolicyTypes_)
	: CacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes_)
{
}

std::unique_ptr<ral::frame::BlazingTable> NonWaitingCacheMachine::pullFromCache() {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->pop();
	auto cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	usedMemory[cache_index] -= cache_data->sizeInBytes();
	return std::move(cache_data->decache());
}


ConcatenatingCacheMachine::ConcatenatingCacheMachine(unsigned long long gpuMemory,
													 std::vector<unsigned long long> memoryPerCache,
													 std::vector<CacheDataType> cachePolicyTypes_)
	: CacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes_)
{
}

std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache() {
	std::vector<std::unique_ptr<ral::frame::BlazingTable>> holder_samples;
	std::vector<ral::frame::BlazingTableView> samples;
	auto all_messages_data = waitingCache->get_all_or_wait();
	for (auto& message_data : all_messages_data) {
		auto cache_data = message_data->releaseData();
		auto cache_index = message_data->cacheIndex();
		usedMemory[cache_index] -= cache_data->sizeInBytes();
		auto tmp_frame = cache_data->decache();
		samples.emplace_back(tmp_frame->toBlazingTableView());
		holder_samples.emplace_back(std::move(tmp_frame));
	}
	return ral::utilities::experimental::concatTables(samples);
}
}  // namespace cache
} // namespace ral