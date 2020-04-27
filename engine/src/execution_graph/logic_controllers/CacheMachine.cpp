#include "CacheMachine.h"
#include <sys/stat.h>
#include <random>
#include <src/utilities/CommonOperations.h>
#include <src/utilities/DebuggingUtils.h>

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
	return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), this->names());
}

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table)
	: CacheData(table->names(), table->get_schema(), table->num_rows()) 
{
	// TODO: make this configurable
	this->filePath_ = "/tmp/.blazing-temp-" + randomString(64) + ".orc";
	std::cout << "CacheDataLocalFile: " << this->filePath_ << std::endl;
	cudf_io::table_metadata metadata;
	for(auto name : table->names()) {
		metadata.column_names.emplace_back(name);
	}
	cudf_io::write_orc_args out_args(cudf_io::sink_info{this->filePath_}, table->view(), &metadata);

	cudf_io::write_orc(out_args);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CacheMachine::CacheMachine()
{
	waitingCache = std::make_unique<WaitingQueue<CacheData>>();
	this->memory_resources.push_back( &blazing_device_memory_resource::getInstance() ); 
	this->memory_resources.push_back( &blazing_host_memory_mesource::getInstance() ); 
	this->memory_resources.push_back( &blazing_disk_memory_resource::getInstance() ); 
	this->num_bytes_added = 0;
	this->num_rows_added = 0;
}

CacheMachine::~CacheMachine() {}


void CacheMachine::finish() {
	this->waitingCache->finish();
}

bool CacheMachine::is_finished() {
	return this->waitingCache->is_finished();
}

uint64_t CacheMachine::get_num_bytes_added(){
	return num_bytes_added.load();
}

uint64_t CacheMachine::get_num_rows_added(){
	return num_rows_added.load();
}

void CacheMachine::addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string message_id) {
	// std::cout<<"addHostFrameToCache "<<message_id<<std::endl;
	// std::cout<<"num_rows: "<<host_table->num_rows()<<std::endl;
	// for (auto name : host_table->names()){
	// 	std::cout<<name<<std::endl;
	// }
	num_rows_added += host_table->num_rows();
	num_bytes_added += host_table->sizeInBytes();
	auto cacheIndex = 1; // HOST MEMORY
	auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
	std::unique_ptr<message<CacheData>> item =
		std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
	this->waitingCache->put(std::move(item));
}

void CacheMachine::put(size_t message_id, std::unique_ptr<ral::frame::BlazingTable> table) {
	this->addToCache(std::move(table), std::to_string(message_id));
}

void CacheMachine::addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, std::string message_id){
	// std::cout<<"addCacheData "<<message_id<<std::endl;
	// std::cout<<"num_rows: "<<cache_data->num_rows()<<std::endl;
	// for (auto name : cache_data->names()){
	// 	std::cout<<name<<std::endl;
	// }

	num_rows_added += cache_data->num_rows();
	num_bytes_added += cache_data->sizeInBytes();
	int cacheIndex = 0;
	while(cacheIndex < this->memory_resources.size()) {
		auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() + cache_data->sizeInBytes());
		if( memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit()) {
			if(cacheIndex == 0) {
				std::unique_ptr<message<CacheData>> item =
					std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
				this->waitingCache->put(std::move(item));
			} else {
				if(cacheIndex == 1) {
					std::unique_ptr<message<CacheData>> item =
						std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
					this->waitingCache->put(std::move(item));
				} else if(cacheIndex == 2) {
					// BlazingMutableThread t([cache_data = std::move(cache_data), this, cacheIndex, message_id]() mutable {
					  std::unique_ptr<message<CacheData>> item =
						  std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
					  this->waitingCache->put(std::move(item));
					  // NOTE: Wait don't kill the main process until the last thread is finished!
					// }); t.detach();
				}
			}
			break;
		}
		cacheIndex++;
	}
}

void CacheMachine::clear() {
	std::unique_ptr<message<CacheData>> message_data;
	while(message_data = waitingCache->pop_or_wait()) {
		printf("...cleaning cache\n");
	}
	this->waitingCache->finish();
}

void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table, std::string message_id) {
	// std::cout<<"addToCache "<<message_id<<std::endl;
 	// ral::utilities::print_blazing_table_view_schema(table->toBlazingTableView(), message_id);
	num_rows_added += table->num_rows();
	num_bytes_added += table->sizeInBytes();
	int cacheIndex = 0;
	while(cacheIndex < memory_resources.size()) {
		auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() + table->sizeInBytes());
		if( memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit()) {
			if(cacheIndex == 0) {
				// before we put into a cache, we need to make sure we fully own the table
				auto column_names = table->names();
				auto cudf_table = table->releaseCudfTable();
				std::unique_ptr<ral::frame::BlazingTable> fully_owned_table = 
					std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), column_names);

				auto cache_data = std::make_unique<GPUCacheData>(std::move(fully_owned_table));
				std::unique_ptr<message<CacheData>> item =
					std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
				this->waitingCache->put(std::move(item));

			} else {
				if(cacheIndex == 1) {
					auto cache_data = std::make_unique<CPUCacheData>(std::move(table));
					std::unique_ptr<message<CacheData>> item =
						std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
					this->waitingCache->put(std::move(item));
				} else if(cacheIndex == 2) {
					// BlazingMutableThread t([table = std::move(table), this, cacheIndex, message_id]() mutable {
					  auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table));
					  std::unique_ptr<message<CacheData>> item =
						  std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex, message_id);
					  this->waitingCache->put(std::move(item));
					  // NOTE: Wait don't kill the main process until the last thread is finished!
					// });t.detach();
				}
			}
			break;
		}
		cacheIndex++;
	}
}

bool CacheMachine::ready_to_execute() {
	return waitingCache->ready_to_execute();
}


std::unique_ptr<ral::frame::BlazingTable> CacheMachine::get_or_wait(size_t index) {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->get_or_wait(std::to_string(index));
	if (message_data == nullptr) {
		return nullptr;
	}
	auto cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	return std::move(cache_data->decache());
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache() {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->pop_or_wait();
	if (message_data == nullptr) {
		return nullptr;
	}
	auto cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	
	return std::move(cache_data->decache());	
}

std::unique_ptr<ral::cache::CacheData> CacheMachine::pullCacheData() {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->pop_or_wait();
	if (message_data == nullptr) {
		return nullptr;
	}
	std::unique_ptr<ral::cache::CacheData> cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	return std::move(cache_data);
}

NonWaitingCacheMachine::NonWaitingCacheMachine()
	: CacheMachine()
{
}

std::unique_ptr<ral::frame::BlazingTable> NonWaitingCacheMachine::pullFromCache() {
	std::unique_ptr<message<CacheData>> message_data = waitingCache->pop();
	auto cache_data = message_data->releaseData();
	auto cache_index = message_data->cacheIndex();
	return std::move(cache_data->decache());
}


ConcatenatingCacheMachine::ConcatenatingCacheMachine()
	: CacheMachine()
{
}

std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache() {
	std::vector<std::unique_ptr<ral::frame::BlazingTable>> holder_samples;
	std::vector<ral::frame::BlazingTableView> samples;
	auto all_messages_data = waitingCache->get_all_or_wait();
	for (auto& message_data : all_messages_data) {
		auto cache_data = message_data->releaseData();
		auto cache_index = message_data->cacheIndex();
		auto tmp_frame = cache_data->decache();
		samples.emplace_back(tmp_frame->toBlazingTableView());
		holder_samples.emplace_back(std::move(tmp_frame));
	}

	auto out = ral::utilities::experimental::concatTables(samples);
	return out;
}
}  // namespace cache
} // namespace ral
