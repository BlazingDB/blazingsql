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

size_t CacheDataLocalFile::sizeInBytes() const {
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
	: CacheData(CacheDataType::LOCAL_FILE, table->names(), table->get_schema(), table->num_rows()) 
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
	waitingCache = std::make_unique<WaitingQueue>();
	this->memory_resources.push_back( &blazing_device_memory_resource::getInstance() ); 
	this->memory_resources.push_back( &blazing_host_memory_mesource::getInstance() ); 
	this->memory_resources.push_back( &blazing_disk_memory_resource::getInstance() );
	this->num_bytes_added = 0;
	this->num_rows_added = 0;
	this->flow_control_batches_threshold = 0;
	this->flow_control_bytes_threshold = 0;
	this->flow_control_batches_count = 0;
	this->flow_control_bytes_count = 0;

	logger = spdlog::get("batch_logger");
}

CacheMachine::CacheMachine(std::uint32_t flow_control_batches_threshold, std::size_t flow_control_bytes_threshold)
{
	waitingCache = std::make_unique<WaitingQueue>();
	this->memory_resources.push_back( &blazing_device_memory_resource::getInstance() ); 
	this->memory_resources.push_back( &blazing_host_memory_mesource::getInstance() ); 
	this->memory_resources.push_back( &blazing_disk_memory_resource::getInstance() );
	this->num_bytes_added = 0;
	this->num_rows_added = 0;
	this->flow_control_batches_threshold = flow_control_batches_threshold;
	this->flow_control_bytes_threshold = flow_control_bytes_threshold;
	this->flow_control_batches_count = 0;
	this->flow_control_bytes_count = 0;

	logger = spdlog::get("batch_logger");
	something_added = false;
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

void CacheMachine::addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, const std::string & message_id, Context * ctx) {
	
	// we dont want to add empty tables to a cache, unless we have never added anything
	if (!this->something_added || host_table->num_rows() > 0){
		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
									"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
									"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
									"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
									"info"_a="Add to CacheMachine",
									"duration"_a="",
									"kernel_id"_a=message_id,
									"rows"_a=host_table->num_rows());

		std::unique_lock<std::mutex> lock(flow_control_mutex);
		flow_control_batches_count++;
		flow_control_bytes_count += host_table->sizeInBytes();
		lock.unlock();

		num_rows_added += host_table->num_rows();
		num_bytes_added += host_table->sizeInBytes();
		auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
		auto item =	std::make_unique<message>(std::move(cache_data), message_id);
		this->waitingCache->put(std::move(item));
		this->something_added = true;
	}
}

void CacheMachine::put(size_t message_id, std::unique_ptr<ral::frame::BlazingTable> table) {
	this->addToCache(std::move(table), std::to_string(message_id));
}

void CacheMachine::clear() {
	std::unique_ptr<message> message_data;
	while(message_data = waitingCache->pop_or_wait()) {
		printf("...cleaning cache\n");
	}
	this->waitingCache->finish();
}

void CacheMachine::addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, const std::string & message_id, Context * ctx){
	
	// we dont want to add empty tables to a cache, unless we have never added anything
	if (!this->something_added || cache_data->num_rows() > 0){
		std::unique_lock<std::mutex> lock(flow_control_mutex);
		flow_control_batches_count++;
		flow_control_bytes_count += cache_data->sizeInBytes();
		lock.unlock();

		num_rows_added += cache_data->num_rows();
		num_bytes_added += cache_data->sizeInBytes();
		int cacheIndex = 0;
		while(cacheIndex < this->memory_resources.size()) {
			auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() + cache_data->sizeInBytes());
			if( memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit()) {
				if(cacheIndex == 0) {
					logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
						"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
						"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
						"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
						"info"_a="Add to CacheMachine general CacheData object into GPU cache ",
						"duration"_a="",
						"kernel_id"_a=message_id,
						"rows"_a=cache_data->num_rows());
			
					auto item = std::make_unique<message>(std::move(cache_data), message_id);
					this->waitingCache->put(std::move(item));
				} else {
					if(cacheIndex == 1) {
			logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
							"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
							"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
							"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
							"info"_a="Add to CacheMachine general CacheData object into CPU cache ",
							"duration"_a="",
							"kernel_id"_a=message_id,
							"rows"_a=cache_data->num_rows());
			
						auto item = std::make_unique<message>(std::move(cache_data), message_id);
						this->waitingCache->put(std::move(item));
					} else if(cacheIndex == 2) {
						logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
							"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
							"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
							"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
							"info"_a="Add to CacheMachine general CacheData object into Disk cache ",
							"duration"_a="",
							"kernel_id"_a=message_id,
							"rows"_a=cache_data->num_rows());

						// BlazingMutableThread t([cache_data = std::move(cache_data), this, cacheIndex, message_id]() mutable {
						auto item = std::make_unique<message>(std::move(cache_data), message_id);
						this->waitingCache->put(std::move(item));
						// NOTE: Wait don't kill the main process until the last thread is finished!
						// }); t.detach();
					}
				}
				break;
			}
			cacheIndex++;
		}
		this->something_added = true;
	}
}

void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table, const std::string & message_id, Context * ctx) {

	// we dont want to add empty tables to a cache, unless we have never added anything
	if (!this->something_added || table->num_rows() > 0){
		for (auto col_ind = 0; col_ind < table->num_columns(); col_ind++){
			if (table->view().column(col_ind).offset() > 0){
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|offset|{offset}",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="Add to CacheMachine into cache table column " + table->names()[col_ind] + " has offset",
								"duration"_a="",
								"kernel_id"_a=message_id,
								"offset"_a=table->view().column(col_ind).offset());
				logger->flush();
			}
		}

		std::unique_lock<std::mutex> lock(flow_control_mutex);
		flow_control_batches_count++;
		flow_control_bytes_count += table->sizeInBytes();
		lock.unlock();
		
		num_rows_added += table->num_rows();
		num_bytes_added += table->sizeInBytes();
		int cacheIndex = 0;
		while(cacheIndex < memory_resources.size()) {
			auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() + table->sizeInBytes());
			if( memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit()) {
				if(cacheIndex == 0) {
					logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
						"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
						"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
						"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
						"info"_a="Add to CacheMachine into GPU cache",
						"duration"_a="",
						"kernel_id"_a=message_id,
						"rows"_a=table->num_rows());

					// before we put into a cache, we need to make sure we fully own the table
					auto column_names = table->names();
					auto cudf_table = table->releaseCudfTable();
					std::unique_ptr<ral::frame::BlazingTable> fully_owned_table = 
						std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), column_names);

					auto cache_data = std::make_unique<GPUCacheData>(std::move(fully_owned_table));
					auto item =	std::make_unique<message>(std::move(cache_data), message_id);
					this->waitingCache->put(std::move(item));
				} else {
					if(cacheIndex == 1) {
						logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
							"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
							"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
							"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
							"info"_a="Add to CacheMachine into CPU cache",
							"duration"_a="",
							"kernel_id"_a=message_id,
							"rows"_a=table->num_rows());

						auto cache_data = std::make_unique<CPUCacheData>(std::move(table));
						auto item =	std::make_unique<message>(std::move(cache_data), message_id);
						this->waitingCache->put(std::move(item));
					} else if(cacheIndex == 2) {
						logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
							"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
							"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
							"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
							"info"_a="Add to CacheMachine into Disk cache",
							"duration"_a="",
							"kernel_id"_a=message_id,
							"rows"_a=table->num_rows());

						// BlazingMutableThread t([table = std::move(table), this, cacheIndex, message_id]() mutable {
						auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table));
						auto item =	std::make_unique<message>(std::move(cache_data), message_id);
						this->waitingCache->put(std::move(item));
						// NOTE: Wait don't kill the main process until the last thread is finished!
						// });t.detach();
					}
				}
				break;
			}
			cacheIndex++;
		}
		this->something_added = true;
	}
}

void CacheMachine::wait_until_finished() {
	return waitingCache->wait_until_finished();
}


std::unique_ptr<ral::frame::BlazingTable> CacheMachine::get_or_wait(size_t index) {
	std::unique_ptr<message> message_data = waitingCache->get_or_wait(std::to_string(index));
	if (message_data == nullptr) {
		return nullptr;
	}
	
	std::unique_ptr<ral::frame::BlazingTable> output = message_data->get_data().decache();
	std::unique_lock<std::mutex> lock(flow_control_mutex);
	flow_control_batches_count--;
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache(Context * ctx) {
	std::unique_ptr<message> message_data = waitingCache->pop_or_wait();
	if (message_data == nullptr) {
		return nullptr;
	}

	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="Pull from CacheMachine type {}"_format(static_cast<int>(message_data->get_data().get_type())),
								"duration"_a="",
								"kernel_id"_a=message_data->get_message_id(),
								"rows"_a=message_data->get_data().num_rows());

	std::unique_ptr<ral::frame::BlazingTable> output = message_data->get_data().decache();
	std::unique_lock<std::mutex> lock(flow_control_mutex);
	flow_control_batches_count--;
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}

std::unique_ptr<ral::cache::CacheData> CacheMachine::pullCacheData(Context * ctx) {
	std::unique_ptr<message> message_data = waitingCache->pop_or_wait();
	if (message_data == nullptr) {
		return nullptr;
	}

	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="Pull from CacheMachine CacheData object type {}"_format(static_cast<int>(message_data->get_data().get_type())),
								"duration"_a="",
								"kernel_id"_a=message_data->get_message_id(),
								"rows"_a=message_data->get_data().num_rows());

	std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();
	std::unique_lock<std::mutex> lock(flow_control_mutex);
	flow_control_batches_count--;
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}

bool CacheMachine::thresholds_are_met(std::uint32_t batches_count, std::size_t bytes_count){
	return batches_count > this->flow_control_batches_threshold && bytes_count > this->flow_control_bytes_threshold;
}

void CacheMachine::wait_if_cache_is_saturated() {

	logger->trace("|||{info}||kernel_id||rows|",
								"info"_a="wait_if_cache_is_saturated start",
								);

	std::unique_lock<std::mutex> lock(flow_control_mutex);
	flow_control_condition_variable.wait(lock, [&, this] { 
		return !thresholds_are_met(flow_control_batches_count, flow_control_bytes_count);
	});

	logger->trace("|||{info}||kernel_id||rows|",
								"info"_a="wait_if_cache_is_saturated end",
								);
}














ConcatenatingCacheMachine::ConcatenatingCacheMachine(std::uint32_t flow_control_batches_threshold, std::size_t flow_control_bytes_threshold)
	: CacheMachine( flow_control_batches_threshold, flow_control_bytes_threshold)
{
}



// This method does not guarantee the relative order of the messages to be preserved
std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache(Context * ctx) {
	
	logger->trace("|||{info}||kernel_id||rows|",
								"info"_a="ConcatenatingCacheMachine::pullFromCache start",
								);
	
	
	size_t total_bytes = 0;
	std::vector<std::unique_ptr<message>> collected_messages;
	std::unique_ptr<message> message_data;
	std::string message_id = "";
	while (message_data = waitingCache->pop_or_wait())
	{
		auto& cache_data = message_data->get_data();
		if (collected_messages.empty() || !thresholds_are_met(collected_messages.size(), total_bytes + cache_data.sizeInBytes())) {
			total_bytes += cache_data.sizeInBytes();
			message_id = message_data->get_message_id();
			collected_messages.push_back(std::move(message_data));
		} else {
			waitingCache->put(std::move(message_data));
			break;
		}
	}
	std::uint32_t output_batches_count = collected_messages.size();
	std::size_t output_bytes_count = 0;
	std::unique_ptr<ral::frame::BlazingTable> output;
	if(collected_messages.empty()){
		output = nullptr;
	} else if (collected_messages.size() == 1) {
		auto data = collected_messages[0]->release_data();
		output_bytes_count += data->sizeInBytes();
		output = std::move(data->decache());		
	}	else {
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_holder(collected_messages.size());
		std::vector<ral::frame::BlazingTableView> table_views(collected_messages.size());
		for (int i = 0; i < collected_messages.size(); i++){
			auto data = collected_messages[i]->release_data();
			output_bytes_count += data->sizeInBytes();
			tables_holder[i] = std::move(data->decache());
			table_views[i] = tables_holder[i]->toBlazingTableView();
		}
		output = ral::utilities::experimental::concatTables(table_views);
	}	

	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="Pull from ConcatenatingCacheMachine",
								"duration"_a="",
								"kernel_id"_a=message_id,
								"rows"_a=output->num_rows());

	std::unique_lock<std::mutex> lock(flow_control_mutex);
	flow_control_batches_count -=  output_batches_count;
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}

}  // namespace cache
} // namespace ral
