#include "CacheMachine.h"
#include <sys/stat.h>
#include <random>
#include <utilities/CommonOperations.h>
#include <utilities/DebuggingUtils.h>
#include "communication/CommunicationData.h"
#include <stdio.h>

using namespace std::chrono_literals;
namespace ral {
namespace cache {

std::size_t CacheMachine::cache_count(900000000);

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

size_t CacheDataLocalFile::fileSizeInBytes() const {
	struct stat st;

	if(stat(this->filePath_.c_str(), &st) == 0)
		return (st.st_size);
	else
		throw;
}

size_t CacheDataLocalFile::sizeInBytes() const {
	return size_in_bytes;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataLocalFile::decache() {
	cudf_io::read_orc_args in_args{cudf_io::source_info{this->filePath_}};
	auto result = cudf_io::read_orc(in_args);

	// Remove temp orc files
	const char *orc_path_file = this->filePath_.c_str();
	remove(orc_path_file);
	return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), this->names());
}

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path)
	: CacheData(CacheDataType::LOCAL_FILE, table->names(), table->get_schema(), table->num_rows())
{
	this->size_in_bytes = table->sizeInBytes();
	this->filePath_ = orc_files_path + "/.blazing-temp-" + randomString(64) + ".orc";

	cudf_io::table_metadata metadata;
	for(auto name : table->names()) {
		metadata.column_names.emplace_back(name);
	}
	cudf_io::write_orc_args out_args(cudf_io::sink_info{this->filePath_}, table->view(), &metadata);

	cudf_io::write_orc(out_args);
}
std::unique_ptr<GPUCacheDataMetaData> cast_cache_data_to_gpu_with_meta(std::unique_ptr<CacheData> base_pointer){
	return std::unique_ptr<GPUCacheDataMetaData>(static_cast<GPUCacheDataMetaData *>(base_pointer.release()));
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CacheMachine::CacheMachine(std::shared_ptr<Context> context): ctx(context), cache_id(CacheMachine::cache_count)
{
	CacheMachine::cache_count++;

	waitingCache = std::make_unique<WaitingQueue>();
	this->memory_resources.push_back( &blazing_device_memory_resource::getInstance() );
	this->memory_resources.push_back( &blazing_host_memory_resource::getInstance() );
	this->memory_resources.push_back( &blazing_disk_memory_resource::getInstance() );
	this->num_bytes_added = 0;
	this->num_rows_added = 0;
	this->flow_control_bytes_threshold = std::numeric_limits<std::size_t>::max();
	this->flow_control_bytes_count = 0;

	logger = spdlog::get("batch_logger");
	cache_events_logger = spdlog::get("cache_events_logger");

	std::shared_ptr<spdlog::logger> kernels_logger;
	kernels_logger = spdlog::get("kernels_logger");

	kernels_logger->info("{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}",
							"ral_id"_a=(context ? context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1 ),
							"query_id"_a=(context ? std::to_string(context->getContextToken()) : "null"),
							"kernel_id"_a=cache_id,
							"is_kernel"_a=0, //false
							"kernel_type"_a="cache");
}

CacheMachine::CacheMachine(std::shared_ptr<Context> context, std::size_t flow_control_bytes_threshold) : ctx(context), cache_id(CacheMachine::cache_count)
{
	CacheMachine::cache_count++;

	waitingCache = std::make_unique<WaitingQueue>();
	this->memory_resources.push_back( &blazing_device_memory_resource::getInstance() );
	this->memory_resources.push_back( &blazing_host_memory_resource::getInstance() );
	this->memory_resources.push_back( &blazing_disk_memory_resource::getInstance() );
	this->num_bytes_added = 0;
	this->num_rows_added = 0;
	this->flow_control_bytes_threshold = flow_control_bytes_threshold;
	this->flow_control_bytes_count = 0;

	logger = spdlog::get("batch_logger");
	cache_events_logger = spdlog::get("cache_events_logger");

	something_added = false;

	std::shared_ptr<spdlog::logger> kernels_logger;
	kernels_logger = spdlog::get("kernels_logger");

	kernels_logger->info("{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}",
							"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
							"query_id"_a=(context ? std::to_string(context->getContextToken()) : "null"),
							"kernel_id"_a=cache_id,
							"is_kernel"_a=0, //false
							"kernel_type"_a="cache");
}

CacheMachine::~CacheMachine() {}

Context * CacheMachine::get_context() const {
	return ctx.get();
}

std::int32_t CacheMachine::get_id() const { return (cache_id); }

void CacheMachine::finish() {
	this->waitingCache->finish();
	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|cache_id|{cache_id}||",
									"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
									"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
									"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
									"info"_a="CacheMachine finish()",
									"duration"_a="",
									"cache_id"_a=cache_id);
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

void CacheMachine::addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, const std::string & message_id) {

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

void CacheMachine::addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, const std::string & message_id, bool always_add){

	// we dont want to add empty tables to a cache, unless we have never added anything
	if ((!this->something_added || cache_data->num_rows() > 0) || always_add){
		std::unique_lock<std::mutex> lock(flow_control_mutex);
		flow_control_bytes_count += cache_data->sizeInBytes();
		lock.unlock();

		num_rows_added += cache_data->num_rows();
		num_bytes_added += cache_data->sizeInBytes();
		int cacheIndex = 0;
		ral::cache::CacheDataType type = cache_data->get_type();
		if (type == ral::cache::CacheDataType::GPU || type == ral::cache::CacheDataType::GPU_METADATA){
			cacheIndex = 0;
		} else if (type == ral::cache::CacheDataType::GPU){
			cacheIndex = 1;
		} else {
			cacheIndex = 2;
		}

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
		} else if(cacheIndex == 1) {
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
		this->something_added = true;
	}
}

void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table, const std::string & message_id, bool always_add) {
	// we dont want to add empty tables to a cache, unless we have never added anything
	if (!this->something_added || table->num_rows() > 0|| always_add){
		for (auto col_ind = 0; col_ind < table->num_columns(); col_ind++){
			if (table->view().column(col_ind).offset() > 0){
				table->ensureOwnership();
				break;
			}

		}
		std::unique_lock<std::mutex> lock(flow_control_mutex);
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
					table->ensureOwnership();
					
					auto cache_data = std::make_unique<GPUCacheData>(std::move(table));
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
						// want to get only cache directory where orc files should be saved
						std::map<std::string, std::string> config_options = ctx->getConfigOptions();
						auto it = config_options.find("BLAZING_CACHE_DIRECTORY");
						std::string orc_files_path;
						if (it != config_options.end()) {
							orc_files_path = config_options["BLAZING_CACHE_DIRECTORY"];
						}
						auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table), orc_files_path);
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
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache() {
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
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}


std::unique_ptr<ral::cache::CacheData> CacheMachine::pullCacheData(std::string message_id) {
	std::unique_ptr<message> message_data = waitingCache->get_or_wait(message_id);
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
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullUnorderedFromCache() {

	std::unique_ptr<message> message_data = nullptr;
	{ // scope for lock
		std::unique_lock<std::mutex> lock = this->waitingCache->lock();
		std::vector<std::unique_ptr<message>> all_messages = this->waitingCache->get_all_unsafe();
		std::vector<std::unique_ptr<message>> remaining_messages;
		for(size_t i = 0; i < all_messages.size(); i++) {
			if (all_messages[i]->get_data().get_type() == CacheDataType::GPU && message_data == nullptr){
				message_data = std::move(all_messages[i]);
			} else {
				remaining_messages.push_back(std::move(all_messages[i]));
			}
		}
		this->waitingCache->put_all_unsafe(std::move(remaining_messages));
	}
	if (message_data){
		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="Pull Unordered from CacheMachine type {}"_format(static_cast<int>(message_data->get_data().get_type())),

								"duration"_a="",
								"kernel_id"_a=message_data->get_message_id(),
								"rows"_a=message_data->get_data().num_rows());


		std::unique_ptr<ral::frame::BlazingTable> output = message_data->get_data().decache();
		std::unique_lock<std::mutex> lock(flow_control_mutex);
		flow_control_bytes_count -= output->sizeInBytes();
		flow_control_condition_variable.notify_all();
		return std::move(output);
	} else {
		return pullFromCache();
	}
}

std::unique_ptr<ral::cache::CacheData> CacheMachine::pullCacheData() {
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
	flow_control_bytes_count -= output->sizeInBytes();
	flow_control_condition_variable.notify_all();
	return std::move(output);
}


bool CacheMachine::thresholds_are_met(std::size_t bytes_count){

	return bytes_count > this->flow_control_bytes_threshold;
}

void CacheMachine::wait_if_cache_is_saturated() {

	CodeTimer blazing_timer;

	std::unique_lock<std::mutex> lock(flow_control_mutex);
	while(!flow_control_condition_variable.wait_for(lock, 60000ms, [&, this] {
			bool cache_not_saturated = !thresholds_are_met(flow_control_bytes_count);

			if (!cache_not_saturated && blazing_timer.elapsed_time() > 59000){
				logger->warn("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
									"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
									"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
									"info"_a="wait_if_cache_is_saturated timed out",
									"duration"_a=blazing_timer.elapsed_time());
			}
			return cache_not_saturated;
		})){}
}

// take the first cacheData in this CacheMachine that it can find (looking in reverse order) that is in the GPU put it in RAM or Disk as oppropriate
// this function does not change the order of the caches
size_t CacheMachine::downgradeCacheData() {
	size_t bytes_downgraded = 0;
	std::unique_lock<std::mutex> lock = this->waitingCache->lock();
	std::vector<std::unique_ptr<message>> all_messages = this->waitingCache->get_all_unsafe();
	for(int i = all_messages.size() - 1; i >= 0; i--) {
		if (all_messages[i]->get_data().get_type() == CacheDataType::GPU){

			std::unique_ptr<ral::frame::BlazingTable> table = all_messages[i]->get_data().decache();

			std::string message_id = all_messages[i]->get_message_id();
			bytes_downgraded += table->sizeInBytes();
			int cacheIndex = 1; // starting at RAM cache
			while(cacheIndex < memory_resources.size()) {
				auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() + table->sizeInBytes());
				if( memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit()) {
					if(cacheIndex == 1) {
						logger->trace("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}|rows|{rows}",
							"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
							"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
							"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
							"info"_a="Downgraded CacheData to CPU cache",
							"kernel_id"_a=message_id,
							"rows"_a=table->num_rows());

						auto cache_data = std::make_unique<CPUCacheData>(std::move(table));
						auto new_message =	std::make_unique<message>(std::move(cache_data), message_id);
						all_messages[i] = std::move(new_message);
					} else if(cacheIndex == 2) {
						logger->trace("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}|rows|{rows}",
							"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
							"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
							"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
							"info"_a="Downgraded CacheData to Disk cache",
							"kernel_id"_a=message_id,
							"rows"_a=table->num_rows());

						// want to get only cache directory where orc files should be saved
						std::map<std::string, std::string> config_options = ctx->getConfigOptions();
						auto it = config_options.find("BLAZING_CACHE_DIRECTORY");
						std::string orc_files_path;
						if (it != config_options.end()) {
							orc_files_path = config_options["BLAZING_CACHE_DIRECTORY"];
						}
						auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table), orc_files_path);
						auto new_message = std::make_unique<message>(std::move(cache_data), message_id);
						all_messages[i] = std::move(new_message);						
					}					
					break;
				}
				cacheIndex++;
			}
			break;
		}
	}
	
	this->waitingCache->put_all_unsafe(std::move(all_messages));
	return bytes_downgraded;
}

ConcatenatingCacheMachine::ConcatenatingCacheMachine(std::shared_ptr<Context> context)
	: CacheMachine(context) {}

ConcatenatingCacheMachine::ConcatenatingCacheMachine(std::shared_ptr<Context> context, std::size_t flow_control_bytes_threshold, bool concat_all)
	: CacheMachine(context, flow_control_bytes_threshold), concat_all(concat_all) {}

// This method does not guarantee the relative order of the messages to be preserved
std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache() {

	if (concat_all){
		waitingCache->wait_until_finished();
	} else {
		waitingCache->wait_until_num_bytes(this->flow_control_bytes_threshold);
	}

	size_t total_bytes = 0;
	std::vector<std::unique_ptr<message>> collected_messages;
	std::unique_ptr<message> message_data;
	std::string message_id = "";

	do {
		message_data = waitingCache->pop_or_wait();
		if (message_data == nullptr){
			break;
		}
		auto& cache_data = message_data->get_data();
		total_bytes += cache_data.sizeInBytes();
		message_id = message_data->get_message_id();
		collected_messages.push_back(std::move(message_data));

		// we need to decrement here and not at the end, otherwise we can end up with a dead lock
		std::unique_lock<std::mutex> lock(flow_control_mutex);
		flow_control_bytes_count -= cache_data.sizeInBytes();
		flow_control_condition_variable.notify_all();

	} while (!thresholds_are_met(total_bytes + waitingCache->get_next_size_in_bytes()));

	std::unique_ptr<ral::frame::BlazingTable> output;
	size_t num_rows = 0;
	if(collected_messages.empty()){
		output = nullptr;
	} else if (collected_messages.size() == 1) {
		auto data = collected_messages[0]->release_data();
		output = std::move(data->decache());
		num_rows = output->num_rows();
	}	else {
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_holder;
		std::vector<ral::frame::BlazingTableView> table_views;
		for (int i = 0; i < collected_messages.size(); i++){
			auto data = collected_messages[i]->release_data();
			tables_holder.push_back(std::move(data->decache()));
			table_views.push_back(tables_holder[i]->toBlazingTableView());

			// if we dont have to concatenate all, lets make sure we are not overflowing, and if we are, lets put one back
			if (!concat_all && ral::utilities::checkIfConcatenatingStringsWillOverflow(table_views)){
				logger->warn("{query_id}|{step}|{substep}|{info}|||||",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="In ConcatenatingCacheMachine::pullFromCache Concatenating could have caused overflow strings length. Adding cache data back");

				auto cache_data = std::make_unique<GPUCacheData>(std::move(tables_holder.back()));
				tables_holder.pop_back();
				table_views.pop_back();
				collected_messages[i] =	std::make_unique<message>(std::move(cache_data), collected_messages[i]->get_message_id());
				std::unique_lock<std::mutex> lock(flow_control_mutex);
				for (; i < collected_messages.size(); i++){
					flow_control_bytes_count += collected_messages[i]->get_data().sizeInBytes();
					this->waitingCache->put(std::move(collected_messages[i]));
				}
				flow_control_condition_variable.notify_all();
				break;				
			}
		}

		if( concat_all && ral::utilities::checkIfConcatenatingStringsWillOverflow(table_views) ) { // if we have to concatenate all, then lets throw a warning if it will overflow strings
			logger->warn("{query_id}|{step}|{substep}|{info}|||||",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="In ConcatenatingCacheMachine::pullFromCache Concatenating will overflow strings length");
		}
		output = ral::utilities::concatTables(table_views);
		num_rows = output->num_rows();
	}

	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=(ctx ? std::to_string(ctx->getContextToken()) : ""),
								"step"_a=(ctx ? std::to_string(ctx->getQueryStep()) : ""),
								"substep"_a=(ctx ? std::to_string(ctx->getQuerySubstep()) : ""),
								"info"_a="Pull from ConcatenatingCacheMachine",
								"duration"_a="",
								"kernel_id"_a=message_id,
								"rows"_a=num_rows);

	return std::move(output);
}

}  // namespace cache
} // namespace ral
