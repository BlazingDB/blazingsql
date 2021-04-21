#include "CacheMachine.h"
#include "CPUCacheData.h"
#include "GPUCacheData.h"
#include "ConcatCacheData.h"
#include "CacheDataLocalFile.h"

#include <sys/stat.h>
#include <random>
#include <utilities/CommonOperations.h>
#include <cudf/io/orc.hpp>
#include "CalciteExpressionParsing.h"
#include "communication/CommunicationData.h"
#include <Util/StringUtil.h>
#include <stdio.h>

#include "Util/StringUtil.h"
#include <src/utilities/DebuggingUtils.h>

namespace ral {
namespace cache {

std::size_t CacheMachine::cache_count(900000000);

CacheMachine::CacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name, bool log_timeout, int cache_level_override, bool is_array_access):
        ctx(context), cache_id(CacheMachine::cache_count), cache_machine_name(cache_machine_name),
        cache_level_override(cache_level_override),
        cache_events_logger(spdlog::get("cache_events_logger")),
        is_array_access(is_array_access),
        global_index(-1)
{
	CacheMachine::cache_count++;

	waitingCache = std::make_unique<WaitingQueue <std::unique_ptr <message> > >(cache_machine_name, 60000, log_timeout);
	this->memory_resources.push_back( &blazing_device_memory_resource::getInstance() );
	this->memory_resources.push_back( &blazing_host_memory_resource::getInstance() );
	this->memory_resources.push_back( &blazing_disk_memory_resource::getInstance() );
	this->num_bytes_added = 0;
	this->num_rows_added = 0;

	something_added = false;

	std::shared_ptr<spdlog::logger> kernels_logger = spdlog::get("kernels_logger");
	if(kernels_logger) {
		kernels_logger->info("{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}|{description}",
							"ral_id"_a=(context ? context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1 ),
							"query_id"_a=(context ? std::to_string(context->getContextToken()) : "null"),
							"kernel_id"_a=cache_id,
							"is_kernel"_a=0, //false
							"kernel_type"_a="cache",
							"description"_a=cache_machine_name);
	}
}

CacheMachine::~CacheMachine() {}

Context * CacheMachine::get_context() const {
	return ctx.get();
}

std::int32_t CacheMachine::get_id() const { return (cache_id); }

void CacheMachine::finish() {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	this->waitingCache->finish();

	cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=cache_machine_name,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows_added,
                                  "num_bytes"_a=num_bytes_added,
                                  "event_type"_a="Finish",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="CacheMachine finish()");
    }
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

uint64_t CacheMachine::get_num_batches_added(){
	return this->waitingCache->processed_parts();
}

bool CacheMachine::addHostFrameToCache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string message_id) {

	// we dont want to add empty tables to a cache, unless we have never added anything
	if (!this->something_added || host_table->num_rows() > 0){
        CodeTimer cacheEventTimer;
        cacheEventTimer.start();

		num_rows_added += host_table->num_rows();
		num_bytes_added += host_table->sizeInBytes();

		if (message_id == ""){
			message_id = this->cache_machine_name;
		}

		auto cache_data = std::make_unique<CPUCacheData>(std::move(host_table));
		auto item =	std::make_unique<message>(std::move(cache_data), message_id);
		this->waitingCache->put(std::move(item));
		this->something_added = true;

        cacheEventTimer.stop();
        if(cache_events_logger) {
            cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                      "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                      "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                      "message_id"_a=cache_machine_name,
                                      "cache_id"_a=cache_id,
                                      "num_rows"_a=num_rows_added,
                                      "num_bytes"_a=num_bytes_added,
                                      "event_type"_a="AddHostFrameToCache",
                                      "timestamp_begin"_a=cacheEventTimer.start_time(),
                                      "timestamp_end"_a=cacheEventTimer.end_time(),
                                      "description"_a="Add to CacheMachine");
        }

		return true;
	}

	return false;
}

void CacheMachine::put(size_t index, std::unique_ptr<ral::frame::BlazingTable> table) {
	this->addToCache(std::move(table), this->cache_machine_name + "_" + std::to_string(index), true);
}

void CacheMachine::put(size_t index, std::unique_ptr<ral::cache::CacheData> cacheData) {
	this->addCacheData(std::move(cacheData), this->cache_machine_name + "_" + std::to_string(index), true);
}

void CacheMachine::clear() {
	CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	auto messages = this->waitingCache->get_all();
	this->waitingCache->finish();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=cache_machine_name,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows_added,
                                  "num_bytes"_a=num_bytes_added,
                                  "event_type"_a="Clear",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="Clear CacheMachine");
    }
}


std::vector<std::unique_ptr<ral::cache::CacheData> > CacheMachine::pull_all_cache_data(){
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	auto messages = this->waitingCache->get_all();
	std::vector<std::unique_ptr<ral::cache::CacheData> > new_messages(messages.size());
	int i = 0;
	for (auto & message_data : messages){
		new_messages[i] = message_data->release_data();
		i++;
	}

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=cache_machine_name,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows_added,
                                  "num_bytes"_a=num_bytes_added,
                                  "event_type"_a="PullAllCacheData",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="Pull all cache data");
    }

	return new_messages;
}

std::vector<size_t> CacheMachine::get_all_indexes() {
	std::vector<std::string> message_ids = this->waitingCache->get_all_message_ids();
	std::vector<size_t> indexes;
	indexes.reserve(message_ids.size());
	for (auto & message_id : message_ids){
		std::string prefix = this->cache_machine_name + "_";
		assert(StringUtil::beginsWith(message_id, prefix));
		indexes.push_back(std::stoi(message_id.substr(prefix.size())));
	}
	return indexes;
}

bool CacheMachine::addCacheData(std::unique_ptr<ral::cache::CacheData> cache_data, std::string message_id, bool always_add){
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	// we dont want to add empty tables to a cache, unless we have never added anything
	if ((!this->something_added || cache_data->num_rows() > 0) || always_add){
		num_rows_added += cache_data->num_rows();
		num_bytes_added += cache_data->sizeInBytes();
		int cacheIndex = 0;
		ral::cache::CacheDataType type = cache_data->get_type();
		if (type == ral::cache::CacheDataType::GPU){
			cacheIndex = 0;
		} else if (type == ral::cache::CacheDataType::CPU){
			cacheIndex = 1;
		} else {
			cacheIndex = 2;
		}

		if (message_id == ""){
			message_id = this->cache_machine_name;
		}

		if(cacheIndex == 0) {
			auto item = std::make_unique<message>(std::move(cache_data), message_id);
			this->waitingCache->put(std::move(item));

            cacheEventTimer.stop();
            if(cache_events_logger) {
                cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                          "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                          "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                          "message_id"_a=message_id,
                                          "cache_id"_a=cache_id,
                                          "num_rows"_a=num_rows_added,
                                          "num_bytes"_a=num_bytes_added,
                                          "event_type"_a="AddCacheData",
                                          "timestamp_begin"_a=cacheEventTimer.start_time(),
                                          "timestamp_end"_a=cacheEventTimer.end_time(),
                                          "description"_a="Add to CacheMachine general CacheData object into GPU cache");
            }
		} else if(cacheIndex == 1) {
			auto item = std::make_unique<message>(std::move(cache_data), message_id);
			this->waitingCache->put(std::move(item));

            cacheEventTimer.stop();
            if(cache_events_logger) {
                cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                          "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                          "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                          "message_id"_a=message_id,
                                          "cache_id"_a=cache_id,
                                          "num_rows"_a=num_rows_added,
                                          "num_bytes"_a=num_bytes_added,
                                          "event_type"_a="AddCacheData",
                                          "timestamp_begin"_a=cacheEventTimer.start_time(),
                                          "timestamp_end"_a=cacheEventTimer.end_time(),
                                          "description"_a="Add to CacheMachine general CacheData object into CPU cache");
            }
		} else if(cacheIndex == 2) {
			// BlazingMutableThread t([cache_data = std::move(cache_data), this, cacheIndex, message_id]() mutable {
			auto item = std::make_unique<message>(std::move(cache_data), message_id);
			this->waitingCache->put(std::move(item));
			// NOTE: Wait don't kill the main process until the last thread is finished!
			// }); t.detach();

            cacheEventTimer.stop();
            if(cache_events_logger) {
                cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                          "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                          "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                          "message_id"_a=message_id,
                                          "cache_id"_a=cache_id,
                                          "num_rows"_a=num_rows_added,
                                          "num_bytes"_a=num_bytes_added,
                                          "event_type"_a="AddCacheData",
                                          "timestamp_begin"_a=cacheEventTimer.start_time(),
                                          "timestamp_end"_a=cacheEventTimer.end_time(),
                                          "description"_a="Add to CacheMachine general CacheData object into Disk cache");
            }
		}
		this->something_added = true;

		return true;
	}

	return false;
}

bool CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table, std::string message_id, bool always_add,const MetadataDictionary & metadata , bool use_pinned) {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

    // we dont want to add empty tables to a cache, unless we have never added anything
	if (!this->something_added || table->num_rows() > 0 || always_add){
		for (auto col_ind = 0; col_ind < table->num_columns(); col_ind++){
			if (table->view().column(col_ind).offset() > 0){
				table->ensureOwnership();
				break;
			}

		}

		if (message_id == ""){
			message_id = this->cache_machine_name;
		}

		num_rows_added += table->num_rows();
		num_bytes_added += table->sizeInBytes();
		size_t cacheIndex = 0;
		while(cacheIndex < memory_resources.size()) {

			auto memory_to_use = (this->memory_resources[cacheIndex]->get_memory_used() + table->sizeInBytes());

			if( memory_to_use < this->memory_resources[cacheIndex]->get_memory_limit() || 
			 	cache_level_override != -1) {
				
				if(cache_level_override != -1){
					cacheIndex = cache_level_override;
				}
				if(cacheIndex == 0) {
					// before we put into a cache, we need to make sure we fully own the table
					table->ensureOwnership();
					std::unique_ptr<CacheData> cache_data;
					cache_data = std::make_unique<GPUCacheData>(std::move(table),metadata);
					auto item =	std::make_unique<message>(std::move(cache_data), message_id);
					this->waitingCache->put(std::move(item));

                    cacheEventTimer.stop();
                    if(cache_events_logger) {
                        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                                  "message_id"_a=message_id,
                                                  "cache_id"_a=cache_id,
                                                  "num_rows"_a=num_rows_added,
                                                  "num_bytes"_a=num_bytes_added,
                                                  "event_type"_a="AddToCache",
                                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                                  "description"_a="Add to CacheMachine into GPU cache");
                    }

				} else {
					if(cacheIndex == 1) {
						std::unique_ptr<CacheData> cache_data;
						cache_data = std::make_unique<CPUCacheData>(std::move(table), metadata, use_pinned);
							
						auto item =	std::make_unique<message>(std::move(cache_data), message_id);
						this->waitingCache->put(std::move(item));

                        cacheEventTimer.stop();
                        if(cache_events_logger) {
                            cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                                      "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                                      "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                                      "message_id"_a=message_id,
                                                      "cache_id"_a=cache_id,
                                                      "num_rows"_a=num_rows_added,
                                                      "num_bytes"_a=num_bytes_added,
                                                      "event_type"_a="AddToCache",
                                                      "timestamp_begin"_a=cacheEventTimer.start_time(),
                                                      "timestamp_end"_a=cacheEventTimer.end_time(),
                                                      "description"_a="Add to CacheMachine into CPU cache");
                        }

					} else if(cacheIndex == 2) {
						// BlazingMutableThread t([table = std::move(table), this, cacheIndex, message_id]() mutable {
						// want to get only cache directory where orc files should be saved
						std::string orc_files_path = ral::communication::CommunicationData::getInstance().get_cache_directory();
						// WSM TODO add metadata to CacheDataLocalFile
						auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table), orc_files_path, (ctx ? std::to_string(ctx->getContextToken()) : "none"));
						auto item =	std::make_unique<message>(std::move(cache_data), message_id);
						this->waitingCache->put(std::move(item));
						// NOTE: Wait don't kill the main process until the last thread is finished!
						// });t.detach();

                        cacheEventTimer.stop();
                        if(cache_events_logger) {
                            cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                                      "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                                      "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                                      "message_id"_a=message_id,
                                                      "cache_id"_a=cache_id,
                                                      "num_rows"_a=num_rows_added,
                                                      "num_bytes"_a=num_bytes_added,
                                                      "event_type"_a="AddToCache",
                                                      "timestamp_begin"_a=cacheEventTimer.start_time(),
                                                      "timestamp_end"_a=cacheEventTimer.end_time(),
                                                      "description"_a="Add to CacheMachine into Disk cache");
                        }
					}
				}
				break;
			}
			cacheIndex++;
		}
		this->something_added = true;

		return true;
	}

	return false;
}

void CacheMachine::wait_until_finished() {
	return waitingCache->wait_until_finished();
}


std::unique_ptr<ral::frame::BlazingTable> CacheMachine::get_or_wait(size_t index) {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	std::unique_ptr<message> message_data = waitingCache->get_or_wait(this->cache_machine_name + "_" + std::to_string(index));
	if (message_data == nullptr) {
		return nullptr;
	}
    std::string message_id = message_data->get_message_id();
    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().sizeInBytes();
    std::unique_ptr<ral::frame::BlazingTable> output = message_data->get_data().decache();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=num_bytes,
                                  "event_type"_a="GetOrWait",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="CacheMachine::get_or_wait pulling from cache ");
    }

	return output;
}

std::unique_ptr<ral::cache::CacheData>  CacheMachine::get_or_wait_CacheData(size_t index) {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	std::unique_ptr<message> message_data = waitingCache->get_or_wait(this->cache_machine_name + "_" + std::to_string(index));
	if (message_data == nullptr) {
		return nullptr;
	}
    std::string message_id = message_data->get_message_id();
    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().sizeInBytes();
	std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=num_bytes,
                                  "event_type"_a="GetOrWaitCacheData",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="CacheMachine::get_or_wait pulling CacheData from cache");
    }

	return output;
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache() {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

    std::string message_id;

	std::unique_ptr<message> message_data = nullptr;
    if (is_array_access) {
        message_id = this->cache_machine_name + "_" + std::to_string(++global_index);
        message_data = waitingCache->get_or_wait(message_id);
    } else {
        message_data = waitingCache->pop_or_wait();
        message_id = message_data->get_message_id();
    }

	if (message_data == nullptr) {
		return nullptr;
	}
    
    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().sizeInBytes();
    int dataType = static_cast<int>(message_data->get_data().get_type());
	std::unique_ptr<ral::frame::BlazingTable> output = message_data->get_data().decache();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=num_bytes,
                                  "event_type"_a="PullFromCache",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="Pull from CacheMachine type {}"_format(dataType));
    }

	return output;
}


std::unique_ptr<ral::cache::CacheData> CacheMachine::pullCacheData(std::string message_id) {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	std::unique_ptr<message> message_data = waitingCache->get_or_wait(message_id);
	if (message_data == nullptr) {
		return nullptr;
	}
    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().sizeInBytes();
    int dataType = static_cast<int>(message_data->get_data().get_type());
	std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=num_bytes,
                                  "event_type"_a="PullCacheData",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="Pull from CacheMachine CacheData object type {}"_format(dataType));
    }
	return output;
}

std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullUnorderedFromCache() {
    if (is_array_access) {
        return this->pullFromCache();
    }

    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

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
        std::string message_id = message_data->get_message_id();
        size_t num_rows = message_data->get_data().num_rows();
        size_t num_bytes = message_data->get_data().sizeInBytes();
        int dataType = static_cast<int>(message_data->get_data().get_type());
        std::unique_ptr<ral::frame::BlazingTable> output = message_data->get_data().decache();

        cacheEventTimer.stop();
        if(cache_events_logger) {
            cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                      "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                      "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                      "message_id"_a=message_id,
                                      "cache_id"_a=cache_id,
                                      "num_rows"_a=num_rows,
                                      "num_bytes"_a=num_bytes,
                                      "event_type"_a="PullUnorderedFromCache",
                                      "timestamp_begin"_a=cacheEventTimer.start_time(),
                                      "timestamp_end"_a=cacheEventTimer.end_time(),
                                      "description"_a="Pull Unordered from CacheMachine type {}"_format(dataType));
        }

		return output;
	} else {
		return pullFromCache();
	}
}

std::unique_ptr<ral::cache::CacheData> CacheMachine::pullCacheData() {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

    std::unique_ptr<message> message_data = nullptr;
    std::string message_id;

    if (is_array_access) {
        message_id = this->cache_machine_name + "_" + std::to_string(++global_index);
        message_data = waitingCache->get_or_wait(message_id);
    } else {
        message_data = waitingCache->pop_or_wait();
        if (message_data == nullptr) {
            return nullptr;
        }
        message_id = message_data->get_message_id();
    }

    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().sizeInBytes();
    int dataType = static_cast<int>(message_data->get_data().get_type());
    std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=num_bytes,
                                  "event_type"_a="PullCacheData",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="Pull from CacheMachine CacheData object type {}"_format(dataType));
    }

	return output;
}


// take the first cacheData in this CacheMachine that it can find (looking in reverse order) that is in the GPU put it in RAM or Disk as oppropriate
// this function does not change the order of the caches
size_t CacheMachine::downgradeCacheData() {
	size_t bytes_downgraded = 0;
	std::unique_lock<std::mutex> lock = this->waitingCache->lock();
	std::vector<std::unique_ptr<message>> all_messages = this->waitingCache->get_all_unsafe();
	for(int i = all_messages.size() - 1; i >= 0; i--) {
		if (all_messages[i]->get_data().get_type() == CacheDataType::GPU){

			std::string message_id = all_messages[i]->get_message_id();
			auto current_cache_data = all_messages[i]->release_data();
			bytes_downgraded += current_cache_data->sizeInBytes();
			auto new_cache_data = CacheData::downgradeCacheData(std::move(current_cache_data), message_id, ctx);

			auto new_message =	std::make_unique<message>(std::move(new_cache_data), message_id);
			all_messages[i] = std::move(new_message);
		}
	}

	this->waitingCache->put_all_unsafe(std::move(all_messages));
	return bytes_downgraded;
}

bool CacheMachine::has_messages_now(std::vector<std::string> messages){

	std::vector<std::string> current_messages = this->waitingCache->get_all_message_ids();
	for (auto & message : messages){
		bool found = false;
		for (auto & cur_message : current_messages){
			if (message == cur_message)	{
				found = true;
				break;
			}
		}
		if (!found){
			return false;
		}
	}
	return true;
}

std::unique_ptr<ral::cache::CacheData> CacheMachine::pullAnyCacheData(const std::vector<std::string> & messages) {

	if (messages.size() == 0){
		return nullptr;
	}

	CodeTimer cacheEventTimer;
    cacheEventTimer.start();

    std::unique_ptr<message> message_data = waitingCache->get_or_wait_any(messages);
	std::string message_id = message_data->get_message_id();
    
    size_t num_rows = message_data->get_data().num_rows();
    size_t num_bytes = message_data->get_data().sizeInBytes();
    int dataType = static_cast<int>(message_data->get_data().get_type());
    std::unique_ptr<ral::cache::CacheData> output = message_data->release_data();

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=num_bytes,
                                  "event_type"_a="pullAnyCacheData",
                                  "timestamp_begin"_a=cacheEventTimer.start_time(),
                                  "timestamp_end"_a=cacheEventTimer.end_time(),
                                  "description"_a="Pull from CacheMachine CacheData object type {}"_format(dataType));
    }

	return output;
}

bool CacheMachine::has_data_in_index_now(size_t index){
    std::string message = this->cache_machine_name + "_" + std::to_string(index);
    std::vector<std::string> current_messages = this->waitingCache->get_all_message_ids();
    bool found = false;
    for (auto & cur_message : current_messages){
        if (message == cur_message)	{
            found = true;
            break;
        }
    }
    return found;
}


ConcatenatingCacheMachine::ConcatenatingCacheMachine(std::shared_ptr<Context> context, std::string cache_machine_name)
	: CacheMachine(context, cache_machine_name) {}

ConcatenatingCacheMachine::ConcatenatingCacheMachine(std::shared_ptr<Context> context,
			std::size_t concat_cache_num_bytes, int num_bytes_timeout, bool concat_all, std::string cache_machine_name)
	: CacheMachine(context, cache_machine_name), concat_cache_num_bytes(concat_cache_num_bytes), num_bytes_timeout(num_bytes_timeout), concat_all(concat_all) {

	}

// This method does not guarantee the relative order of the messages to be preserved
std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache() {
    CodeTimer cacheEventTimerGeneral;
    cacheEventTimerGeneral.start();

	if (concat_all){
		waitingCache->wait_until_finished();
	} else {
		waitingCache->wait_until_num_bytes(this->concat_cache_num_bytes, this->num_bytes_timeout);
	}

	size_t total_bytes = 0;
	std::vector<std::unique_ptr<message>> collected_messages;
	std::unique_ptr<message> message_data;
	std::string message_id = "";

	do {
		if (concat_all || waitingCache->has_next_now()){
			message_data = waitingCache->pop_or_wait();
		} else {
			message_data = nullptr;
		}
		if (message_data == nullptr){
			break;
		}
		auto& cache_data = message_data->get_data();
		total_bytes += cache_data.sizeInBytes();
		message_id = message_data->get_message_id();
		collected_messages.push_back(std::move(message_data));

	} while (concat_all || (total_bytes + waitingCache->get_next_size_in_bytes() <= this->concat_cache_num_bytes));

	std::unique_ptr<ral::frame::BlazingTable> output;
	size_t num_rows = 0;
	if(collected_messages.empty()){
		output = nullptr;
	} else if (collected_messages.size() == 1) {
		auto data = collected_messages[0]->release_data();
		output = data->decache();
		num_rows = output->num_rows();
	}	else {
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_holder;
		std::vector<ral::frame::BlazingTableView> table_views;
		for (size_t i = 0; i < collected_messages.size(); i++){
            CodeTimer cacheEventTimer;
		    cacheEventTimer.start();

			auto data = collected_messages[i]->release_data();
			tables_holder.push_back(data->decache());
			table_views.push_back(tables_holder[i]->toBlazingTableView());

			// if we dont have to concatenate all, lets make sure we are not overflowing, and if we are, lets put one back
			if (!concat_all && ral::utilities::checkIfConcatenatingStringsWillOverflow(table_views)){
				auto cache_data = std::make_unique<GPUCacheData>(std::move(tables_holder.back()));
				tables_holder.pop_back();
				table_views.pop_back();
				collected_messages[i] =	std::make_unique<message>(std::move(cache_data), collected_messages[i]->get_message_id());
				for (; i < collected_messages.size(); i++){
					this->waitingCache->put(std::move(collected_messages[i]));
				}

                cacheEventTimer.stop();
                if(cache_events_logger) {
                    cache_events_logger->warn("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                              "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                              "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                              "message_id"_a=message_id,
                                              "cache_id"_a=cache_id,
                                              "num_rows"_a=num_rows,
                                              "num_bytes"_a=total_bytes,
                                              "event_type"_a="PullFromCache",
                                              "timestamp_begin"_a=cacheEventTimer.start_time(),
                                              "timestamp_end"_a=cacheEventTimer.end_time(),
                                              "description"_a="In ConcatenatingCacheMachine::pullFromCache Concatenating could have caused overflow strings length. Adding cache data back");
                }

				break;
			}
		}

		if( concat_all && ral::utilities::checkIfConcatenatingStringsWillOverflow(table_views) ) { // if we have to concatenate all, then lets throw a warning if it will overflow strings
            CodeTimer cacheEventTimer;
            cacheEventTimer.start();
            cacheEventTimer.stop();
		    if(cache_events_logger) {
                cache_events_logger->warn("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                          "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                          "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                          "message_id"_a=message_id,
                                          "cache_id"_a=cache_id,
                                          "num_rows"_a=num_rows,
                                          "num_bytes"_a=total_bytes,
                                          "event_type"_a="PullFromCache",
                                          "timestamp_begin"_a=cacheEventTimer.start_time(),
                                          "timestamp_end"_a=cacheEventTimer.end_time(),
                                          "description"_a="In ConcatenatingCacheMachine::pullFromCache Concatenating will overflow strings length");
            }
		}
		output = ral::utilities::concatTables(table_views);
		num_rows = output->num_rows();
	}

    cacheEventTimerGeneral.stop();
    if(cache_events_logger) {
        cache_events_logger->trace("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                  "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                  "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                  "message_id"_a=message_id,
                                  "cache_id"_a=cache_id,
                                  "num_rows"_a=num_rows,
                                  "num_bytes"_a=total_bytes,
                                  "event_type"_a="PullFromCache",
                                  "timestamp_begin"_a=cacheEventTimerGeneral.start_time(),
                                  "timestamp_end"_a=cacheEventTimerGeneral.end_time(),
                                  "description"_a="Pull from ConcatenatingCacheMachine");
    }

	return output;
}

std::unique_ptr<ral::cache::CacheData> ConcatenatingCacheMachine::pullCacheData() {
    CodeTimer cacheEventTimer;
    cacheEventTimer.start();

	if (concat_all){
		waitingCache->wait_until_finished();
	} else {
		waitingCache->wait_until_num_bytes(this->concat_cache_num_bytes, this->num_bytes_timeout);		
	}

	size_t total_bytes = 0;
	std::vector<std::unique_ptr<message>> collected_messages;
	std::unique_ptr<message> message_data;
	std::string message_id;

	do {
		if (concat_all || waitingCache->has_next_now()){
			message_data = waitingCache->pop_or_wait();
		} else {
			message_data = nullptr;
		}
		if (message_data == nullptr){
			break;
		}
		auto& cache_data = message_data->get_data();
		total_bytes += cache_data.sizeInBytes();
		message_id = message_data->get_message_id();
		collected_messages.push_back(std::move(message_data));
	} while (concat_all || (total_bytes + waitingCache->get_next_size_in_bytes() <= this->concat_cache_num_bytes));

	std::unique_ptr<ral::cache::CacheData> output;
	size_t num_rows = 0;
	if(collected_messages.empty()){
		output = nullptr;
	} else if (collected_messages.size() == 1) {
		output = collected_messages[0]->release_data();
		num_rows = output->num_rows();
	}	else {
		std::vector<std::unique_ptr<ral::cache::CacheData>> cache_datas;
		for (std::size_t i = 0; i < collected_messages.size(); i++){
			cache_datas.push_back(collected_messages[i]->release_data());
		}

		output = std::make_unique<ConcatCacheData>(std::move(cache_datas), cache_datas[0]->names(), cache_datas[0]->get_schema());
		num_rows = output->num_rows();
	}

    cacheEventTimer.stop();
    if(cache_events_logger) {
        cache_events_logger->trace("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
                                   "ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
                                   "query_id"_a=(ctx ? ctx->getContextToken() : -1),
                                   "message_id"_a=message_id,
                                   "cache_id"_a=cache_id,
                                   "num_rows"_a=num_rows,
                                   "num_bytes"_a=total_bytes,
                                   "event_type"_a="PullCacheData",
                                   "timestamp_begin"_a=cacheEventTimer.start_time(),
                                   "timestamp_end"_a=cacheEventTimer.end_time(),
                                   "description"_a="Pull cache data from ConcatenatingCacheMachine");
    }

	return output;
}

} // namespace cache
} // namespace ral
