#include "CacheMachine.h"
#include "CacheDataLocalFile.h"
#include "CPUCacheData.h"
#include "communication/CommunicationData.h"

namespace ral {
namespace cache {

std::string MetadataDictionary::get_value(std::string key) {
	if (!this->has_value(key)) {
		return std::string();
	}
	return this->values.at(key);
}

void MetadataDictionary::set_value(std::string key, std::string value) {
	this->values[key] = value;
}

std::unique_ptr<CacheData> CacheData::downgradeCacheData(std::unique_ptr<CacheData> cacheData, std::string id, std::shared_ptr<Context> ctx) {
	
	// if its not a GPU cacheData, then we can't downgrade it, so we can just return it
	if (cacheData->get_type() != ral::cache::CacheDataType::GPU){
		return cacheData;
	} else {
		CodeTimer cacheEventTimer(false);
		cacheEventTimer.start();

		std::unique_ptr<ral::frame::BlazingTable> table = cacheData->decache();

		std::shared_ptr<spdlog::logger> cache_events_logger = spdlog::get("cache_events_logger");

		// lets first try to put it into CPU
		if (blazing_host_memory_resource::getInstance().get_memory_used() + table->sizeInBytes() <
				blazing_host_memory_resource::getInstance().get_memory_limit()){

			auto CPUCache = std::make_unique<CPUCacheData>(std::move(table));

			cacheEventTimer.stop();
			if(cache_events_logger) {
						cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
						"ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
						"query_id"_a=(ctx ? ctx->getContextToken() : -1),
						"message_id"_a="",
						"cache_id"_a=id,
						"num_rows"_a=(CPUCache ? CPUCache->num_rows() : -1),
						"num_bytes"_a=(CPUCache ? CPUCache->sizeInBytes() : -1),
						"event_type"_a="DowngradeCacheData",
						"timestamp_begin"_a=cacheEventTimer.start_time(),
						"timestamp_end"_a=cacheEventTimer.end_time(),
						"description"_a="Downgraded CacheData to CPU cache");
					}
			return CPUCache;
		} else {
			// want to get only cache directory where orc files should be saved
			std::string orc_files_path = ral::communication::CommunicationData::getInstance().get_cache_directory();

			auto localCache = std::make_unique<CacheDataLocalFile>(std::move(table), orc_files_path,
																(ctx ? std::to_string(ctx->getContextToken())
																		: "none"));

			cacheEventTimer.stop();
			if(cache_events_logger) {
				cache_events_logger->info("{ral_id}|{query_id}|{message_id}|{cache_id}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}|{description}",
										"ral_id"_a=(ctx ? ctx->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()) : -1),
										"query_id"_a=(ctx ? ctx->getContextToken() : -1),
										"message_id"_a="",
										"cache_id"_a=id,
										"num_rows"_a=(localCache ? localCache->num_rows() : -1),
										"num_bytes"_a=(localCache ? localCache->sizeInBytes() : -1),
										"event_type"_a="DowngradeCacheData",
										"timestamp_begin"_a=cacheEventTimer.start_time(),
										"timestamp_end"_a=cacheEventTimer.end_time(),
										"description"_a="Downgraded CacheData to Disk cache to path: " + orc_files_path);
			}

			return localCache;
		}
	}
}

} // namespace cache
} // namespace ral
