#include "CPUCacheData.h"

namespace ral {
namespace cache {

CPUCacheData::CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table, bool use_pinned)
	: CacheData(CacheDataType::CPU, gpu_table->names(), gpu_table->get_schema(), gpu_table->num_rows())
{
	this->host_table = ral::communication::messages::serialize_gpu_message_to_host_table(gpu_table->toBlazingTableView(), use_pinned);
}

CPUCacheData::CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table,const MetadataDictionary & metadata, bool use_pinned)
	: CacheData(CacheDataType::CPU, gpu_table->names(), gpu_table->get_schema(), gpu_table->num_rows())
{
	this->host_table = ral::communication::messages::serialize_gpu_message_to_host_table(gpu_table->toBlazingTableView(), use_pinned);
	this->metadata = metadata;
}

CPUCacheData::CPUCacheData(const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
			std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
			std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations,
			const MetadataDictionary & metadata)  {

	
	this->cache_type = CacheDataType::CPU;
	for(int i = 0; i < column_transports.size(); i++){
		this->col_names.push_back(std::string(column_transports[i].metadata.col_name));
		this->schema.push_back(cudf::data_type{cudf::type_id(column_transports[i].metadata.dtype)});			
	}
	if(column_transports.size() == 0){
		this->n_rows = 0;
	}else{
		this->n_rows = column_transports[0].metadata.size;
	}
	this->host_table = std::make_unique<ral::frame::BlazingHostTable>(column_transports,std::move(chunked_column_infos), std::move(allocations));
	this->metadata = metadata;
}

CPUCacheData::CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table)
	: CacheData(CacheDataType::CPU, host_table->names(), host_table->get_schema(), host_table->num_rows()), host_table{std::move(host_table)}
{
}

} // namespace cache
} // namespace ral