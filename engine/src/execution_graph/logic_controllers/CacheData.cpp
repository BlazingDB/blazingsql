#include "CacheMachine.h"
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
using namespace std::chrono_literals;
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

// BEGIN CPUCacheData

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

// END CPUCacheData

// BEGIN CacheDataLocalFile

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path, std::string ctx_token)
	: CacheData(CacheDataType::LOCAL_FILE, table->names(), table->get_schema(), table->num_rows())
{
	this->size_in_bytes = table->sizeInBytes();
	this->filePath_ = orc_files_path + "/.blazing-temp-" + ctx_token + "-" + randomString(64) + ".orc";

	// filling this->col_names
	for(auto name : table->names()) {
		this->col_names.push_back(name);
	}

	int attempts = 0;
	int attempts_limit = 10;
	while(attempts <= attempts_limit){
		try {
			cudf::io::table_metadata metadata;
			for(auto name : table->names()) {
				metadata.column_names.emplace_back(name);
			}

			cudf::io::orc_writer_options out_opts = cudf::io::orc_writer_options::builder(cudf::io::sink_info{this->filePath_}, table->view())
				.metadata(&metadata);

			cudf::io::write_orc(out_opts);
		} catch (cudf::logic_error & err){
            std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
			if(logger) {
				logger->error("|||{info}||||rows|{rows}",
					"info"_a="Failed to create CacheDataLocalFile in path: " + this->filePath_ + " attempt " + std::to_string(attempts),
					"rows"_a=table->num_rows());
			}	
			attempts++;
			if (attempts == attempts_limit){
				throw;
			}
			std::this_thread::sleep_for (std::chrono::milliseconds(5 * attempts));
		}
	}
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

	cudf::io::orc_reader_options read_opts = cudf::io::orc_reader_options::builder(cudf::io::source_info{this->filePath_});
	auto result = cudf::io::read_orc(read_opts);

	// Remove temp orc files
	const char *orc_path_file = this->filePath_.c_str();
	remove(orc_path_file);
	return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), this->col_names );
}

// END CacheDataLocalFile

// BEGIN CacheDataIO

CacheDataIO::CacheDataIO(ral::io::data_handle handle,
	std::shared_ptr<ral::io::data_parser> parser,
	ral::io::Schema schema,
	ral::io::Schema file_schema,
	std::vector<int> row_group_ids,
	std::vector<int> projections)
	: CacheData(CacheDataType::IO_FILE, schema.get_names(), schema.get_data_types(), 1),
	handle(handle), parser(parser), schema(schema),
	file_schema(file_schema), row_group_ids(row_group_ids),
	projections(projections)
	{

	}

size_t CacheDataIO::sizeInBytes() const{
	return 0;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataIO::decache(){
	if (schema.all_in_file()){
		std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(handle, file_schema, projections, row_group_ids);
		return loaded_table;
	} else {
		std::vector<int> column_indices_in_file;  // column indices that are from files
		for (auto projection_idx : projections){
			if(schema.get_in_file()[projection_idx]) {
				column_indices_in_file.push_back(projection_idx);
			}
		}

		std::vector<std::unique_ptr<cudf::column>> all_columns(projections.size());
		std::vector<std::unique_ptr<cudf::column>> file_columns;
		std::vector<std::string> names;
		cudf::size_type num_rows;
		if (column_indices_in_file.size() > 0){
			std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = parser->parse_batch(handle, file_schema, column_indices_in_file, row_group_ids);
			names = current_blazing_table->names();
			std::unique_ptr<CudfTable> current_table = current_blazing_table->releaseCudfTable();
			num_rows = current_table->num_rows();
			file_columns = current_table->release();

		} else { // all tables we are "loading" are from hive partitions, so we dont know how many rows we need unless we load something to get the number of rows
			std::vector<int> temp_column_indices = {0};
			std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(handle, file_schema, temp_column_indices, row_group_ids);
			num_rows = loaded_table->num_rows();
		}

		int in_file_column_counter = 0;
		for(std::size_t i = 0; i < projections.size(); i++) {
			int col_ind = projections[i];
			if(!schema.get_in_file()[col_ind]) {
				std::string name = schema.get_name(col_ind);
				names.push_back(name);
				cudf::type_id type = schema.get_dtype(col_ind);
				std::string literal_str = handle.column_values[name];
				std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, cudf::data_type{type},false);
				all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
			} else {
				all_columns[i] = std::move(file_columns[in_file_column_counter]);
				in_file_column_counter++;
			}
		}
		auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
		return std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), names);
	}
}

// END CacheDataIO

// BEGIN ConcatCacheData

ConcatCacheData::ConcatCacheData(std::vector<std::unique_ptr<CacheData>> cache_datas, const std::vector<std::string>& col_names, const std::vector<cudf::data_type>& schema)
	: CacheData(CacheDataType::CONCATENATING, col_names, schema, 0), _cache_datas{std::move(cache_datas)} {
	n_rows = 0;
	for (auto && cache_data : _cache_datas) {
		auto cache_schema = cache_data->get_schema();
		RAL_EXPECTS(std::equal(schema.begin(), schema.end(), cache_schema.begin()), "Cache data has a different schema");
		n_rows += cache_data->num_rows();
	}
}

std::unique_ptr<ral::frame::BlazingTable> ConcatCacheData::decache() {
	if(_cache_datas.empty()) {
		return ral::utilities::create_empty_table(col_names, schema);
	}

	if (_cache_datas.size() == 1)	{
		return _cache_datas[0]->decache();
	}

	std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables;
	for (auto && cache_data : _cache_datas){
		tables.push_back(cache_data->decache());
		
		RAL_EXPECTS(!ral::utilities::checkIfConcatenatingStringsWillOverflow(tables), "Concatenating tables will overflow");
	}
	return ral::utilities::concatTables(std::move(tables));
}

size_t ConcatCacheData::sizeInBytes() const {
	size_t total_size = 0;
	for (auto && cache_data : _cache_datas) {
		total_size += cache_data->sizeInBytes();
	}
	return total_size;
};

void ConcatCacheData::set_names(const std::vector<std::string> & names) {
	for (size_t i = 0; i < _cache_datas.size(); ++i) {
		_cache_datas[i]->set_names(names);
	}
}

std::vector<std::unique_ptr<CacheData>> ConcatCacheData::releaseCacheDatas(){
	return std::move(_cache_datas);
}

// END ConcatCacheData

}  // namespace cache
} // namespace ral
