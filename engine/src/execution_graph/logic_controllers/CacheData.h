#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <map>

#include <spdlog/spdlog.h>
#include "cudf/types.hpp"
#include "error.hpp"
#include "CodeTimer.h"
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <execution_graph/Context.h>
#include <bmr/BlazingMemoryResource.h>
#include "communication/CommunicationData.h"
#include <exception>
#include "io/data_provider/DataProvider.h"
#include "io/data_parser/DataParser.h"

#include "communication/messages/GPUComponentMessage.h"

using namespace std::chrono_literals;

namespace ral {
namespace cache {

using Context = blazingdb::manager::Context;
using namespace fmt::literals;


/**
* Indicates a type of CacheData
* CacheData are type erased so we need to know if they reside in GPU,
* CPU, or a file. We can also have GPU messages that contain metadata
* which are used for sending CacheData from node to node
*/
enum class CacheDataType { GPU, CPU, LOCAL_FILE, GPU_METADATA, IO_FILE, CONCATENATING, PINNED };

const std::string KERNEL_ID_METADATA_LABEL = "kernel_id"; /**< A message metadata field that indicates which kernel owns this message. */
const std::string RAL_ID_METADATA_LABEL = "ral_id"; /**< A message metadata field that indicates RAL ran this. */
const std::string QUERY_ID_METADATA_LABEL = "query_id"; /**< A message metadata field that indicates which query owns this message. */
const std::string CACHE_ID_METADATA_LABEL = "cache_id";  /**< A message metadata field that indicates what cache a message should be routed to. Can be empty string if
 																															and only if add_to_specific_cache == false. */
const std::string ADD_TO_SPECIFIC_CACHE_METADATA_LABEL = "add_to_specific_cache";  /**< A message metadata field that indicates if a message should be routed to a specific cache or to the global input cache. */
const std::string SENDER_WORKER_ID_METADATA_LABEL = "sender_worker_id"; /**< A message metadata field that indicates what worker is sending this message. */
const std::string WORKER_IDS_METADATA_LABEL = "worker_ids"; /**< A message metadata field that indicates what worker is sending this message. */
const std::string TOTAL_TABLE_ROWS_METADATA_LABEL = "total_table_rows"; /**< A message metadata field that indicates how many rows are in this message. */
const std::string JOIN_LEFT_BYTES_METADATA_LABEL = "join_left_bytes_metadata_label"; /**< A message metadata field that indicates how many bytes were found in a left table for join scheduling.  */
const std::string JOIN_RIGHT_BYTES_METADATA_LABEL = "join_right_bytes_metadata_label"; /**< A message metadata field that indicates how many bytes were found in a right table for join scheduling.  */
const std::string AVG_BYTES_PER_ROW_METADATA_LABEL = "avg_bytes_per_row"; /** < A message metadata field that indicates the average of bytes per row. */
const std::string MESSAGE_ID = "message_id"; /**< A message metadata field that indicates the id of a message. Not all messages have an id. Any message that has add_to_specific_cache == false MUST have a message id. */
const std::string PARTITION_COUNT = "partition_count"; /**< A message metadata field that indicates the number of partitions a kernel processed.  */
const std::string UNIQUE_MESSAGE_ID = "unique_message_id"; /**< A message metadata field that indicates the unique id of a message. */

/**
* Lightweight wrapper for a map that will one day be used for compile time checks.
* Currently it is just a wrapper for map but in the future the intention is for
* us to manage the Metadata in a struct as opposed to a map of string to string.
*/
class MetadataDictionary{
public:

	/**
	* Gets the type of CacheData that was used to construct this CacheData
	* @param key The key in the map that we will be modifying.
	* @param value The value that we will set the key to.
	*/
	void add_value(std::string key, std::string value){
		this->values[key] = value;
	}

	/**
	* Gets the type of CacheData that was used to construct this CacheData
	* @param key The key in the map that we will be modifying.
	* @param value The value that we will set the key to.
	*/
	void add_value(std::string key, int value){
		this->values[key] = std::to_string(value);
	}

	/**
	* Gets id of creating kernel.
	* @return Get the id of the kernel that created this message.
	*/
	int get_kernel_id(){
		if( this->values.find(KERNEL_ID_METADATA_LABEL) == this->values.end()){
			throw BlazingMissingMetadataException(KERNEL_ID_METADATA_LABEL);
		}
		return std::stoi(values[KERNEL_ID_METADATA_LABEL]);
	}

	/**
	* Print every key => value pair in the map.
	* Only used for debugging purposes.
	*/
	void print(){
		for(auto elem : this->values)
		{
		   std::cout << elem.first << " " << elem.second<< "\n";
		}
	}
	/**
	* Gets the map storing the metadata.
	* @return the map storing all of the metadata.
	*/

	std::map<std::string,std::string> get_values() const {
		return this->values;
	}

	/**
	* Erases all current metadata and sets new values.
	* @param new_values A map to copy into this->values .
	*/
	void set_values(std::map<std::string,std::string> new_values){
		this->values= new_values;
	}

	/**
	* Checks if metadata has a specific key
	* @param key The key to check if is in the metadata
	* @return true if the key is in the metadata, otherwise return false
	*/
	bool has_value(std::string key){
		auto it = this->values.find(key);
		return it != this->values.end();
	}

    std::string get_value(std::string key);

    void set_value(std::string key, std::string value);

private:
	std::map<std::string,std::string> values; /**< Stores the mapping of metdata label to metadata value */
};

/**
* Base Class for all CacheData
* A CacheData represents a combination of a schema along with a container for
* a dataframe. This gives us one type that can be sent around and whose only
* purpose is to hold data until it is ready to be operated on by calling
* the decache method.
*/
class CacheData {
public:

	/**
	* Constructor for CacheData
	* This is only invoked by the derived classes when constructing.
	* @param cache_type The CacheDataType of this cache letting us know where the data is stored.
	* @param col_names The names of the columns in the dataframe.
	* @param schema The types of the columns in the dataframe.
	* @param n_rows The number of rows in the dataframe.
	*/
	CacheData(CacheDataType cache_type, std::vector<std::string> col_names, std::vector<cudf::data_type> schema, size_t n_rows)
		: cache_type(cache_type), col_names(col_names), schema(schema), n_rows(n_rows)
	{
	}

	CacheData()
	{
	}
	/**
	* Remove the payload from this CacheData. A pure virtual function.
	* This removes the payload for the CacheData. After this the CacheData will
	* almost always go out of scope and be destroyed.
	* @return a BlazingTable generated from the source of data for this CacheData
	*/
	virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;

	/**
	* . A pure virtual function.
	* This removes the payload for the CacheData. After this the CacheData will
	* almost always go out of scope and be destroyed.
	* @return the number of bytes our dataframe occupies in whatever format it is being stored
	*/
	virtual size_t sizeInBytes() const = 0;

	/**
	* Destructor
	*/
	virtual ~CacheData() {}

	/**
	* Get the names of the columns.
	* @return a vector of the column names
	*/
	std::vector<std::string> names() const {
		return col_names;
	}

	/**
	* Get the cudf::data_type of each column.
	* @return a vector of the cudf::data_type of each column.
	*/
	std::vector<cudf::data_type> get_schema() const {
		return schema;
	}

	/**
	* Get the number of columns this CacheData will generate with decache.
	*/
	size_t num_columns() const {
		return col_names.size();
	}

	/**
	* Get the number of rows this CacheData will generate with decache.
	*/
	size_t num_rows() const {
		return n_rows;
	}

	/**
	* Gets the type of CacheData that was used to construct this CacheData
	* @return The CacheDataType that is used to store the dataframe representation.
	*/
	CacheDataType get_type() const {
		return cache_type;
	}

	/**
	* Get the MetadataDictionary
	* @return The MetadataDictionary which is used in routing and planning.
	*/
	MetadataDictionary getMetadata(){
		return this->metadata;
	}

	/**
	 * Utility function which can take a CacheData and if its a standard GPU cache data, it will downgrade it to CPU or Disk
	 * @return If the input CacheData is not of a type that can be downgraded, it will just return the original input, otherwise it will return the downgraded CacheData.
	 */
	static std::unique_ptr<CacheData> downgradeCacheData(std::unique_ptr<CacheData> cacheData, std::string id, std::shared_ptr<Context> ctx);

protected:
	CacheDataType cache_type; /**< The CacheDataType that is used to store the dataframe representation. */
	std::vector<std::string> col_names; /**< A vector storing the names of the columns in the dataframe representation. */
	std::vector<cudf::data_type> schema; /**< A vector storing the cudf::data_type of the columns in the dataframe representation. */
	size_t n_rows; /**< Stores the number of rows in the dataframe representation. */
    MetadataDictionary metadata; /**< The metadata used for routing and planning. */
};

/**
* A CacheData that keeps its dataframe in GPU memory.
* This is a CacheData representation that wraps a ral::frame::BlazingTable. It
* is the most performant since its construction and decaching are basically
* no ops.
*/
class GPUCacheData : public CacheData {
public:
	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table)
		: CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}

	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	* @param metadata The metadata that will be used in transport and planning.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
		: GPUCacheData(std::move(table)) {
			this->metadata = metadata;
		}
    
	/**
	* Move the BlazingTable out of this Cache
	* This function only exists so that we can interact with all cache data by
	* calling decache on them.
	* @return The BlazingTable that was used to construct this CacheData.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }

	/**
	* Get the amount of GPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingTable consumes.
	*/
	size_t sizeInBytes() const override { return data->sizeInBytes(); }
	/**
	* Destructor
	*/
	virtual ~GPUCacheData() {}

	/**
	* Get a ral::frame::BlazingTableView of the underlying data.
	* This allows you to read the data while it remains in cache.
	* @return a view of the data in this instance.
	*/
	ral::frame::BlazingTableView getTableView(){
		return this->data->toBlazingTableView();
	}

	void set_data(std::unique_ptr<ral::frame::BlazingTable> table ){
		this->data = std::move(table);
	}
protected:
	std::unique_ptr<ral::frame::BlazingTable> data; /**< Stores the data to be returned in decache */
};


/**
* A CacheData that keeps its dataframe in CPU memory.
* This is a CacheData representation that wraps a ral::frame::BlazingHostTable.
* It is the more performant than most file based caching strategies but less
* efficient than a GPUCacheData.
*/
 class CPUCacheData : public CacheData {
 public:
	/**
 	* Constructor
	* Takes a GPU based ral::frame::BlazingTable and converts it CPU version
	* that is stored in a ral::frame::BlazingHostTable.
	* @param table The BlazingTable that is converted to a BlazingHostTable and
	* stored.
 	*/
 	CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table, bool use_pinned = false);

 	CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table,const MetadataDictionary & metadata, bool use_pinned = false);

	CPUCacheData(const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
    		    std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
        		std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations,
				const MetadataDictionary & metadata);

	/**
 	* Constructor
 	* Takes a GPU based ral::frame::BlazingHostTable and stores it in this
 	* CacheData instance.
	* @param table The BlazingHostTable that is moved into the CacheData.
 	*/
	CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table);

	/**
	* Decache from a BlazingHostTable to BlazingTable and return the BlazingTable.
	* @return A unique_ptr to a BlazingTable
 	*/
 	std::unique_ptr<ral::frame::BlazingTable> decache() override {
 		return std::move(host_table->get_gpu_table());
 	}

	/**
 	* Release this BlazingHostTable from this CacheData
	* If you want to allow this CacheData to be destroyed but want to keep the
	* memory in CPU this allows you to pull it out as a BlazingHostTable.
	* @return a unique_ptr to the BlazingHostTable that this was either
	* constructed with or which was generated during construction from a
	* BlazingTable.
 	*/
	std::unique_ptr<ral::frame::BlazingHostTable> releaseHostTable() {
 		return std::move(host_table);
 	}

	/**
	* Get the amount of CPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingHostTable consumes.
	*/
 	size_t sizeInBytes() const override { return host_table->sizeInBytes(); }
	/**
	* Destructor
	*/
 	virtual ~CPUCacheData() {}

protected:
	std::unique_ptr<ral::frame::BlazingHostTable> host_table; /**< The CPU representation of a DataFrame  */
 	MetadataDictionary metadata; /**< The metadata used for routing and planning. */
 };


/**
* A CacheData that stores is data in an ORC file.
* This allows us to cache onto filesystems to allow larger queries to run on
* limited resources. This is the least performant cache in most instances.
*/
class CacheDataLocalFile : public CacheData {
public:

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	* @ param ctx_id The context token to identify the query that generated the file.
	*/
	CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path, std::string ctx_token);

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	/**
 	* Get the amount of GPU memory that the decached BlazingTable WOULD consume.
 	* Having this function allows us to have one api for seeing how much GPU
	* memory is necessary to decache the file from disk.
 	* @return The number of bytes needed for the BlazingTable decache would
	* generate.
 	*/
	size_t sizeInBytes() const override;
	/**
	* Get the amount of disk space consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the ORC file consumes.
	*/
	size_t fileSizeInBytes() const;

	/**
	* Destructor
	*/
	virtual ~CacheDataLocalFile() {}

	/**
	* Get the file path of the ORC file.
	* @return The path to the ORC file.
	*/
	std::string filePath() const { return filePath_; }

private:
	std::string filePath_; /**< The path to the ORC file. Is usually generated randomly. */
	size_t size_in_bytes; /**< The size of the file being stored. */
};


/**
* A CacheData that stores is data in an ORC file.
* This allows us to cache onto filesystems to allow larger queries to run on
* limited resources. This is the least performant cache in most instances.
*/
class CacheDataIO : public CacheData {
public:

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	 CacheDataIO(ral::io::data_handle handle,
	 	std::shared_ptr<ral::io::data_parser> parser,
	 	ral::io::Schema schema,
		ral::io::Schema file_schema,
		std::vector<int> row_group_ids,
		std::vector<int> projections
		 );

	/**
	* Constructor
	* @param table The BlazingTable that is converted into an ORC file and stored
	* on disk.
	* @ param orc_files_path The path where the file should be stored.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	/**
 	* Get the amount of GPU memory that the decached BlazingTable WOULD consume.
 	* Having this function allows us to have one api for seeing how much GPU
	* memory is necessary to decache the file from disk.
 	* @return The number of bytes needed for the BlazingTable decache would
	* generate.
 	*/
	size_t sizeInBytes() const override;


	/**
	* Destructor
	*/
	virtual ~CacheDataIO() {}


private:
	ral::io::data_handle handle;
	std::shared_ptr<ral::io::data_parser> parser;
	ral::io::Schema schema;
	ral::io::Schema file_schema;
	std::vector<int> row_group_ids;
	std::vector<int> projections;
};

class ConcatCacheData : public CacheData {
public:
	/**
	* Constructor
	* @param table The cache_datas that will be concatenated when decached.
	* @param col_names The names of the columns in the dataframe.
	* @param schema The types of the columns in the dataframe.
	*/
	ConcatCacheData(std::vector<std::unique_ptr<CacheData>> cache_datas, const std::vector<std::string>& col_names, const std::vector<cudf::data_type>& schema);

	/**
	* Decaches all caches datas and concatenates them into one BlazingTable
	* @return The BlazingTable that results from concatenating all cache datas.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	/**
	* Get the amount of GPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingTable consumes.
	*/
	size_t sizeInBytes() const override;

	std::vector<std::unique_ptr<CacheData>> releaseCacheDatas();

	virtual ~ConcatCacheData() {}

protected:
	std::vector<std::unique_ptr<CacheData>> _cache_datas;
};


/**
* A wrapper for a CacheData object with a message_id.
* Bundles together a String mesage_id with a CacheData object. Really not very
* necessary as we could just as easily store this in the CacheData object.
*/
class message { //TODO: the cache_data object can store its id. This is not needed.
public:
	message(std::unique_ptr<CacheData> content, std::string message_id = "")
		: data(std::move(content)), message_id(message_id)
	{
		assert(data != nullptr);
	}

	~message() = default;

	std::string get_message_id() const { return (message_id); }

	CacheData& get_data() const { return *data; }

	std::unique_ptr<CacheData> release_data() { return std::move(data); }

protected:
	std::unique_ptr<CacheData> data;
	const std::string message_id;
};

}  // namespace cache


} // namespace ral
