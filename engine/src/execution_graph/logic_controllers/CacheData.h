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
enum class CacheDataType { GPU, CPU, LOCAL_FILE, IO_FILE, CONCATENATING, PINNED, ARROW };

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

// fields for window functions
const std::string OVERLAP_STATUS = "overlap_status"; /**< A message metadata field that indicates the status of this overlap data. */
const std::string OVERLAP_MESSAGE_TYPE = "overlap_message_type"; /**< A message metadata field that indicates the type of overlap request (preceding_request, following_request, preceding_response, following_response)*/
const std::string OVERLAP_SIZE = "overlap_size"; /**< A message metadata field that contains an integer indicating the amount of overlap*/
const std::string OVERLAP_SOURCE_NODE_INDEX = "overlap_source_node_index"; /**< A message metadata field that contains an integer indicating the node to from where it came*/
const std::string OVERLAP_TARGET_NODE_INDEX = "overlap_target_node_index"; /**< A message metadata field that contains an integer indicating the node to whom it will be sent*/
const std::string OVERLAP_TARGET_BATCH_INDEX = "overlap_target_batch_index"; /**< A message metadata field that contains an integer indicating the batch index for the node to whom it will be sent*/

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
	* Set the names of the columns.
	* @param names a vector of the column names.
	*/
	virtual void set_names(const std::vector<std::string> & names) = 0;

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
	* Set the MetadataDictionary
	*/
	void setMetadata(MetadataDictionary new_metadata){
		this->metadata = new_metadata;
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
