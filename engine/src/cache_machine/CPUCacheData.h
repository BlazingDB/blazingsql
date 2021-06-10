#include "CacheData.h"

namespace ral {
namespace cache {

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
	* Set the names of the columns of a BlazingHostTable.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override
	{
		host_table->set_names(names);
	}

	/**
	* Destructor
	*/
	virtual ~CPUCacheData() {}

protected:
	std::unique_ptr<ral::frame::BlazingHostTable> host_table; /**< The CPU representation of a DataFrame  */ 	
};

} // namespace cache
} // namespace ral