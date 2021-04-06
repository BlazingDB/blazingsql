#include "CacheData.h"

namespace ral {
namespace cache {

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
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table);

	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	* @param metadata The metadata that will be used in transport and planning.
	*/
	GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata);

	/**
	* Move the BlazingTable out of this Cache
	* This function only exists so that we can interact with all cache data by
	* calling decache on them.
	* @return The BlazingTable that was used to construct this CacheData.
	*/
	std::unique_ptr<ral::frame::BlazingTable> decache() override;

	/**
	* Get the amount of GPU memory consumed by this CacheData
	* Having this function allows us to have one api for seeing the consumption
	* of all the CacheData objects that are currently in Caches.
	* @return The number of bytes the BlazingTable consumes.
	*/
	size_t sizeInBytes() const;

	/**
	* Set the names of the columns of a BlazingTable.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override;

	/**
	* Destructor
	*/
	virtual ~GPUCacheData();

	/**
	* Get a ral::frame::BlazingTableView of the underlying data.
	* This allows you to read the data while it remains in cache.
	* @return a view of the data in this instance.
	*/
	ral::frame::BlazingTableView getTableView();

	void set_data(std::unique_ptr<ral::frame::BlazingTable> table);

protected:
	std::unique_ptr<ral::frame::BlazingTable> data; /**< Stores the data to be returned in decache */
};

} // namespace cache
} // namespace ral