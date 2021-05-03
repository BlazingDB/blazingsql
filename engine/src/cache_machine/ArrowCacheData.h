#include "CacheData.h"
#include <arrow/table.h>

namespace ral {
namespace cache {

/**
* A CacheData that keeps its dataframe as an Arrow table.
* This is a CacheData representation that wraps a arrow::Table.
*/
class ArrowCacheData : public CacheData {
public:
	/**
	* Constructor
	* @param table The BlazingTable that is moved into the CacheData.
	*/
	ArrowCacheData(std::shared_ptr<arrow::Table> table, ral::io::Schema schema);

// 	/**
// 	* Constructor
// 	* @param table The BlazingTable that is moved into the CacheData.
// 	* @param metadata The metadata that will be used in transport and planning.
// 	*/
// 	ArrowCacheData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
// 		: CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {
// 			this->metadata = metadata;
// 		}

	/**
	* Move the arrow::Table out of this Cache
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
	size_t sizeInBytes() const override;

	/**
	* Set the names of the columns of a BlazingTable.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override;

	/**
	* Destructor
	*/
	virtual ~ArrowCacheData();

// 	/**
// 	* Get a ral::frame::BlazingTableView of the underlying data.
// 	* This allows you to read the data while it remains in cache.
// 	* @return a view of the data in this instance.
// 	*/
// 	ral::frame::BlazingTableView getTableView(){
// 		return this->data->toBlazingTableView();
// 	}

// 	void set_data(std::unique_ptr<ral::frame::BlazingTable> table ){
// 		this->data = std::move(table);
// 	}

protected:
	std::vector<std::string> col_names; /**< The names of the columns */
	std::shared_ptr<arrow::Table> data; /**< Stores the data to be returned in decache */
};

} // namespace cache
} // namespace ral