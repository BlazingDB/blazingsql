#include "CacheData.h"

namespace ral {
namespace cache {

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

	/**
	* Set the names of the columns.
	* @param names a vector of the column names.
	*/
	void set_names(const std::vector<std::string> & names) override;

	std::vector<std::unique_ptr<CacheData>> releaseCacheDatas();

	virtual ~ConcatCacheData() {}

protected:
	std::vector<std::unique_ptr<CacheData>> _cache_datas;
};

} // namespace cache
} // namespace ral