#include "ConcatCacheData.h"
#include "utilities/CommonOperations.h"

namespace ral {
namespace cache {

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

	std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_holder;
	std::vector<ral::frame::BlazingTableView> table_views;
	for (auto && cache_data : _cache_datas){
		tables_holder.push_back(cache_data->decache());
		table_views.push_back(tables_holder.back()->toBlazingTableView());

		RAL_EXPECTS(!ral::utilities::checkIfConcatenatingStringsWillOverflow(table_views), "Concatenating tables will overflow");
	}

	return ral::utilities::concatTables(table_views);
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

} // namespace cache
} // namespace ral