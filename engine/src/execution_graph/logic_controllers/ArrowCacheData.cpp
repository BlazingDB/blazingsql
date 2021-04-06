#include "ArrowCacheData.h"

namespace ral {
namespace cache {

ArrowCacheData::ArrowCacheData(std::unique_ptr<arrow::Table> table, ral::io::Schema schema)
    : CacheData(CacheDataType::ARROW, schema.get_names(), schema.get_data_types(), table->num_rows()), data{std::move(table)} {}

std::unique_ptr<ral::frame::BlazingTable> ArrowCacheData::decache() {
    return std::make_unique<ral::frame::BlazingTable>(std::move(cudf::from_arrow(*data)), this->col_names);
}

ArrowCacheData::~ArrowCacheData() {}

} // namespace cache
} // namespace ral