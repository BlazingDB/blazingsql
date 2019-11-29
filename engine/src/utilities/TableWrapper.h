#ifndef BLAZINGDB_RAL_UTILITIES_WRAPPERTABLE_H
#define BLAZINGDB_RAL_UTILITIES_WRAPPERTABLE_H

#include <vector>
#include <cudf.h>

class gdf_column_cpp;

namespace ral {
namespace utilities {

class TableWrapper {
public:
    TableWrapper(const std::vector<gdf_column_cpp>& columns);

    TableWrapper(const std::vector<gdf_column_cpp>& columns, const std::vector<int>& colIndices);

protected:
    TableWrapper(TableWrapper&&) = delete;

    TableWrapper(const TableWrapper&) = delete;

    TableWrapper& operator=(TableWrapper&&) = delete;

    TableWrapper& operator=(const TableWrapper&) = delete;

public:
    gdf_column** getColumns();

    gdf_size_type getQuantity();

private:
    std::vector<gdf_column*> columns_;
};

} // namespace utilities
} // namespace ral

#endif //BLAZINGDB_RAL_UTILITIES_WRAPPERTABLE_H
