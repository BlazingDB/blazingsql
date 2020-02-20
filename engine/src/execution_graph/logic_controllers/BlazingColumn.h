#pragma once

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <memory>
#include <string>
#include <vector>

typedef cudf::experimental::table CudfTable;
typedef cudf::table_view CudfTableView;
typedef cudf::column CudfColumn;
typedef cudf::column_view CudfColumnView;

namespace ral {

namespace frame {


class BlazingColumn {
	public:
		virtual CudfColumnView view() const = 0;
		virtual std::unique_ptr<CudfColumn> release() = 0;
		
	
};

}  // namespace frame

}  // namespace ral