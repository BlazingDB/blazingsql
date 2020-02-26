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

enum class blazing_column_type {
	OWNER,
	VIEW
};


class BlazingColumn {
	public:
		BlazingColumn() =default;
		BlazingColumn(const BlazingColumn&) =delete;
  		BlazingColumn& operator=(const BlazingColumn&) =delete;
		virtual CudfColumnView view() const = 0;
		virtual std::unique_ptr<CudfColumn> release() = 0;
		virtual blazing_column_type type() = 0;
		virtual ~BlazingColumn() = default;
		
	
};

}  // namespace frame

}  // namespace ral