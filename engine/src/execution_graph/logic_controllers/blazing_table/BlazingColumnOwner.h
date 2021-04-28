#pragma once

#include "execution_graph/logic_controllers/blazing_table/BlazingColumn.h"

namespace ral {
namespace frame {

class BlazingColumnOwner : public BlazingColumn {
	public:
		BlazingColumnOwner() =default;
		BlazingColumnOwner(const BlazingColumn&) =delete;
		BlazingColumnOwner& operator=(const BlazingColumnOwner&) =delete;
		BlazingColumnOwner(std::unique_ptr<CudfColumn> column);
		~BlazingColumnOwner() = default;
		CudfColumnView view() const {
			return column->view();
		}
		std::unique_ptr<CudfColumn> release() { return std::move(column); }
		blazing_column_type type() { return blazing_column_type::OWNER; }
		
	private:
		std::unique_ptr<CudfColumn> column;
};


}  // namespace frame
}  // namespace ral
