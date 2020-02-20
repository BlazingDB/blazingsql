#pragma once

#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"

namespace ral {

namespace frame {

class BlazingColumnView : public BlazingColumn {
	public:
		BlazingColumnView();
		CudfColumnView view() const {
			return column;
		}
		// release of a BlazingColumnView will make a copy since its not the owner and therefore cannot transfer ownership
		std::unique_ptr<CudfColumn> release() { return std::make_unique<CudfColumn>(column); }
		
	private:
		CudfColumnView column;
};

}  // namespace frame

}  // namespace ral