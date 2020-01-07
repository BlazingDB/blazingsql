
#include "LogicalFilter.h"
#include "cudf/stream_compaction.hpp"

namespace ral{

namespace processor{


  std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(const ral::frame::BlazingTableView & table, const CudfColumnView boolValues){
        auto filteredTable = cudf::experimental::apply_boolean_mask(table.view(),boolValues);
        return std::make_unique(new ral::frame::BlazingTable(std::move(filteredTable),table.names()));
  }
}

}
