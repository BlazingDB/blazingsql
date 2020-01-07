
#include "LogicalFilter.h"

namespace ral{

namespace processor{


  std::unique_ptr<::frame::BlazingTable> applyBooleanFilter(const BlazingTableView & table, const CudfColumnView boolValues){
        auto filteredTable = apply_boolean_mask(table.view(),boolValues);
        return std::make_unique(new BlazingTable(std::move(filteredTable),table.names()));
  }
}

}
