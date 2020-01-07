#pragma once

#include "LogicPrimitives.h"

namespace ral{

namespace processor{


/**
Takes a table and applies a boolean filter to it
*/
std::unique_ptr<::frame::BlazingTable> applyBooleanFilter(
  const BlazingTableView & table,
  const CudfColumnView boolValues);

}

}
