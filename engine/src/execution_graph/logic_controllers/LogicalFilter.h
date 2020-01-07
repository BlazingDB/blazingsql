#pragma once

#include "LogicPrimitives.h"


namespace ral{

namespace processor{


/**
Takes a table and applies a boolean filter to it
*/
std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  const ral::frame::BlazingTableView & table,
  const CudfColumnView boolValues);

}

}
