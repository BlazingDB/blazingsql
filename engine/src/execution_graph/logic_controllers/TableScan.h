#pragma once

#include <blazingdb/manager/Context.h>

#include "LogicPrimitives.h"
#include "io/Schema.h"
#include "io/DataLoader.h"
#include <blazingdb/manager/Context.h>

namespace ral{

namespace processor{

std::unique_ptr<ral::frame::BlazingTable> process_table_scan(
  ral::io::data_loader& input_loader,
  const std::string & query_part,
  ral::io::Schema &schema,
  blazingdb::manager::experimental::Context * queryContext);

} // end namespace processor

} // end namespace ral
