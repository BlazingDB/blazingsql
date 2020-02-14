
#ifndef SKIPDATAPROCESSOR_H_
#define SKIPDATAPROCESSOR_H_

#include <iostream>
#include <string>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace skip_data {

std::pair<std::unique_ptr<ral::frame::BlazingTable>, bool> process_skipdata_for_table(
    const ral::frame::BlazingTableView & metadata_view, const std::vector<std::string> & names, std::string table_scan);

} // namespace skip_data
} // namespace ral


#endif //SKIPDATAPROCESSOR_H_
