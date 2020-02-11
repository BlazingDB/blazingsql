
#ifndef SKIPDATAPROCESSOR_H_
#define SKIPDATAPROCESSOR_H_

#include <iostream>
#include <string>
#include <blazingdb/manager/Context.h>
#include "io/DataLoader.h"

using blazingdb::manager::experimental::Context;

namespace ral {
namespace skip_data {

// std::vector<gdf_column_cpp> process_skipdata_for_table(ral::io::data_loader & input_loader, std::vector<gdf_column*> new_minmax_metadata_table, std::string table_scan, const Context& context);

} // namespace skip_data
} // namespace ral


#endif //SKIPDATAPROCESSOR_H_
