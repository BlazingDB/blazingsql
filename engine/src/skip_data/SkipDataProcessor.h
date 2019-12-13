
#ifndef SKIPDATAPROCESSOR_H_
#define SKIPDATAPROCESSOR_H_

#include <iostream>
#include <string>
#include <blazingdb/manager/Context.h>
#include "io/DataLoader.h"

using blazingdb::manager::Context;

namespace ral {
namespace skip_data {

using skipdata_output_t = std::pair<std::vector<int>, std::vector<std::vector<int> > >;

skipdata_output_t process_skipdata_for_table(ral::io::data_loader & input_loader, std::vector<gdf_column_cpp> new_minmax_metadata_table, std::string table_scan, const Context& context);

} // namespace skip_data
} // namespace ral


#endif //SKIPDATAPROCESSOR_H_