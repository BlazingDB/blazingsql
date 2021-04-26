/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#include "SnowFlakeDataProvider.h"

namespace ral {
namespace io {

snowflake_data_provider::snowflake_data_provider(
    const sql_info & sql,
    std::size_t total_number_of_nodes,
    std::size_t self_node_idx)
    : abstractsql_data_provider{sql, total_number_of_nodes, self_node_idx},
      row_count{0}, batch_position{0} {}

snowflake_data_provider::~snowflake_data_provider() = default;

std::shared_ptr<data_provider> snowflake_data_provider::clone() {
  return nullptr;
}

bool snowflake_data_provider::has_next() { return false; }

void snowflake_data_provider::reset() {}

data_handle snowflake_data_provider::get_next(bool) {
  data_handle handle;
  return handle;
}

std::size_t snowflake_data_provider::get_num_handles() { return 0; }

}  // namespace io
}  // namespace ral
