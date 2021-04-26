/**
 * Copyright 2021 Cristhian Alberto Gonzales Castillo <gcca.lib@gmail.com>
 */

#ifndef SNOWFLAKEDATAPROVIDER_H_
#define SNOWFLAKEDATAPROVIDER_H_

#include <cstddef>

#include "AbstractSQLDataProvider.h"

namespace ral {
namespace io {

/**
 * can generate a series of data handles from snowflake database schema.
 */
class snowflake_data_provider : public abstractsql_data_provider {
public:
  snowflake_data_provider(const sql_info & sql,
                          std::size_t total_number_of_nodes,
                          std::size_t self_node_idx);

  virtual ~snowflake_data_provider();

  std::shared_ptr<data_provider> clone() override;

  /**
   * tells us if this provider can generate more sql resultsets
   */
  bool has_next() override;

  /**
   *  Resets file read count to 0 for file based DataProvider
   */
  void reset() override;

  /**
   * gets us the next arrow::io::RandomAccessFile
   * if open_file is false will not run te query and just returns a data_handle
   * with columns info
   */
  data_handle get_next(bool = true) override;

  /**
   * Get the number of data_handles that will be provided.
   */
  std::size_t get_num_handles() override;

protected:
  std::unique_ptr<parser::node_transformer>
  get_predicate_transformer() const override {
    return nullptr;
  }

private:
  std::size_t row_count;
  std::size_t batch_position;
};

} /* namespace io */
} /* namespace ral */

#endif /* SNOWFLAKEDATAPROVIDER_H_ */
