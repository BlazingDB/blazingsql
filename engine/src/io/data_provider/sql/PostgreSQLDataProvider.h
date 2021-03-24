/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <cristhian@blazingdb.com>
 */

#ifndef POSTGRESQLDATAPROVIDER_H_
#define POSTGRESQLDATAPROVIDER_H_

#include "AbstractSQLDataProvider.h"

#include <libpq-fe.h>

namespace ral {
namespace io {

class postgresql_data_provider : public abstractsql_data_provider {
public:
	postgresql_data_provider(const sql_info &sql);

  virtual ~postgresql_data_provider();

  std::shared_ptr<data_provider> clone() override;

  bool has_next() override;

  void reset() override;

  data_handle get_next(bool open_file = true) override;

  std::size_t get_num_handles() override;

private:
  PGconn *connection;
  bool table_fetch_completed;
  std::size_t batch_position;
  std::size_t estimated_table_row_count;
};

} /* namespace io */
} /* namespace ral */

#endif /* POSTGRESQLDATAPROVIDER_H_ */
