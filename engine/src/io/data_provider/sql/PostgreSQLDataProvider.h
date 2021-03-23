/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <percy@blazingdb.com>
 */

#ifndef POSTGRESQLDATAPROVIDER_H_
#define POSTGRESQLDATAPROVIDER_H_

#include "AbstractSQLDataProvider.h"

#include <libpq-fe.h>
#include <jdbc/cppconn/connection.h>

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
};

} /* namespace io */
} /* namespace ral */

#endif /* POSTGRESQLDATAPROVIDER_H_ */
