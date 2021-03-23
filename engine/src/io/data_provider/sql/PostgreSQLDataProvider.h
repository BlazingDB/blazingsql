/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo <percy@blazingdb.com>
 */

#ifndef POSTGRESQLDATAPROVIDER_H_
#define POSTGRESQLDATAPROVIDER_H_

#include <memory>

#include "AbstractSQLDataProvider.h"

#include <libpq-fe.h>
#include <jdbc/cppconn/connection.h>

namespace ral {
namespace io {

class postgresql_data_provider : public abstractsql_data_provider {
public:
	postgresql_data_provider(const sql_connection &sql_conn,
                           const std::string &table,
                           size_t batch_size_hint = abstractsql_data_provider::DETAULT_BATCH_SIZE_HINT,
                           bool use_partitions = false);

  virtual ~postgresql_data_provider();

private:
  std::shared_ptr<PGconn> connection;

};

} /* namespace io */
} /* namespace ral */

#endif /* POSTGRESQLDATAPROVIDER_H_ */
