/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 * Copyright 2021 Cristhian Alberto Gonzales Castillo
 */

#ifndef POSTGRESQLDATAPROVIDER_H_
#define POSTGRESQLDATAPROVIDER_H_

#include "AbstractSQLDataProvider.h"

#include <libpq-fe.h>

namespace ral {
namespace io {

class postgresql_data_provider : public abstractsql_data_provider {
public:
  postgresql_data_provider(const sql_info &sql,
                           size_t total_number_of_nodes,
                           size_t self_node_idx);

  virtual ~postgresql_data_provider();

  std::shared_ptr<data_provider> clone() override;

  bool has_next() override;

  void reset() override;

  data_handle get_next(bool open_file = true) override;

  std::size_t get_num_handles() override;

protected:
  // TODO percy c.gonzales
  std::unique_ptr<ral::parser::node_transformer> get_predicate_transformer() const override { return nullptr; }

private:
  PGconn *connection;
  bool table_fetch_completed;
  std::size_t batch_position;
  std::size_t estimated_table_row_count;
};

} /* namespace io */
} /* namespace ral */

#endif /* POSTGRESQLDATAPROVIDER_H_ */
