/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Cristhian Alberto Gonzales Castillo
 * <cristhian@blazingdb.com>
 */

#ifndef _POSTGRESQLSQLPARSER_H_
#define _POSTGRESQLSQLPARSER_H_

#include "io/data_parser/DataParser.h"

namespace ral {
namespace io {

class postgresql_parser : public data_parser {
public:
  postgresql_parser();

  virtual ~postgresql_parser();

  std::unique_ptr<frame::BlazingTable>
  parse_batch(data_handle handle,
              const Schema &schema,
              std::vector<int> column_indices,
              std::vector<cudf::size_type> row_groups) override;

  void parse_schema(ral::io::data_handle handle, Schema &schema) override;

  std::unique_ptr<frame::BlazingTable>
  get_metadata(std::vector<data_handle> handles, int offset) override;

  DataType type() const override { return DataType::PARQUET; }
};

} /* namespace io */
} /* namespace ral */

#endif /* _POSTGRESQLSQLPARSER_H_ */
