/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#ifndef ABSTRACTSQLDATAPROVIDER_H_
#define ABSTRACTSQLDATAPROVIDER_H_

#include "io/data_provider/DataProvider.h"
#include "io/Schema.h"
#include "parser/expression_tree.hpp"

namespace ral {
namespace io {

struct sql_info {
  std::string host;
  size_t port;
  std::string user;
  std::string password;
  std::string schema; // aka database name
  std::string table;
  std::string table_filter;
  size_t table_batch_size;
};

/**
 * serves as base abstract class for all sql providers
 * can generate a series of resultsets from databases that are provided
 * when it goes out of scope it will close any resultset it opened
 * this last point is debatable in terms of if this is the desired functionality
 */
class abstractsql_data_provider : public data_provider {
public:
  abstractsql_data_provider(const sql_info &sql,
    size_t total_number_of_nodes,
    size_t self_node_idx);

  virtual ~abstractsql_data_provider();

  /**
	 * Get batch_count batches from the sql database using while has_next() and next()
	 */
	std::vector<data_handle> get_some(std::size_t batch_count, bool = true) override;

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	void close_file_handles() override;

  /**
	 * Set the column indices (aka projections) that will use to select the table
	 */
  void set_column_indices(std::vector<int> column_indices) { this->column_indices = column_indices; }

  bool set_predicate_pushdown(const std::string &queryString);

protected:
  virtual std::unique_ptr<ral::parser::node_transformer> get_predicate_transformer() const = 0;

protected:
  // returns SELECT ... FROM ... WHERE ... LIMIT ... OFFSET
  std::string build_select_query(size_t batch_index) const;

protected:
  sql_info sql;
  std::vector<int> column_indices;
  std::vector<std::string> column_names;
  std::vector<std::string> column_types;
  size_t total_number_of_nodes;
  size_t self_node_idx;
  std::string where;
};

template<class SQLProvider>
void set_sql_projections(data_provider *provider, const std::vector<int> &projections) {
  auto sql_provider = static_cast<SQLProvider*>(provider);
  sql_provider->set_column_indices(projections);
}

template<class SQLProvider>
bool set_sql_predicate_pushdown(data_provider *provider, const std::string &queryString)
{
  auto sql_provider = static_cast<SQLProvider*>(provider);
  return sql_provider->set_predicate_pushdown(queryString);
}

} /* namespace io */
} /* namespace ral */

#endif /* ABSTRACTSQLDATAPROVIDER_H_ */
