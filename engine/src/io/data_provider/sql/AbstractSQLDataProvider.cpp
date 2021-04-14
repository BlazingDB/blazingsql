/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "AbstractSQLDataProvider.h"

#include "parser/expression_utils.hpp"
#include "CalciteExpressionParsing.h"
#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"

namespace ral {
namespace io {

// we go in order here and apply the ops names for specific sql providers
std::string transpile_predicate(const ral::parser::node &node,
  const std::map<operator_type, std::string> &ops,
  const std::vector<int> &column_indices,
  const std::vector<std::string> &column_names)
{
  if (&node == nullptr) {
      return "";
  }
  if (node.type == ral::parser::node_type::OPERATOR) {
    operator_type op = map_to_operator_type(node.value);
    std::string op_name = ops.empty()? node.value:(ops.count(op)? ops.at(op):node.value);
    if (is_unary_operator(op)) {
      auto c1 = node.children[0].get();
      if (op == operator_type::BLZ_IS_NULL ||
          op == operator_type::BLZ_IS_NOT_NULL || 
          op == operator_type::BLZ_IS_NOT_DISTINCT_FROM) {
        return "(" + transpile_predicate(*c1, ops, column_indices, column_names) + " " + op_name + ")";
      } else {
        return "(" + op_name + " " + transpile_predicate(*c1, ops, column_indices, column_names) + ")";    
      }
    } else if (is_binary_operator(op)) {
      auto c1 = node.children[0].get();
      auto c2 = node.children[1].get();
      return "(" + transpile_predicate(*c1, ops, column_indices, column_names) +
        " " + op_name + " " + transpile_predicate(*c2, ops, column_indices, column_names) +
        ")";
    }
  } else if (node.type == ral::parser::node_type::VARIABLE) {
    std::string var = StringUtil::split(node.value, "$")[1];
    int idx = std::atoi(var.c_str());
    return column_names[column_indices[idx]];
  }
  return node.value;
}

abstractsql_data_provider::abstractsql_data_provider(
    const sql_info &sql,
    size_t total_number_of_nodes,
    size_t self_node_idx)
	: data_provider(), sql(sql), total_number_of_nodes(total_number_of_nodes), self_node_idx(self_node_idx) {}

abstractsql_data_provider::~abstractsql_data_provider() {
	this->close_file_handles(); 
}

std::vector<data_handle> abstractsql_data_provider::get_some(std::size_t batch_count, bool){
	std::size_t count = 0;
	std::vector<data_handle> file_handles;
	while(this->has_next() && count < batch_count) {
		auto handle = this->get_next();
		if (handle.is_valid()) {
			file_handles.emplace_back(std::move(handle));
		}
		count++;
	}
	return file_handles;
}

/**
 * Closes currently open set of file handles maintained by the provider
*/
void abstractsql_data_provider::close_file_handles() {
  // NOTE we don't use any file handle for this provider so nothing to do here
}

void abstractsql_data_provider::set_predicate_pushdown(ral::io::Schema & schema,
  const std::string &queryString)
{
  std::string filter_string = get_named_expression(queryString, "filters");
  std::string predicate = replace_calcite_regex(filter_string);
  parser::parse_tree tree;
  tree.build(predicate);
  this->where = transpile_predicate(tree.root(), this->ops, this->column_indices, schema.get_names());
}

std::string abstractsql_data_provider::build_select_from() const {
  std::string cols;
  if (this->column_indices.empty()) {
    cols = "* ";
  } else {
    for (int i = 0; i < this->column_indices.size(); ++i) {
      int col_idx = this->column_indices[i];
      cols += this->column_names[col_idx];
      if ((i + 1) == this->column_indices.size()) {
        cols += " ";
      } else {
        cols += ", ";
      }
    }
  }
  auto ret = "SELECT " + cols + "FROM " + this->sql.table;
  if (this->where.empty()) return ret;
  return ret + " where " + this->where;
}

std::string abstractsql_data_provider::build_limit_offset(size_t offset) const {
  return " LIMIT " + std::to_string(this->sql.table_batch_size) + " OFFSET " + std::to_string(offset);
}

void abstractsql_data_provider::register_operator(operator_type op_type,
  const std::string &op_name, bool is)
{
  this->ops[op_type] = op_name;
}

} /* namespace io */
} /* namespace ral */
