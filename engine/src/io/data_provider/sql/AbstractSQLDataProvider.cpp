/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "AbstractSQLDataProvider.h"

#include "parser/expression_utils.hpp"
#include "CalciteExpressionParsing.h"
#include "parser/expression_tree.hpp"

namespace ral {
namespace io {

parser::parse_tree parse_blazingsql_predicate(const std::string &source)
{
  std::string filter_string = get_named_expression(source, "filters");
  std::string predicate = replace_calcite_regex(filter_string);
  parser::parse_tree tree;
  tree.build(predicate);
  return tree;
}

bool transform_sql_predicate(parser::parse_tree &source_ast,
  ral::parser::node_transformer* predicate_transformer)
{
  // TODO percy c.gonzales try/catch and check if ogher conds can return false
  source_ast.transform(*predicate_transformer);
  return true;
}

std::string in_order(const ral::parser::node &node) {
  if (&node == nullptr) {
      return "";
  }
  if (node.type == ral::parser::node_type::OPERATOR) {
    ral::parser::operator_node &op_node = ((ral::parser::operator_node&)node);
    operator_type op = map_to_operator_type(op_node.value);
    if (is_unary_operator(op)) {
      auto c1 = node.children[0].get();
      auto placement = op_node.placement;
      if (placement == ral::parser::operator_node::placement_type::END) {
        return "(" + in_order(*c1) + " " + op_node.label + ")";
      } else if (placement == ral::parser::operator_node::placement_type::BEGIN) {
        return "(" + op_node.label + " " + in_order(*c1) + ")";
      }
    } else if (is_binary_operator(op)) {
      auto c1 = node.children[0].get();
      auto c2 = node.children[1].get();
      return "(" + in_order(*c1) + " " + op_node.label + " " + in_order(*c2) + ")";
    }
  }
  return node.value;
}

std::string generate_sql_predicate(ral::parser::parse_tree &target_ast) {
  return in_order(target_ast.root());
}

// NOTE
// the source ast and the target ast are based on the same struct: ral::parser::parse_tree
// for now we can use that one for this use case, but in the future it would be a good idea
// to create a dedicated ast for each target sql backend/dialect
std::string transpile_sql_predicate(const std::string &source,
  ral::parser::node_transformer *predicate_transformer)
{
  parser::parse_tree ast = parse_blazingsql_predicate(source);
  if (transform_sql_predicate(ast, predicate_transformer)) {
    return generate_sql_predicate(ast);
  }
  return "";
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

bool abstractsql_data_provider::set_predicate_pushdown(
  const std::string &queryString,
  const std::vector<cudf::type_id> &cudf_types)
{
  auto predicate_transformer = this->get_predicate_transformer(cudf_types);
  this->where = transpile_sql_predicate(queryString, predicate_transformer.get());
  // DEBUG
  std::cout << "\nWHERE stmt for the predicate pushdown:\n" << this->where << "\n\n";
  return !this->where.empty();
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

} /* namespace io */
} /* namespace ral */
