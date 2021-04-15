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

bool parse_blazingsql_predicate(const std::string &source, parser::parse_tree &ast)
{
  if (source.empty()) return false;
  std::string filter_string = get_named_expression(source, "filters");
  if (filter_string.empty()) return false;
  std::string predicate = replace_calcite_regex(filter_string);
  if (predicate.empty()) return false;
  ast.build(predicate);
  return true;
}

bool transform_sql_predicate(parser::parse_tree &source_ast,
  ral::parser::node_transformer* predicate_transformer)
{
  // TODO percy c.gonzales try/catch and check if other conds can return false
  source_ast.transform(*predicate_transformer);
  return true;
}

std::string in_order(const ral::parser::node &node) {
  using ral::parser::operator_node;
  if (&node == nullptr) {
      return "";
  }
  if (node.type == ral::parser::node_type::OPERATOR) {
    ral::parser::operator_node &op_node = ((ral::parser::operator_node&)node);
    operator_type op = map_to_operator_type(op_node.value);
    if (is_unary_operator(op)) {
      auto c1 = node.children[0].get();
      auto placement = op_node.placement;
      if (placement == operator_node::END) {
        auto body = in_order(*c1) + " " + op_node.label;
        if (op_node.parentheses_wrap) {
          return "(" + body + ")";
        } else {
          return body;
        }
      } else if (placement == operator_node::BEGIN || placement == operator_node::AUTO) {
        auto body = op_node.label + " " + in_order(*c1);
        if (op_node.parentheses_wrap) {
          return "(" + body + ")";
        } else {
          return body;
        }
      }
    } else if (is_binary_operator(op)) {
      std::string body;
      for (int i = 0; i < node.children.size(); ++i) {
        auto c = node.children[i].get();
        body += in_order(*c);
        if (i < node.children.size()-1) {
          body += " " + op_node.label + " ";
        }
      }
      if (op_node.parentheses_wrap) {
        return "(" + body + ")";
      } else {
        return body;
      }
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
  parser::parse_tree ast;
  if (!parse_blazingsql_predicate(source, ast)) return "";
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
  // DEBUG
  std::cout << "\nORIGINAL query part for the predicate pushdown:\n" << queryString << "\n\n";
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
