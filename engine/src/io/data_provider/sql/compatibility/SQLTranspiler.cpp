/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#include "SQLTranspiler.h"

namespace ral {
namespace io {
namespace sql_tools {

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

bool transform_predicate(parser::parse_tree &source_ast,
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
    auto op_name = op_node.label.empty()? op_node.value : op_node.label;
    operator_type op = map_to_operator_type(op_node.value);
    if (is_unary_operator(op)) {
      auto c1 = node.children[0].get();
      auto placement = op_node.placement;
      if (placement == operator_node::END) {
        auto body = in_order(*c1) + " " + op_name;
        if (op_node.parentheses_wrap) {
          return "(" + body + ")";
        } else {
          return body;
        }
      } else if (placement == operator_node::BEGIN || placement == operator_node::AUTO) {
        auto body = op_name + " " + in_order(*c1);
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
          body += " " + op_name + " ";
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

std::string generate_predicate(ral::parser::parse_tree &target_ast) {
  return in_order(target_ast.root());
}

std::string transpile_predicate(const std::string &source,
  ral::parser::node_transformer *predicate_transformer)
{
  parser::parse_tree ast;
  if (!parse_blazingsql_predicate(source, ast)) return "";
  if (transform_predicate(ast, predicate_transformer)) {
    return generate_predicate(ast);
  }
  return "";
}

std::map<operator_type, operator_info> get_default_operators() {
  using ral::parser::operator_node;
  static std::map<operator_type, operator_info> operators;
  if (operators.empty()) {
    operators[operator_type::BLZ_IS_NOT_NULL] = {.label = "IS NOT NULL", .placement = operator_node::END};
    operators[operator_type::BLZ_IS_NULL] = {.label = "IS NULL", .placement = operator_node::END};
  }
  return operators;
}

default_predicate_transformer::default_predicate_transformer(
  const std::vector<int> &column_indices,
  const std::vector<std::string> &column_names,
  const std::map<operator_type, operator_info> &operators)
  : column_indices(column_indices), column_names(column_names), operators(operators) {}

default_predicate_transformer::~default_predicate_transformer() {}

ral::parser::node * default_predicate_transformer::transform(ral::parser::operad_node& node) {
  auto ndir = &((ral::parser::node&)node);
  if (this->visited.count(ndir)) {
    return &node;
  }
  if (node.type == ral::parser::node_type::VARIABLE) {
    std::string var = StringUtil::split(node.value, "$")[1];
    size_t idx = std::atoi(var.c_str());
    size_t col = column_indices[idx];
    node.value = column_names[col];
  } else if (node.type == ral::parser::node_type::LITERAL) {
    ral::parser::literal_node &literal_node = ((ral::parser::literal_node&)node);
    if (literal_node.type().id() == cudf::type_id::TIMESTAMP_DAYS ||
        literal_node.type().id() == cudf::type_id::TIMESTAMP_SECONDS ||
        literal_node.type().id() == cudf::type_id::TIMESTAMP_NANOSECONDS ||
        literal_node.type().id() == cudf::type_id::TIMESTAMP_MICROSECONDS ||
        literal_node.type().id() == cudf::type_id::TIMESTAMP_MILLISECONDS)
    {
      node.value = "\"" + node.value + "\"";
    }
  }
  this->visited[ndir] = true;
  return &node;
}

ral::parser::node * default_predicate_transformer::transform(parser::operator_node& node) {
  auto ndir = &((ral::parser::node&)node);
  if (this->visited.count(ndir)) {
    return &node;
  }
  if (!this->operators.empty()) {
    operator_type op = map_to_operator_type(node.value);
    if (this->operators.count(op)) {
      auto op_obj = this->operators.at(op);
      node.label = op_obj.label;
      node.placement = op_obj.placement;
      node.parentheses_wrap = op_obj.parentheses_wrap;
    }
  }
  this->visited[ndir] = true;
  return &node;
}

} /* namespace sql_tools */
} /* namespace io */
} /* namespace ral */
