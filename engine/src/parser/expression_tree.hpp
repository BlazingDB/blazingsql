#pragma once
#include <blazingdb/io/Util/StringUtil.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stack>
#include <string>
#include <vector>
#include "parser/expression_utils.hpp"

namespace ral {
namespace parser {

enum parse_node_type { OPERATOR, OPERAND };

struct parse_node {
  parse_node_type type;
  std::vector<std::unique_ptr<parse_node>> children;
  std::string value;

  parse_node(parse_node_type type, const std::string &value)
      : type{type}, value{value} {};

  virtual parse_node *transform_to_custom_op() = 0;
};

struct operad_node : parse_node {
  operad_node(const std::string &value) : parse_node{OPERAND, value} {};

  parse_node *transform_to_custom_op() override { return this; }
};

struct operator_node : parse_node {
  operator_node(const std::string &value) : parse_node{OPERATOR, value} {};

  parse_node *transform_to_custom_op() override {
    for (auto &&c : this->children) {
      parse_node *transformed_node = c->transform_to_custom_op();
      if (transformed_node != c.get()) {
        c.reset(transformed_node);
      }
    }

    if (this->value == "CASE") {
      return transform_case(0);
    } else if (this->value == "CAST") {
      return transform_cast();
    } else if (this->value == "SUBSTRING") {
      return transform_substring();
    }

    return this;
  }

 private:
  parse_node *transform_case(size_t child_idx) {
    assert(this->children.size() >= 3 && this->children.size() % 2 != 0);
    assert(child_idx < this->children.size());

    if (child_idx == this->children.size() - 1) {
      return this->children[child_idx].release();
    }

    parse_node *condition = this->children[child_idx].release();
    parse_node *then = this->children[child_idx + 1].release();

    parse_node *magic_if_not = new operator_node{"MAGIC_IF_NOT"};
    magic_if_not->children.push_back(std::unique_ptr<parse_node>(condition));
    magic_if_not->children.push_back(std::unique_ptr<parse_node>(then));

    parse_node *first_non_magic = new operator_node{"FIRST_NON_MAGIC"};
    first_non_magic->children.push_back(
        std::unique_ptr<parse_node>(magic_if_not));
    first_non_magic->children.push_back(
        std::unique_ptr<parse_node>(transform_case(child_idx + 2)));

    return first_non_magic;
  }

  parse_node *transform_cast() {
    assert(this->children.size() == 2);

    parse_node *exp = this->children[0].release();
    std::string target_type = this->children[1]->value;

    parse_node *cast_op = new operator_node{"CAST_" + target_type};
    cast_op->children.push_back(std::unique_ptr<parse_node>(exp));

    return cast_op;
  }

  parse_node *transform_substring() {
    assert(this->children.size() == 2 || this->children.size() == 3);

    parse_node *target = this->children[0].release();
    std::string start_end_str = "'" + this->children[1]->value;
    if (this->children.size() == 3) {
      start_end_str += ":" + this->children[2]->value;
    }
    start_end_str += "'";
    parse_node *start_end_params = new operad_node{start_end_str};

    parse_node *substring_op = new operator_node{this->value};
    substring_op->children.push_back(std::unique_ptr<parse_node>(target));
    substring_op->children.push_back(std::unique_ptr<parse_node>(start_end_params));

    return substring_op;
  }
};

struct parse_tree {
  std::unique_ptr<parse_node> root;

 private:
  size_t build_helper(parse_node *parent_node, const std::string &expression,
                      size_t pos) {
    bool return_type = false;
    while (pos != std::string::npos) {
      size_t nextPos = expression.find_first_of("(),", pos);
      std::string token = expression.substr(pos, nextPos - pos);
      token = StringUtil::ltrim(token);

      parse_node *new_node = nullptr;
      if (!token.empty()) {
        if (is_operator_token(token)) {
          new_node = new operator_node(token);
        } else {
          assert(is_var_column(token) || is_literal(token));
          new_node = new operad_node(token);
        }

        if (!parent_node) {
          this->root.reset(new_node);
        } else {
          parent_node->children.push_back(
              std::unique_ptr<parse_node>(new_node));
        }
      }

      if (nextPos == std::string::npos) {
        return nextPos;
      } else if (expression[nextPos] == ')') {
        if (nextPos + 1 < expression.size() && expression[nextPos + 1] == ':') {
          return_type = true;
          pos = nextPos + 2;
          break;
        } else {
          return nextPos + 1;
        }
      } else if (expression[nextPos] == '(') {
        assert(new_node != nullptr);
        pos = build_helper(new_node, expression, nextPos + 1);
      } else {  // expression[pos] == ','
        pos = nextPos + 1;
      }
    }

    if (return_type) {
      // Special case for '):' as in CAST($0):DOUBLE
      // Parse as a child of current parent
      assert(pos < expression.size());
      assert(parent_node != nullptr);

      size_t nextPos = expression.find_first_of("(),", pos);
      std::string token = expression.substr(pos, nextPos - pos);
      token = StringUtil::ltrim(token);

      parse_node *new_node = new operad_node(token);
      parent_node->children.push_back(std::unique_ptr<parse_node>(new_node));

      // Don't advance position so that the parent can process it
      return nextPos;
    } else {
      assert(pos == std::string::npos);
    }

    return pos;
  }

  void print_helper(parse_node *node, size_t depth) {
    if (!node) return;

    for (size_t i = 0; i < depth; ++i) {
      std::cout << "    ";
    }

    std::cout << node->value << "\n";

    for (auto &&c : node->children) {
      print_helper(c.get(), depth + 1);
    }
  }

  std::string rebuild_helper(parse_node *node) {
    if (!node) return "";

    if (node->type == OPERATOR) {
      std::string operands = "";
      for (auto &&c : node->children) {
        std::string sep = operands.empty() ? "" : ", ";
        operands += sep + rebuild_helper(c.get());
      }

      return node->value + "(" + operands + ")";
    }

    return node->value;
  }

 public:
  parse_tree(){};

  void build(const std::string &expression) {
    build_helper(nullptr, expression, 0);
    assert(!!this->root);
  }

  void print() {
    assert(!!this->root);
    print_helper(this->root.get(), 0);
  }

  void transform_to_custom_op() {
    assert(!!this->root);
    parse_node *transformed_root = this->root->transform_to_custom_op();
    if (transformed_root != this->root.get()) {
      this->root.reset(transformed_root);
    }
  }

  void split_inequality_join_into_join_and_filter(std::string & join_out, std::string & filter_out)  {
    assert(!!this->root);
    assert(this->root.get()->type == OPERATOR);

    if (this->root.get()->value == "="){
      // this would be a regular single equality join
      join_out = this->rebuildExpression();  // the join_out is the same as the original input
      filter_out = "";  // no filter out
    } else if (this->root.get()->value == "AND"){
      int num_equalities = 0;
      for (auto &&c : this->root.get()->children) {
        if (c.get()->value == "=") {
          num_equalities++;
        }
      }
      if (num_equalities == this->root.get()->children.size()) { // all are equalities. this would be a regular multiple equality join
        join_out = this->rebuildExpression();  // the join_out is the same as the original input
        filter_out = "";  // no filter out
      } else if (num_equalities > 0) {  // i can split this into an equality join and a filter
        if (num_equalities == 1){ // if there is only one equality, then the root for join_out wont be an AND, and we will just have this equality as the root
          if (this->root.get()->children.size() == 2){
            for (auto &&c : this->root.get()->children) {
              if (c.get()->value == "=") {
                join_out = rebuild_helper(c.get());
              } else {
                filter_out = rebuild_helper(c.get());
              }
            }
          } else {
            parse_node *filter_root = new operator_node{"AND"};
            for (auto &&c : this->root.get()->children) {
              if (c.get()->value == "=") {
                join_out = rebuild_helper(c.get());
              } else {
                filter_root->children.push_back( std::unique_ptr<parse_node>(c.release()) );
              }
            }
            filter_out = rebuild_helper(filter_root);
          }
        } else if (num_equalities == this->root.get()->children.size() - 1){ // only one that does not have an inequality and therefore will be in the filter (without an and at the root)
          parse_node *join_out_root = new operator_node{"AND"};
          for (auto &&c : this->root.get()->children) {
            if (c.get()->value == "=") {
              join_out_root->children.push_back( std::unique_ptr<parse_node>(c.release()) );                 
            } else {
              filter_out = rebuild_helper(c.get());
            }
          }
          join_out = rebuild_helper(join_out_root);
        } else {
          parse_node *join_out_root = new operator_node{"AND"};
          parse_node *filter_root = new operator_node{"AND"};
            for (auto &&c : this->root.get()->children) {
              if (c.get()->value == "=") {
                join_out_root->children.push_back( std::unique_ptr<parse_node>(c.release()) );     
              } else {
                filter_root->children.push_back( std::unique_ptr<parse_node>(c.release()) ); 
              }
            }
            join_out = rebuild_helper(join_out_root);
            filter_out = rebuild_helper(filter_root);
        }
      } else {  // this is not supported. Throw error
        std::string original_join_condition = this->rebuildExpression();
        throw std::runtime_error("Join condition is currently not supported. Join received: " + original_join_condition);
      }
    } else { // this is not supported. Throw error 
      std::string original_join_condition = this->rebuildExpression();
      throw std::runtime_error("Join condition is currently not supported. Join received: " + original_join_condition);
    }
  }

  std::string rebuildExpression() {
    assert(!!this->root);
    return rebuild_helper(this->root.get());
  }
};

}  // namespace parser
}  // namespace ral