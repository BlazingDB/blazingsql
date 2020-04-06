#include "expression_tree.hpp"
#include "utils.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stack>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "parser/expression_tree.hpp"
#include "parser/expression_utils.hpp"

namespace ral {
namespace skip_data {


std::shared_ptr<abstract_node> expression_tree::make_and(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m) {
  auto ptr = std::make_shared<binary_op_node>("AND");
  ptr->left = n;
  ptr->right = m;
  return ptr;
}
std::shared_ptr<abstract_node> expression_tree::make_greater_eq(std::shared_ptr<abstract_node> &n,
                                                std::shared_ptr<abstract_node> &m, int inc_n,
                                                int inc_m) {
  auto ptr = std::make_shared<binary_op_node>(">=");
  ptr->left = n->transform(inc_n);
  ptr->right = m->transform(inc_m);
  return ptr;
}
std::shared_ptr<abstract_node> expression_tree::make_less_eq(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m,
                                             int inc_n, int inc_m) {
  auto ptr = std::make_shared<binary_op_node>("<=");
  ptr->left = n->transform(inc_n);
  ptr->right = m->transform(inc_m);
  return ptr;
}

std::shared_ptr<abstract_node> expression_tree::make_pair_node(std::shared_ptr<abstract_node> &n,
                                               std::shared_ptr<abstract_node> &m) {
  auto ptr = std::make_shared<pair_node>();
  ptr->left = n;
  ptr->right = m;
  return ptr;
}
std::shared_ptr<abstract_node> expression_tree::make_sum(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m,
                                         int inc_n, int inc_m) {
  auto ptr = std::make_shared<binary_op_node>("+");
  ptr->left = n->transform(inc_n);
  ptr->right = m->transform(inc_m);
  return ptr;
}
std::shared_ptr<abstract_node> expression_tree::make_sub(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m,
                                         int inc_n, int inc_m) {
  auto ptr = std::make_shared<binary_op_node>("-");
  ptr->left = n->transform(inc_n);
  ptr->right = m->transform(inc_m);
  return ptr;
}
std::shared_ptr<abstract_node> expression_tree::transform_less_or_lesseq(std::shared_ptr<abstract_node> &subtree,
                                                         std::string op) {
  auto n = subtree->left;
  auto m = subtree->right;
  auto ptr = std::make_shared<binary_op_node>(op);
  ptr->left = n->transform(0);
  ptr->right = m->transform(1);
  return ptr;
}

std::shared_ptr<abstract_node> expression_tree::transform_greater_or_greatereq(std::shared_ptr<abstract_node> &subtree,
                                                std::string op) {
  auto n = subtree->left;
  auto m = subtree->right;
  auto ptr = std::make_shared<binary_op_node>(op);
  ptr->left = n->transform(1);
  ptr->right = m->transform(0);
  return ptr;
}
std::shared_ptr<abstract_node> expression_tree::transform_equal(std::shared_ptr<abstract_node> &subtree) {
  auto n = subtree->left;
  auto m = subtree->right;
  auto less_eq = make_less_eq(n, m, 0, 1);
  auto greater_eq = make_greater_eq(n, m, 1, 0);
  return make_and(less_eq, greater_eq);
}

std::shared_ptr<abstract_node> expression_tree::transform_make_sum(std::shared_ptr<abstract_node> &subtree) {
  auto n = subtree->left;
  auto m = subtree->right;
  auto first = make_sum(n, m, 0, 0);
  auto second = make_sum(n, m, 1, 1);
    return make_pair_node(first, second);
}

std::shared_ptr<abstract_node> expression_tree::transform_make_sub(std::shared_ptr<abstract_node> &subtree) {
  auto n = subtree->left;
  auto m = subtree->right;
  auto expr1 = make_sub(n, m, 0, 0);
  auto expr2 = make_sub(n, m, 1, 1);
  return make_pair_node(expr1, expr2);
}

void expression_tree::get_prefix_nodes( std::shared_ptr<abstract_node> &p,
                                       std::stack<  std::shared_ptr<abstract_node>*> &nodes) {
  if (p == nullptr) {
    return;
  } else {
    nodes.push(&p);
    get_prefix_nodes(p->left, nodes);
    get_prefix_nodes(p->right, nodes);
  }
}

void expression_tree::apply_skip_data_rules_helper(
    std::stack< std::shared_ptr<abstract_node> *> &nodes) {
  while (not nodes.empty()) {
    std::shared_ptr<abstract_node> *p = nodes.top();
    if ((*p)->type == node_type::BINARY_OP_NODE) {
      std::shared_ptr<binary_op_node> bin_node = std::dynamic_pointer_cast<binary_op_node>(*p);
      auto op = bin_node->data;
      if (op == "=") {
        *p = transform_equal(*p);
      } else if (op == "<" or op == "<=") {
        *p = transform_less_or_lesseq(*p, op);
      } else if (op == ">" or op == ">=") {
        *p = transform_greater_or_greatereq(*p, op);
      } else if (op == "+") {
        *p = transform_make_sum(*p);
      } else if (op == "-") {
        *p = transform_make_sub(*p);
      } else {
        // just skip like AND or OR
      }
    } else if ((*p)->type == node_type::UNARY_OP_NODE) {
      // TODO here when not is enable!
      //auto op = ((unary_op_node *)(*p))->data;
      // std::cout << ((unary_op_node *) p)->data << " ";
    }
    nodes.pop();
  }
}


void expression_tree::apply_skip_data_rules() {
  if (root){
    root = root->reduce();
    if (root->to_string() == "NONE") {
      root = nullptr;
      return;
    }
    std::stack< std::shared_ptr<abstract_node> *> nodes;
    get_prefix_nodes(root, nodes);
    apply_skip_data_rules_helper(nodes);
  }
}

template <typename predicate>
void drop_helper(std::shared_ptr<abstract_node> &p, predicate pred) {
  if (p == nullptr) {
    return;
  } else {
    if (pred(p.get())) {
      p = std::make_shared<unary_op_node>("NONE");
    }
    drop_helper(p->left, pred);
    drop_helper(p->right, pred);
  }
}

void expression_tree::drop(std::vector<std::string> const &column_names) {
  for (auto &col_name : column_names) {
    drop_helper(this->root, [&col_name](abstract_node *p) {
      return p->to_string() == col_name;
    });
  }
}

bool expression_tree::build(std::string str) {
  // lets use our newest good parser to help us tokenize until we merge both expression tree parsers
  ral::parser::parse_tree tree;
	tree.build(str);
  tree.transform_to_custom_op();
  std::string tokenizable_string = tree.buildTokenizableString();
  std::vector<std::string> tokens = split(tokenizable_string, "@#@"); 
  
  return build(root, tokens, 0) == -1;
}

void expression_tree::print() {
  print_helper(root.get(), 0);
  printf("\n");
}
std::string expression_tree::prefix() {
  std::stringstream ss;
  prefix_helper(root.get(), ss);
  std::string returned_str = ss.str();
  return returned_str.substr(0, returned_str.length() - 1);
}

std::string expression_tree::rebuildExpression() {
  std::stringstream ss;
  rebuild_helper(root.get(), ss);
  std::string returned_str = ss.str();
  return returned_str;
}

int expression_tree::build(std::shared_ptr<abstract_node> &parent,
                           const std::vector<std::string> &parts, int index) {
  // If its the end of the expression
  if (parts.size() == index)
    return -1;

  while (true) {
    auto str = parts[index];
    if (parent == nullptr) {
      if (is_var_column(str))
        parent = std::make_shared<var_node>(str);
      else if (is_literal(str))
        parent = std::make_shared<const_node>(str);
      else if (is_unary_op(str))
        parent = std::make_shared<unary_op_node>(str);
      else
        parent = std::make_shared<binary_op_node>(str);
    } else {
      // If the character is an operand
      if (is_var_column(str) || is_literal(str)) {
        return index;
      }
      bool unary_operation = false;
      bool binary_operation = false;
      if (is_unary_op(str)) {
        unary_operation = true;
      } else if (is_binary_op(str)) {
        binary_operation = true;
      }
      if (binary_operation or unary_operation) {
        // Build the left sub-tree
        auto current = build(parent->left, parts, index + 1);
        if (current == -2)
          return -2;
        // Build the right sub-tree
        if (not unary_operation) {
          if (current == -1)
            return -2;
          current = build(parent->right, parts, current + 1);
          if (current == -2)
            return -2;
        }
        if (parts.size() == current + 1)
          return -1;
        return current;
      } else {
        return -2;
      }
    }
  }
}

// Function to print the print expression for the tree
void expression_tree::print_helper(abstract_node *p, int level) // recursion
{
  if (p == nullptr) {
    return;
  } else {
    print_helper(p->right.get(), level + 1);
    for (int i = 0; i < level; ++i) {
      std::cout << "    ";
    }
    std::cout << p->to_string() << "\n";
    print_helper(p->left.get(), level + 1);
  }
}
void expression_tree::prefix_helper(abstract_node *p, std::stringstream &out) {
  if (p == nullptr) {
    return;
  } else {
    out << p->to_string() << " ";
    prefix_helper(p->left.get(), out);
    prefix_helper(p->right.get(), out);
  }
}
void expression_tree::rebuild_helper(abstract_node *p, std::stringstream &out) {
  if (p == nullptr) {
    return;
  } else {
    if (p->left.get() != nullptr && p->right.get() != nullptr &&
            p->left.get()->to_string() != "" && p->right.get()->to_string() != "" ){ // binary op
      out << p->to_string() << "(";
      rebuild_helper(p->left.get(), out);
      out << ", ";
      rebuild_helper(p->right.get(), out);
      out << ")";
    } else if (p->left.get() != nullptr && p->left.get()->to_string() != "") { // unary op
      out << p->to_string() << "(";
      rebuild_helper(p->left.get(), out);
      out << ")";
    } else {
      out << p->to_string();
    }    
  }
}

} // namespace skip_data
} // namespace ral