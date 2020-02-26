#include "parser/expression_utils.hpp"
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
#include <memory>

namespace ral {
namespace skip_data {


enum class node_type {
  BINARY_OP_NODE,
  PAIR_NODE,
  UNARY_OP_NODE,
  CONSTANT_NODE,
  VARIABLE_NODE
};

struct abstract_node {
  std::shared_ptr<abstract_node> left{nullptr};
  std::shared_ptr<abstract_node> right{nullptr};
  node_type type;
  virtual ~abstract_node(){

  }
  explicit abstract_node(node_type _type) : type{_type} {}
  virtual std::shared_ptr<abstract_node> transform(int inc) = 0;
  virtual std::shared_ptr<abstract_node> reduce() = 0;
  virtual std::string to_string() = 0;
};

struct unary_op_node : abstract_node, std::enable_shared_from_this<unary_op_node> {
  std::string data;
  explicit unary_op_node(std::string _data)
      : abstract_node{node_type::UNARY_OP_NODE}, data{_data} {}

  virtual std::shared_ptr<abstract_node> transform(int inc) override {
    auto ptr = std::make_shared<unary_op_node>(data);
    ptr->left = this->left;
    ptr->right = nullptr;
    return ptr;
  }
  virtual std::shared_ptr<abstract_node> reduce() override {
    if (is_unsupported_binary_op(this->to_string())) {
      return  std::make_shared<unary_op_node>("NONE");
    } else {
      return shared_from_this();
    }    
  }
  virtual std::string to_string() override { return data; }
};

struct binary_op_node : abstract_node {
  std::string data;
  explicit binary_op_node(std::string _data)
      : abstract_node{node_type::BINARY_OP_NODE}, data{_data} {}
  virtual std::shared_ptr<abstract_node> transform(int inc) override {
    auto ptr = std::make_shared<binary_op_node>(data);
    ptr->left = this->left;
    ptr->right = this->right;
    return ptr;
  }
  std::shared_ptr<abstract_node> reduce_helper(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m) {

    auto ptr = std::make_shared<binary_op_node>(data);
    ptr->left = n;
    ptr->right = m;
    bool left_is_exclusion_unary_op = false;
    bool right_is_exclusion_unary_op = false;
    if (n->type == node_type::UNARY_OP_NODE) {
      if (is_exclusion_unary_op(n->to_string())) {
        left_is_exclusion_unary_op = true;
      }
    }
    if (m->type == node_type::UNARY_OP_NODE) {
      if (is_exclusion_unary_op(m->to_string())) {
        right_is_exclusion_unary_op = true;
      }
    }
    if (left_is_exclusion_unary_op and not right_is_exclusion_unary_op) {
      if (this->to_string() == "AND") {
        return m;
      } else {
        return std::make_shared<unary_op_node>("NONE");
      }
    } else if (right_is_exclusion_unary_op and not left_is_exclusion_unary_op) {
      if (this->to_string() == "AND") {
        return n;
      } else {
        return std::make_shared<unary_op_node>("NONE");
      }
    } else if (left_is_exclusion_unary_op and right_is_exclusion_unary_op) {
      return std::make_shared<unary_op_node>("NONE");
    }
    return ptr;
  }

  virtual  std::shared_ptr<abstract_node>  reduce() override {
    if (is_unsupported_binary_op(this->to_string())) {
      return  std::make_shared<unary_op_node>("NONE");
    }
    auto n = this->left->reduce();
    auto m = this->right->reduce();
    return reduce_helper(n, m);
  }
  virtual std::string to_string() override { return data; }
};

struct pair_node : abstract_node {
  explicit pair_node() : abstract_node{node_type::PAIR_NODE} {}
  virtual std::shared_ptr<abstract_node> transform(int inc) override {
    if (inc == 0) // min
      return std::shared_ptr<abstract_node>(this->left);
    return std::shared_ptr<abstract_node>(this->right);
  }
  // check this after
  virtual std::shared_ptr<abstract_node> reduce() override { return nullptr; }

  virtual std::string to_string() override { return "&&&"; }
};

struct var_node : abstract_node, std::enable_shared_from_this<var_node> {
  std::string data;
  explicit var_node(std::string _data)
      : abstract_node{node_type::VARIABLE_NODE}, data{_data} {}
  virtual std::shared_ptr<abstract_node> transform(int inc) override {
    auto id = get_id(data);
    auto ptr = std::make_shared<var_node>("$" + std::to_string(2 * id + inc));
    ptr->left = this->left;
    ptr->right = this->right;
    return ptr;
  }
  virtual std::shared_ptr<abstract_node> reduce() override { return shared_from_this(); }
  virtual std::string to_string() override { return data; }
};

struct const_node : abstract_node, std::enable_shared_from_this<const_node> {
  std::string data;
  explicit const_node(std::string _data)
      : abstract_node{node_type::VARIABLE_NODE}, data{_data} {}
  virtual std::shared_ptr<abstract_node> transform(int inc) override {
    auto ptr = std::make_shared<binary_op_node>(data);
    ptr->left = this->left;
    ptr->right = this->right;
    return ptr;
  }
  virtual std::shared_ptr<abstract_node> reduce() override { return  shared_from_this(); }
  virtual std::string to_string() override { return data; }
};

class expression_tree {
  std::shared_ptr<abstract_node> root{nullptr};

private:
  std::shared_ptr<abstract_node> make_and(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m);
  std::shared_ptr<abstract_node> make_greater_eq(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m, int inc_n,
                                 int inc_m);
  std::shared_ptr<abstract_node> make_less_eq(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m, int inc_n,
                              int inc_m);
  std::shared_ptr<abstract_node> make_pair_node(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m);
  std::shared_ptr<abstract_node> make_sum(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m, int inc_n,
                          int inc_m);
  std::shared_ptr<abstract_node> make_sub(std::shared_ptr<abstract_node> &n, std::shared_ptr<abstract_node> &m, int inc_n,
                          int inc_m);
  std::shared_ptr<abstract_node> transform_less_or_lesseq(std::shared_ptr<abstract_node>& subtree,
                                          std::string op);
  std::shared_ptr<abstract_node>transform_greater_or_greatereq(std::shared_ptr<abstract_node> &subtree,
                                                std::string op);
  std::shared_ptr<abstract_node>transform_equal(std::shared_ptr<abstract_node> &subtree);
  std::shared_ptr<abstract_node>transform_make_sum(std::shared_ptr<abstract_node> &subtree);

  std::shared_ptr<abstract_node>transform_make_sub(std::shared_ptr<abstract_node> &subtree);
  void get_prefix_nodes( std::shared_ptr<abstract_node> &p,
                         std::stack<  std::shared_ptr<abstract_node>*> &nodes);

public:
  bool build(std::string str);
  
  void print();

  std::string prefix();
  std::string rebuildExpression();
  void apply_skip_data_rules();
  void drop(std::vector<std::string> const &column_names);

private:
  void apply_skip_data_rules_helper(std::stack< std::shared_ptr<abstract_node> *> &nodes);
  int build(std::shared_ptr<abstract_node> &parent, const std::vector<std::string> &parts,
            int index);
  // Function to print the print expression for the tree
  void print_helper(abstract_node *p, int level);
  void prefix_helper(abstract_node *p, std::stringstream &out);
  void rebuild_helper(abstract_node *p, std::stringstream &out);
};

} // namespace skip_data
} // namespace ral