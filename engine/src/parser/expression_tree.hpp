#pragma once

#include <cassert>
#include <algorithm>
#include <blazingdb/io/Util/StringUtil.h>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <cudf/types.hpp>

#include "CalciteExpressionParsing.h"
#include "skip_data/utils.hpp"
#include "expression_utils.hpp"
#include "error.hpp"

namespace ral {
namespace parser {

enum class node_type { OPERATOR, OPERAND, LITERAL, VARIABLE };

struct node;
struct operad_node;
struct operator_node;

struct node_visitor {
    virtual void visit(const operad_node& node) = 0;
    virtual void visit(const operator_node& node) = 0;
};

struct node_transformer {
    virtual node * transform(operad_node& node) = 0;
    virtual node * transform(operator_node& node) = 0;
};

struct node {
    node(node_type type, const std::string& value) : type{ type }, value{ value } {};

    virtual node * clone() const = 0;

    virtual void accept(node_visitor&) const = 0;
    virtual node * accept(node_transformer&) = 0;

    node_type type;
    std::string value;
    std::vector<std::unique_ptr<node>> children;
};

struct operad_node : public node {
    using node::node;

    void accept(node_visitor& visitor) const override { visitor.visit(*this); }
    node * accept(node_transformer& transformer) override { return transformer.transform(*this); }
};

struct literal_node : public operad_node {
    literal_node(const std::string& value, cudf::data_type type) : operad_node{node_type::LITERAL, value}, _type{type} {};

    node * clone() const override { return new literal_node(this->value, this->_type); };

    cudf::data_type type() const { return _type; }

private:
    cudf::data_type _type;
};

struct variable_node : public operad_node {
    variable_node(const std::string& value) : operad_node{node_type::VARIABLE, value} {
        index_ = std::stoi(value.substr(1, value.size() - 1));
    };

    node * clone() const override { return new variable_node(this->value); };

    cudf::size_type index() const { return index_; }

private:
    cudf::size_type index_;
};

struct operator_node : public node {
    operator_node(const std::string& value) : node{ node_type::OPERATOR, value } {};

    node * clone() const override {
        node * ret = new operator_node(this->value);

        ret->children.reserve(this->children.size());
        for (auto&& c : this->children) {
            ret->children.push_back(std::unique_ptr<node>(c->clone()));
        }

        return ret;
    };

    void accept(node_visitor& visitor) const override {
        for (auto&& c : this->children) {
            c->accept(visitor);
        }
        visitor.visit(*this);
    }

    node * accept(node_transformer& transformer) override {
        for (auto&& c : this->children) {
            node * transformed_node = c->accept(transformer);
            if(transformed_node != c.get()) {
                c.reset(transformed_node);
            }
        }
        return transformer.transform(*this);
    }
};

namespace detail {

inline void print_helper(const node* node, size_t depth) {
    if (!node)
        return;

    if (node->children.size() > 1)
        print_helper(node->children[1].get(), depth + 1);

    for(size_t i = 0; i < depth; ++i) {
        std::cout << "    ";
    }

    std::cout << node->value << "\n";

    if (node->children.size() > 0)
        print_helper(node->children[0].get(), depth + 1);

    // for(auto && c : node->children) {
    // 	print_helper(c.get(), depth + 1);
    // }
}

inline std::string rebuild_helper(const node* node) {
    if (!node)
        return "";

    if (node->type == node_type::OPERATOR) {
        std::string operands = "";
        for (auto&& c : node->children) {
            std::string sep = operands.empty() ? "" : ", ";
            operands += sep + rebuild_helper(c.get());
        }

        return node->value + "(" + operands + ")";
    }

    return node->value;
}

inline std::string tokenizer_helper(const node* node) {
    if (!node)
        return "";

    if (node->value.length() > 0 and node->type == node_type::OPERATOR) {
        std::string operands = "";
        for (auto&& c : node->children) {
            std::string sep = operands.empty() ? "" : " ";
            operands += sep + tokenizer_helper(c.get());
        }

        return node->value + " " + operands;
    }

    return node->value;
}

struct custom_op_transformer : public node_transformer {
public:
    node * transform(operad_node& node) override { return &node; }

    node * transform(operator_node& node) override {
        if(node.value == "CASE") {
            return transform_case(node, 0);
        } else if(node.value == "Reinterpret") {
            return remove_reinterpret(node);
        } else if(node.value == "ROUND") {
            return transform_round(node);
        }

        return &node;
    }

private:
    node * transform_case(operator_node& case_node, size_t child_idx) {
        assert(case_node.children.size() >= 3 && case_node.children.size() % 2 != 0);
        assert(child_idx < case_node.children.size());

        if (child_idx == case_node.children.size() - 1) {
            return case_node.children[child_idx].release();
        }

        auto condition = std::move(case_node.children[child_idx]);
        auto then = std::move(case_node.children[child_idx + 1]);

        auto magic_if_not = std::unique_ptr<node>(new operator_node("MAGIC_IF_NOT"));
        magic_if_not->children.push_back(std::move(condition));
        magic_if_not->children.push_back(std::move(then));

        node * first_non_magic = new operator_node{"FIRST_NON_MAGIC"};
        first_non_magic->children.push_back(std::move(magic_if_not));
        first_non_magic->children.push_back(std::unique_ptr<node>(transform_case(case_node, child_idx + 2)));

        return first_non_magic;
    }

    node * remove_reinterpret(operator_node& reinterpret_node) {
        assert(reinterpret_node.children.size() == 1);

        return reinterpret_node.children[0].release();
    }

    node * transform_round(operator_node& round_node) {
        assert(round_node.children.size() == 1 || round_node.children.size() == 2);

        if (round_node.children.size() == 1) {
            round_node.children.push_back(std::unique_ptr<node>(new literal_node("0", cudf::data_type{cudf::type_id::INT8})));
        }

        return &round_node;
    }
};

struct skip_data_transformer : public node_transformer {
public:
    node * transform(operad_node& node) override { return &node; }

    node * transform(operator_node& node) override {
        const std::string & op = node.value;
        if (op == "=") {
            return transform_equal(node);
        } else if (op == "<" or op == "<=") {
            return transform_less_or_lesseq(node);
        } else if (op == ">" or op == ">=") {
            return transform_greater_or_greatereq(node);
        } else if (op == "+" or op == "-") {
            return transform_sum_or_sub(node);
        }

        // just skip like AND or OR
        return &node;
    }

private:
    node * transform_operand_inc(operad_node * node, int inc) {
        if (node->type == node_type::VARIABLE ) {
            auto id = ral::skip_data::get_id(node->value);
            std::string new_expr = "$" + std::to_string(2 * id + inc);

            return new variable_node(new_expr);
        }

        return node->clone();
    }

    node * transform_operator_inc(operator_node * node, int inc) {
        assert(node->children.size() == 2);

        if (node->value == "&&&") {
            if (inc == 0) {
                assert(node->children[0] != nullptr);
                return node->children[0].release();
            } else {
                assert(node->children[1] != nullptr);
                return node->children[1].release();
            }
        }

        return node->clone();
    }

    node * transform_inc(node * node, int inc) {
        if (node->type == node_type::OPERATOR) {
            return transform_operator_inc(static_cast<operator_node*>(node), inc);
        } else {
            return transform_operand_inc(static_cast<operad_node*>(node), inc);
        }
    }

    node * transform_equal(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        auto less_eq = std::unique_ptr<node>(new operator_node("<="));
        less_eq->children.push_back(std::unique_ptr<node>(transform_inc(n, 0)));
        less_eq->children.push_back(std::unique_ptr<node>(transform_inc(m, 1)));

        auto greater_eq = std::unique_ptr<node>(new operator_node(">="));
        greater_eq->children.push_back(std::unique_ptr<node>(transform_inc(n, 1)));
        greater_eq->children.push_back(std::unique_ptr<node>(transform_inc(m, 0)));

        node * and_node = new operator_node("AND");
        and_node->children.push_back(std::move(less_eq));
        and_node->children.push_back(std::move(greater_eq));

        return and_node;
    }

    node * transform_less_or_lesseq(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        node * ptr = new operator_node(operator_node_.value);
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(n, 0)));
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(m, 1)));

        return ptr;
    }

    node * transform_greater_or_greatereq(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        node * ptr = new operator_node(operator_node_.value);
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(n, 1)));
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(m, 0)));

        return ptr;
    }

    node * transform_sum_or_sub(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        auto expr1 = std::unique_ptr<node>(new operator_node(operator_node_.value));
        expr1->children.push_back(std::unique_ptr<node>(transform_inc(n, 0)));
        expr1->children.push_back(std::unique_ptr<node>(transform_inc(m, 0)));

        auto expr2 = std::unique_ptr<node>(new operator_node(operator_node_.value));
        expr2->children.push_back(std::unique_ptr<node>(transform_inc(n, 1)));
        expr2->children.push_back(std::unique_ptr<node>(transform_inc(m, 1)));

        node * ptr = new operator_node("&&&"); // this is parent node (), for sum, sub after skip_data
        ptr->children.push_back(std::move(expr1));
        ptr->children.push_back(std::move(expr2));

        return ptr;
    }
};

struct skip_data_reducer : public node_transformer {
public:
    node * transform(operad_node& node) override { return &node; }

    node * transform(operator_node& node) override {
        if (ral::skip_data::is_unsupported_binary_op(node.value)) {
            return new operator_node("NONE");
        }

        assert(node.children.size() == 2);

        auto& n = node.children[0];
        auto& m = node.children[1];

        bool left_is_exclusion_unary_op = false;
        bool right_is_exclusion_unary_op = false;
        if (n->type == node_type::OPERATOR) {
            if (ral::skip_data::is_exclusion_unary_op(n->value)) {
                left_is_exclusion_unary_op = true;
            }
        }
        if (m->type == node_type::OPERATOR) {
            if (ral::skip_data::is_exclusion_unary_op(m->value)) {
                right_is_exclusion_unary_op = true;
            }
        }
        if (left_is_exclusion_unary_op and not right_is_exclusion_unary_op) {
            if (node.value == "AND") {
                return m.release();
            } else {
                return new operator_node("NONE");
            }
        } else if (right_is_exclusion_unary_op and not left_is_exclusion_unary_op) {
            if (node.value == "AND") {
                return n.release();
            } else {
                return new operator_node("NONE");
            }
        } else if (left_is_exclusion_unary_op and right_is_exclusion_unary_op) {
            return new operator_node("NONE");
        }

        return &node;
    }
};

struct skip_data_drop_transformer : public node_transformer {
public:
    skip_data_drop_transformer(const std::string & drop_value) : drop_value_{drop_value} {}

    node * transform(operad_node& node) override {
        if (node.value == drop_value_) {
            return new operator_node("NONE");
        }

        return &node;
    }

    node * transform(operator_node& node) override {
        if (node.value == drop_value_) {
            return new operator_node("NONE");
        }

        return &node;
    }

private:
    std::string drop_value_;
};

class lexer
{
public:
    constexpr static char VARIABLE_REGEX_STR[] = R"(\$\d+)";
    constexpr static char NULL_REGEX_STR[] = R"(null)";
    constexpr static char BOOLEAN_REGEX_STR[] = R"(true|false)";
    constexpr static char NUMBER_REGEX_STR[] = R"([-+]?\d*\.?\d+([eE][-+]?\d+)?)";
    constexpr static char TIMESTAMP_REGEX_STR[] = R"(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?)";
    constexpr static char STRING_REGEX_STR[] = R"((["'])(?:(?!\1|\\).|\\.)*?\1)";

    enum class token_type
    {
        ParenthesisOpen,
        ParenthesisClose,
        Comma,
        Colon,
        Variable,
        Null,
        Boolean,
        Number,
        Timestamp,
        String,
        Identifier,
        EOF_
    };

    struct token
    {
        token_type type;
        std::string value;
    };

public:
    explicit lexer(const std::string & str);

    token next_token() ;

private:
    void advance(size_t offset = 1);

    std::string text_;
    size_t pos_;

    std::regex variable_regex{"^" + std::string(lexer::VARIABLE_REGEX_STR)};
    std::regex null_regex{"^" + std::string(lexer::NULL_REGEX_STR)};
    std::regex boolean_regex{"^" + std::string(lexer::BOOLEAN_REGEX_STR)};
    std::regex number_regex{"^" + std::string(lexer::NUMBER_REGEX_STR)};
    std::regex timestamp_regex{"^" + std::string(lexer::TIMESTAMP_REGEX_STR)};
    std::regex string_regex{"^" + std::string(lexer::STRING_REGEX_STR)};
};

class expr_parser {
public:
    explicit expr_parser(const std::string & expr_str);

    std::unique_ptr<node> parse();

private:
    bool accept(lexer::token_type type);

    std::unique_ptr<node> expr();

    std::unique_ptr<node> term();

    std::unique_ptr<node> func();

    std::vector<std::unique_ptr<node>> func_args();

    std::unique_ptr<node> literal();

    cudf::data_type infer_type_from_literal_token(const lexer::token & token);

    cudf::data_type type_from_type_token(const lexer::token & token);

    lexer lexer_;
    lexer::token token_;
};

} // namespace detail

class parse_tree {
private:
    std::unique_ptr<node> root_;

public:
    parse_tree() = default;
    ~parse_tree() = default;

    parse_tree(const parse_tree & other) = delete;
    parse_tree& operator=(const parse_tree & other) = delete;

    parse_tree(parse_tree&& other) : root_{std::move(other.root_)} { }
    parse_tree& operator=(parse_tree&& other) = delete;

    const node & root() {
        assert(!!this->root_);
        return *(this->root_);
    }

    bool build(const std::string& expression) {
        detail::expr_parser parser(expression);
        this->root_ = parser.parse();
        assert(!!this->root_);
        return true;
    }

    void print() const {
        assert(!!this->root_);
        detail::print_helper(this->root_.get(), 0);
    }

    void visit(node_visitor& visitor) const {
        assert(!!this->root_);
        this->root_->accept(visitor);
    }

    void transform(node_transformer& transformer) {
        assert(!!this->root_);
        node * transformed_root = this->root_->accept(transformer);
        if(transformed_root != this->root_.get()) {
            this->root_.reset(transformed_root);
        }
    }

    void transform_to_custom_op() {
        assert(!!this->root_);
        detail::custom_op_transformer t;
        transform(t);
    }

    void apply_skip_data_rules() {
        assert(!!this->root_);

        detail::skip_data_reducer r;
        transform(r);

        if (this->root_->value == "NONE") {
            this->root_->value = "";
            return;
        }

        detail::skip_data_transformer t;
        transform(t);
    }

    void drop(const std::vector<std::string> & column_names) {
        for (const auto &col_name : column_names) {
            detail::skip_data_drop_transformer t(col_name);
            transform(t);
        }
    }

    std::string rebuildExpression() {
        assert(!!this->root_);
        return detail::rebuild_helper(this->root_.get());
    }

    std::string prefix() {
        assert(!!this->root_);
        return detail::tokenizer_helper(this->root_.get());
    }
};

}  // namespace parser
}  // namespace ral
