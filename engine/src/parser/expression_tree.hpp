#pragma once

#include <algorithm>
#include <blazingdb/io/Util/StringUtil.h>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <stack>

#include "parser/expression_utils.hpp"
#include "skip_data/utils.hpp"

namespace ral {
namespace parser {

enum class parse_node_type { OPERATOR, OPERAND };

struct parse_node;
struct operad_node;
struct operator_node;

struct parse_node_visitor {
    virtual void visit(const operad_node& node) = 0;
    virtual void visit(const operator_node& node) = 0;
};

struct parse_node_transformer {
    virtual parse_node * transform(operad_node& node) = 0;
    virtual parse_node * transform(operator_node& node) = 0;
};

struct parse_node {
    parse_node_type type;
    std::vector<std::unique_ptr<parse_node>> children;
    std::string value;

    parse_node(parse_node_type type, const std::string& value) : type{ type }, value{ value } {};

    virtual parse_node * clone() = 0;

    virtual void accept(parse_node_visitor&) = 0;

    virtual parse_node * accept(parse_node_transformer&) = 0;
};

struct operad_node : parse_node {
    operad_node(const std::string& value) : parse_node{ parse_node_type::OPERAND, value } {};

    parse_node * clone() override { return new operad_node(this->value); };

    void accept(parse_node_visitor& visitor) override { visitor.visit(*this); }

    parse_node * accept(parse_node_transformer& transformer) override { return transformer.transform(*this); }
};

struct operator_node : parse_node {
    operator_node(const std::string& value) : parse_node{ parse_node_type::OPERATOR, value } {};

    parse_node * clone() override {
        parse_node * ret = new operator_node(this->value);

        ret->children.reserve(this->children.size());
        for (auto&& c : this->children) {
            ret->children.push_back(std::unique_ptr<parse_node>(c->clone()));
        }

        return ret;
    };

    void accept(parse_node_visitor& visitor) override {
        for (auto&& c : this->children) {
            c->accept(visitor);
        }
        visitor.visit(*this);
    }

    parse_node * accept(parse_node_transformer& transformer) override {
        for (auto&& c : this->children) {
            parse_node * transformed_node = c->accept(transformer);
            if(transformed_node != c.get()) {
                c.reset(transformed_node);
            }
        }
        return transformer.transform(*this);
    }
};

namespace detail {

inline void print_helper(const parse_node* node, size_t depth) {
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

inline std::string rebuild_helper(const parse_node* node) {
    if (!node)
        return "";

    if (node->type == parse_node_type::OPERATOR) {
        std::string operands = "";
        for (auto&& c : node->children) {
            std::string sep = operands.empty() ? "" : ", ";
            operands += sep + rebuild_helper(c.get());
        }

        return node->value + "(" + operands + ")";
    }

    return node->value;
}

inline std::string tokenizer_helper(const parse_node* node) {
    if (!node)
        return "";

    if (node->value.length() > 0 and node->type == parse_node_type::OPERATOR) {
        std::string operands = "";
        for (auto&& c : node->children) {
            std::string sep = operands.empty() ? "" : " ";
            operands += sep + tokenizer_helper(c.get());
        }

        return node->value + " " + operands;
    }

    return node->value;
}

struct custom_op_transformer : public parse_node_transformer {
public:
    parse_node * transform(operad_node& node) override { return &node; }

    parse_node * transform(operator_node& node) override {
        if(node.value == "CASE") {
            return transform_case(node, 0);
        } else if(node.value == "CAST") {
            return transform_cast(node);
        } else if(node.value == "Reinterpret") {
            return remove_reinterpret(node);
        } else if(node.value == "ROUND") {
            return transform_round(node);
        }

        return &node;
    }

private:
    parse_node * transform_case(operator_node& node, size_t child_idx) {
        assert(node.children.size() >= 3 && node.children.size() % 2 != 0);
        assert(child_idx < node.children.size());

        if (child_idx == node.children.size() - 1) {
            return node.children[child_idx].release();
        }

        auto condition = std::move(node.children[child_idx]);
        auto then = std::move(node.children[child_idx + 1]);

        auto magic_if_not = std::unique_ptr<parse_node>(new operator_node("MAGIC_IF_NOT"));
        magic_if_not->children.push_back(std::move(condition));
        magic_if_not->children.push_back(std::move(then));

        parse_node * first_non_magic = new operator_node{"FIRST_NON_MAGIC"};
        first_non_magic->children.push_back(std::move(magic_if_not));
        first_non_magic->children.push_back(std::unique_ptr<parse_node>(transform_case(node, child_idx + 2)));

        return first_non_magic;
    }

    parse_node * transform_cast(operator_node& node) {
        assert(node.children.size() == 2);

        auto exp = std::move(node.children[0]);
        std::string target_type = node.children[1]->value;

        parse_node * cast_op = new operator_node{"CAST_" + target_type};
        cast_op->children.push_back(std::move(exp));

        return cast_op;
    }

    parse_node * remove_reinterpret(operator_node& node) {
        assert(node.children.size() == 1);

        return node.children[0].release();
    }

    parse_node * transform_round(operator_node& node) {
        assert(node.children.size() == 1 || node.children.size() == 2);

        if (node.children.size() == 1) {
            node.children.push_back(std::unique_ptr<parse_node>(new operad_node("0")));
        }

        return &node;
    }
};

struct skip_data_transformer : public parse_node_transformer {
public:
    parse_node * transform(operad_node& node) override { return &node; }

    parse_node * transform(operator_node& node) override {
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
    parse_node * transform_operand_inc(operad_node * node, int inc) {
        if (is_var_column(node->value) ) {
            auto id = ral::skip_data::get_id(node->value);
            std::string new_expr = "$" + std::to_string(2 * id + inc);

            return new operad_node(new_expr);
        }

        return node->clone();
    }

    parse_node * transform_operator_inc(operator_node * node, int inc) {
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

    parse_node * transform_inc(parse_node * node, int inc) {
        if (node->type == parse_node_type::OPERAND) {
            return transform_operand_inc(static_cast<operad_node*>(node), inc);
        } else {
            return transform_operator_inc(static_cast<operator_node*>(node), inc);
        }
    }

    parse_node * transform_equal(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        parse_node * n = operator_node_.children[0].get();
        parse_node * m = operator_node_.children[1].get();

        auto less_eq = std::unique_ptr<parse_node>(new operator_node("<="));
        less_eq->children.push_back(std::unique_ptr<parse_node>(transform_inc(n, 0)));
        less_eq->children.push_back(std::unique_ptr<parse_node>(transform_inc(m, 1)));

        auto greater_eq = std::unique_ptr<parse_node>(new operator_node(">="));
        greater_eq->children.push_back(std::unique_ptr<parse_node>(transform_inc(n, 1)));
        greater_eq->children.push_back(std::unique_ptr<parse_node>(transform_inc(m, 0)));

        parse_node * and_node = new operator_node("AND");
        and_node->children.push_back(std::move(less_eq));
        and_node->children.push_back(std::move(greater_eq));

        return and_node;
    }

    parse_node * transform_less_or_lesseq(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        parse_node * n = operator_node_.children[0].get();
        parse_node * m = operator_node_.children[1].get();

        parse_node * ptr = new operator_node(operator_node_.value);
        ptr->children.push_back(std::unique_ptr<parse_node>(transform_inc(n, 0)));
        ptr->children.push_back(std::unique_ptr<parse_node>(transform_inc(m, 1)));

        return ptr;
    }

    parse_node * transform_greater_or_greatereq(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        parse_node * n = operator_node_.children[0].get();
        parse_node * m = operator_node_.children[1].get();

        parse_node * ptr = new operator_node(operator_node_.value);
        ptr->children.push_back(std::unique_ptr<parse_node>(transform_inc(n, 1)));
        ptr->children.push_back(std::unique_ptr<parse_node>(transform_inc(m, 0)));

        return ptr;
    }

    parse_node * transform_sum_or_sub(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        parse_node * n = operator_node_.children[0].get();
        parse_node * m = operator_node_.children[1].get();

        auto expr1 = std::unique_ptr<parse_node>(new operator_node(operator_node_.value));
        expr1->children.push_back(std::unique_ptr<parse_node>(transform_inc(n, 0)));
        expr1->children.push_back(std::unique_ptr<parse_node>(transform_inc(m, 0)));

        auto expr2 = std::unique_ptr<parse_node>(new operator_node(operator_node_.value));
        expr2->children.push_back(std::unique_ptr<parse_node>(transform_inc(n, 1)));
        expr2->children.push_back(std::unique_ptr<parse_node>(transform_inc(m, 1)));

        parse_node * ptr = new operator_node("&&&"); // this is parent node (), for sum, sub after skip_data
        ptr->children.push_back(std::move(expr1));
        ptr->children.push_back(std::move(expr2));

        return ptr;
    }
};

struct skip_data_reducer : public parse_node_transformer {
public:
    parse_node * transform(operad_node& node) override { return &node; }

    parse_node * transform(operator_node& node) override {
        if (ral::skip_data::is_unsupported_binary_op(node.value)) {
            return new operator_node("NONE");
        }

        assert(node.children.size() == 2);

        auto& n = node.children[0];
        auto& m = node.children[1];

        bool left_is_exclusion_unary_op = false;
        bool right_is_exclusion_unary_op = false;
        if (n->type == parse_node_type::OPERATOR) {
            if (ral::skip_data::is_exclusion_unary_op(n->value)) {
                left_is_exclusion_unary_op = true;
            }
        }
        if (m->type == parse_node_type::OPERATOR) {
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

struct skip_data_drop_transformer : public parse_node_transformer {
public:
    skip_data_drop_transformer(const std::string & drop_value) : drop_value_{drop_value} {}

    parse_node * transform(operad_node& node) override {
        if (node.value == drop_value_) {
            return new operator_node("NONE");
        }

        return &node;
    }

    parse_node * transform(operator_node& node) override {
        if (node.value == drop_value_) {
            return new operator_node("NONE");
        }

        return &node;
    }

private:
    std::string drop_value_;
};

} // namespace detail

struct parse_tree {
    std::unique_ptr<parse_node> root;

private:
    size_t build_helper(parse_node* parent_node, const std::string& expression, size_t pos) {
        bool return_type = false;
        while (pos != std::string::npos) {
            size_t nextPos = expression.find_first_of("(),", pos);
            std::string token = expression.substr(pos, nextPos - pos);
            token = StringUtil::ltrim(token);

            parse_node * new_node = nullptr;
            if (!token.empty()) {
                if (is_operator_token(token)) {
                    new_node = new operator_node(token);
                } else {
                    new_node = new operad_node(token);
                }

                if (!parent_node) {
                    this->root.reset(new_node);
                } else {
                    parent_node->children.push_back(std::unique_ptr<parse_node>(new_node));
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

            parse_node * new_node = new operad_node(token);
            parent_node->children.push_back(std::unique_ptr<parse_node>(new_node));

            // Don't advance position so that the parent can process it
            return nextPos;
        } else {
            assert(pos == std::string::npos);
        }

        return pos;
    }

public:
    parse_tree() = default;

    bool build(const std::string& expression) {
        build_helper(nullptr, expression, 0);
        assert(!!this->root);
        return true;
    }

    void print() {
        assert(!!this->root);
        detail::print_helper(this->root.get(), 0);
    }

    void visit(parse_node_visitor& visitor) {
        assert(!!this->root);
        this->root->accept(visitor);
    }

    void transform(parse_node_transformer& transformer) {
        assert(!!this->root);
        parse_node * transformed_root = this->root->accept(transformer);
        if(transformed_root != this->root.get()) {
            this->root.reset(transformed_root);
        }
    }

    void transform_to_custom_op() {
        assert(!!this->root);
        detail::custom_op_transformer t;
        transform(t);
    }

    void apply_skip_data_rules() {
        assert(!!this->root);

        detail::skip_data_reducer r;
        transform(r);

        if (this->root->value == "NONE") {
            this->root->value = "";
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

    void split_inequality_join_into_join_and_filter(std::string& join_out, std::string& filter_out) {
        assert(!!this->root);
        assert(this->root->type == parse_node_type::OPERATOR);

        if (this->root->value == "=") {
            // this would be a regular single equality join
            join_out = this->rebuildExpression();  // the join_out is the same as the original input
            filter_out = "";					   // no filter out
        } else if (this->root->value == "AND") {
            int num_equalities = 0;
            for (auto&& c : this->root->children) {
                if (c->value == "=") {
                    num_equalities++;
                }
            }
            if (num_equalities == this->root->children.size()) {  // all are equalities. this would be a regular multiple equality join
                join_out = this->rebuildExpression();  // the join_out is the same as the original input
                filter_out = "";					   // no filter out
            } else if (num_equalities > 0) {			   // i can split this into an equality join and a filter
                if (num_equalities == 1) {  // if there is only one equality, then the root for join_out wont be an AND,
                    // and we will just have this equality as the root
                    if (this->root->children.size() == 2) {
                        for (auto&& c : this->root->children) {
                            if (c->value == "=") {
                                join_out = detail::rebuild_helper(c.get());
                            } else {
                                filter_out = detail::rebuild_helper(c.get());
                            }
                        }
                    } else {
                        auto filter_root = std::make_unique<operator_node>("AND");
                        for (auto&& c : this->root->children) {
                            if (c->value == "=") {
                                join_out = detail::rebuild_helper(c.get());
                            } else {
                                filter_root->children.push_back(std::unique_ptr<parse_node>(c.release()));
                            }
                        }
                        filter_out = detail::rebuild_helper(filter_root.get());
                    }
                } else if (num_equalities == this->root->children.size() - 1) {
                    // only one that does not have an inequality and therefore will be
                    // in the filter (without an and at the root)
                    auto join_out_root = std::make_unique<operator_node>("AND");
                    for (auto&& c : this->root->children) {
                        if (c->value == "=") {
                            join_out_root->children.push_back(std::unique_ptr<parse_node>(c.release()));
                        } else {
                            filter_out = detail::rebuild_helper(c.get());
                        }
                    }
                    join_out = detail::rebuild_helper(join_out_root.get());
                } else {
                    auto join_out_root = std::make_unique<operator_node>("AND");
                    auto filter_root = std::make_unique<operator_node>("AND");
                    for (auto&& c : this->root->children) {
                        if (c->value == "=") {
                            join_out_root->children.push_back(std::unique_ptr<parse_node>(c.release()));
                        } else {
                            filter_root->children.push_back(std::unique_ptr<parse_node>(c.release()));
                        }
                    }
                    join_out = detail::rebuild_helper(join_out_root.get());
                    filter_out = detail::rebuild_helper(filter_root.get());
                }
            } else {  // this is not supported. Throw error
                std::string original_join_condition = this->rebuildExpression();
                throw std::runtime_error(
                        "Join condition is currently not supported. Join received: " + original_join_condition);
            }
        } else {  // this is not supported. Throw error
            std::string original_join_condition = this->rebuildExpression();
            throw std::runtime_error(
                    "Join condition is currently not supported. Join received: " + original_join_condition);
        }
    }

    bool is_valid() {
        return this->root->value.length() > 0;
    }

    std::string rebuildExpression() {
        assert(!!this->root);
        return detail::rebuild_helper(this->root.get());
    }

    std::string prefix() {
        assert(!!this->root);
        return detail::tokenizer_helper(this->root.get());
    }
};

}  // namespace parser
}  // namespace ral
