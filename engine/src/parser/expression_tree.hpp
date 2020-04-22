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
    virtual std::shared_ptr<parse_node> transform(const operad_node& node) = 0;
    virtual std::shared_ptr<parse_node> transform(const operator_node& node) = 0;
};


struct parse_node {
    parse_node_type type;
    std::vector<std::shared_ptr<parse_node>> children;
    std::string value;

    size_t n_nodes() {
        return children.size();
    }
    std::shared_ptr<parse_node> left() {
        return children.at(0);
    }
    std::shared_ptr<parse_node> right() {
        return children.at(1);
    }

    void set_left(std::shared_ptr<parse_node> child) {
        if (children.size() == 0) {
            children.push_back(child);
        }
        else {
            children[0] = child;
        }
    }
    void set_right(std::shared_ptr<parse_node> child) {
        if (children.size() == 1) {
            children.push_back(child);
        }
        else {
            assert(children.size() > 2);
            children[1] = child;
        }
    }

    parse_node(parse_node_type type, const std::string& value) : type{ type }, value{ value } {};

    virtual void accept(parse_node_visitor&) = 0;

    virtual std::shared_ptr<parse_node> accept(parse_node_transformer&) = 0;

    virtual std::shared_ptr<parse_node> transform_to_custom_op() = 0;

    virtual std::shared_ptr<parse_node> reduce() = 0;

    virtual std::shared_ptr<parse_node> transform(int inc) = 0;
};


struct operad_node : parse_node, std::enable_shared_from_this<operad_node> {
    operad_node(const std::string& value) : parse_node{ parse_node_type::OPERAND, value } {};

    void accept(parse_node_visitor& visitor) override { visitor.visit(*this); }

    std::shared_ptr<parse_node> accept(parse_node_transformer& transformer) override { return transformer.transform(*this); }

    std::shared_ptr<parse_node> transform_to_custom_op() override { return shared_from_this(); }

    std::shared_ptr<parse_node> reduce()  override {
        return shared_from_this();
    }
    virtual std::shared_ptr<parse_node> transform(int inc) override {
        std::string new_expr = this->value;
        if (is_var_column(new_expr) ) {
            auto id = ral::skip_data::get_id(new_expr);
            new_expr = "$" + std::to_string(2 * id + inc);
        }
        auto ptr = std::make_shared<operad_node>(new_expr);
        ptr->children = this->children;
        return ptr;
    }
};


struct operator_node : parse_node, std::enable_shared_from_this<operator_node> {
    operator_node(const std::string& value) : parse_node{ parse_node_type::OPERATOR, value } {};



    std::shared_ptr<parse_node> reduce_helper(std::shared_ptr<parse_node> &n, std::shared_ptr<parse_node> &m) {
        auto ptr = std::make_shared<operator_node>(this->value);
        ptr->children.push_back(n);
        ptr->children.push_back(m);

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
            if (this->value == "AND") {
                return m;
            }
            else {
                return std::make_shared<operator_node>("NONE");
            }
        }
        else if (right_is_exclusion_unary_op and not left_is_exclusion_unary_op) {
            if (this->value == "AND") {
                return n;
            }
            else {
                return std::make_shared<operator_node>("NONE");
            }
        }
        else if (left_is_exclusion_unary_op and right_is_exclusion_unary_op) {
            return std::make_shared <operator_node>("NONE");
        }
        return ptr;
    }

    std::shared_ptr<parse_node> reduce()  override {
        if (ral::skip_data::is_unsupported_binary_op(this->value)) {
            return std::make_shared<operator_node>("NONE");
        }
        if (this->children.size() == 2) {
            auto n = this->children[0]->reduce();
            auto m = this->children[1]->reduce();
            return reduce_helper(n, m);
        }
    }

    virtual std::shared_ptr<parse_node> transform(int inc) override {
        if (this->value == "&&&") {
            if (inc == 0) // min
                return std::shared_ptr<parse_node>(this->left());
            return std::shared_ptr<parse_node>(this->right());
        }else {
            auto ptr = std::make_shared<operad_node>(this->value);
            ptr->children = this->children;
            return ptr;
        }
    }

    void accept(parse_node_visitor& visitor) override {
        for (auto&& c : this->children) {
            c->accept(visitor);
        }
        visitor.visit(*this);
    }

    std::shared_ptr<parse_node> accept(parse_node_transformer& transformer) override {
        for (auto&& c : this->children) {
            c = c->accept(transformer);
        }
        return transformer.transform(*this);
    }

    std::shared_ptr<parse_node> transform_to_custom_op() override {
        for(size_t index = 0; index < this->children.size(); index++) {
            this->children[index] = this->children[index]->transform_to_custom_op();
        }
        if(this->value == "CASE") {
            return transform_case(0);
        }
        else if(this->value == "CAST") {
            return transform_cast();
        } else if(this->value == "SUBSTRING") {
            return transform_substring();
        } else if(this->value == "Reinterpret") {
            return remove_reinterpret();
        } else if(this->value == "ROUND") {
            return transform_round();
        }
        return shared_from_this();
    }

private:
    std::shared_ptr<parse_node>  transform_case(size_t child_idx) {
        assert(this->children.size() >= 3 && this->children.size() % 2 != 0);
        assert(child_idx < this->children.size());

        if (child_idx == this->children.size() - 1) {
            return this->children[child_idx];
        }

        auto condition = this->children[child_idx];
        auto then = this->children[child_idx + 1];

        auto magic_if_not = std::make_shared<operator_node>( "MAGIC_IF_NOT" );
        magic_if_not->children.push_back(condition);
        magic_if_not->children.push_back(then);

        auto first_non_magic = std::make_shared<operator_node>("FIRST_NON_MAGIC");
        first_non_magic->children.push_back(magic_if_not);
        first_non_magic->children.push_back(transform_case(child_idx + 2));

        return first_non_magic;
    }

    std::shared_ptr<parse_node> transform_cast() {
        assert(this->children.size() == 2);

        auto exp = this->children[0];
        std::string target_type = this->children[1]->value;

        auto cast_op = std::make_shared<operator_node>("CAST_" + target_type);
        cast_op->children.push_back(exp);
        return cast_op;
    }

    std::shared_ptr<parse_node>  transform_substring() {
        assert(this->children.size() == 2 || this->children.size() == 3);

        auto target = this->children[0];
        std::string start_end_str = "'" + this->children[1]->value;
        if (this->children.size() == 3) {
            start_end_str += ":" + this->children[2]->value;
        }
        start_end_str += "'";
        auto start_end_params = std::make_shared<operad_node>( start_end_str );

        auto substring_op = std::make_shared<operator_node>(this->value);
        substring_op->children.push_back(target);
        substring_op->children.push_back(start_end_params);

        return substring_op;
    }

    std::shared_ptr<parse_node> remove_reinterpret() {
        assert(this->children.size() == 1);
        return this->children[0];
    }

    std::shared_ptr<parse_node> transform_round() {
        assert(this->children.size() == 1 || this->children.size() == 2);

        if (this->children.size() == 1) {
            auto second_arg = std::make_shared<operad_node>( "0" );
            this->children.push_back(second_arg);
        }

        return shared_from_this();
    }
};

namespace detail {
    inline void print_helper(const parse_node* node, size_t depth) {
        if (!node)
            return;
        if (node->children.size() > 1)
            print_helper(node->children[1].get(), depth + 1);

        for (int k = 0; k < depth; k++) {
            std::cout << "    ";
        }
        std::cout << node->value << "\n";
        if (node->children.size() > 0)
            print_helper(node->children[0].get(), depth + 1);
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
}

struct parse_tree {
    std::shared_ptr<parse_node> root;

private:
    size_t build_helper(parse_node* parent_node, const std::string& expression, size_t pos) {
        bool return_type = false;
        while (pos != std::string::npos) {
            size_t nextPos = expression.find_first_of("(),", pos);
            std::string token = expression.substr(pos, nextPos - pos);
            token = StringUtil::ltrim(token);

            std::shared_ptr<parse_node> new_node = nullptr;
            if (!token.empty()) {
                if (is_operator_token(token)) {
                    new_node = std::make_shared<operator_node>(token);
                }
                else {
                    new_node = std::make_shared <operad_node>(token);
                }

                if (!parent_node) {
                    this->root = new_node;
                }
                else {
                    parent_node->children.push_back(new_node);
                }
            }

            if (nextPos == std::string::npos) {
                return nextPos;
            }
            else if (expression[nextPos] == ')') {
                if (nextPos + 1 < expression.size() && expression[nextPos + 1] == ':') {
                    return_type = true;
                    pos = nextPos + 2;
                    break;
                }
                else {
                    return nextPos + 1;
                }
            }
            else if (expression[nextPos] == '(') {
                assert(new_node != nullptr);
                pos = build_helper(new_node.get(), expression, nextPos + 1);
            }
            else {  // expression[pos] == ','
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

            auto new_node = std::make_shared<operad_node>(token);
            parent_node->children.push_back(new_node);

            // Don't advance position so that the parent can process it
            return nextPos;
        }
        else {
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
        this->root = this->root->accept(transformer);
    }

    void transform_to_custom_op() {
        assert(!!this->root);
        this->root = this->root->transform_to_custom_op();
    }

    void get_prefix_nodes(std::shared_ptr<parse_node>& p, std::stack< std::shared_ptr<parse_node>*>& nodes) {
        if (p) {
            nodes.push(&p);
            for (size_t index = 0; index < p->children.size(); index++) {
                get_prefix_nodes(p->children[index], nodes);
            }
        }
    }

    std::shared_ptr<parse_node> make_and(std::shared_ptr<parse_node> &n, std::shared_ptr<parse_node> &m) {
        auto ptr = std::make_shared<operator_node>("AND");
        ptr->set_left(n);
        ptr->set_right(m);
        return ptr;
    }
    std::shared_ptr<parse_node> make_greater_eq(std::shared_ptr<parse_node> &n,
                                                std::shared_ptr<parse_node> &m, int inc_n,
                                                int inc_m) {
        auto ptr = std::make_shared<operator_node>(">=");
        ptr->set_left(n->transform(inc_n));
        ptr->set_right(m->transform(inc_m));
        return ptr;
    }
    std::shared_ptr<parse_node> make_less_eq(std::shared_ptr<parse_node> &n, std::shared_ptr<parse_node> &m,
                                             int inc_n, int inc_m) {
        auto ptr = std::make_shared<operator_node>("<=");
        ptr->set_left(n->transform(inc_n));
        ptr->set_right(m->transform(inc_m));
        return ptr;
    }

    std::shared_ptr<parse_node> make_pair_node(std::shared_ptr<parse_node> &n,
                                               std::shared_ptr<parse_node> &m) {
        auto ptr = std::make_shared<operator_node>("&&&"); // this is parent node (), for sum, sub after skip_data
        ptr->set_left(n);
        ptr->set_right(m);
        return ptr;
    }
    std::shared_ptr<parse_node> make_sum(std::shared_ptr<parse_node> &n, std::shared_ptr<parse_node> &m,
                                         int inc_n, int inc_m) {
        auto ptr = std::make_shared<operator_node>("+");
        ptr->set_left(n->transform(inc_n));
        ptr->set_right(m->transform(inc_m));
        return ptr;
    }
    std::shared_ptr<parse_node> make_sub(std::shared_ptr<parse_node> &n, std::shared_ptr<parse_node> &m,
                                         int inc_n, int inc_m) {
        auto ptr = std::make_shared<operator_node>("-");
        ptr->set_left(n->transform(inc_n));
        ptr->set_right(m->transform(inc_m));
        return ptr;
    }
    std::shared_ptr<parse_node> transform_less_or_lesseq(std::shared_ptr<parse_node> &subtree,
                                                         std::string op) {
        auto n = subtree->left();
        auto m = subtree->right();
        auto ptr = std::make_shared<operator_node>(op);
        ptr->set_left(n->transform(0));
        ptr->set_right(m->transform(1));
        return ptr;
    }

    std::shared_ptr<parse_node> transform_greater_or_greatereq(std::shared_ptr<parse_node> &subtree,
                                                               std::string op) {
        auto n = subtree->left();
        auto m = subtree->right();
        auto ptr = std::make_shared<operator_node>(op);
        ptr->set_left(n->transform(1));
        ptr->set_right(m->transform(0));
        return ptr;
    }
    std::shared_ptr<parse_node> transform_equal(std::shared_ptr<parse_node> &subtree) {
        auto n = subtree->left();
        auto m = subtree->right();
        auto less_eq = make_less_eq(n, m, 0, 1);
        auto greater_eq = make_greater_eq(n, m, 1, 0);
        return make_and(less_eq, greater_eq);
    }

    std::shared_ptr<parse_node> transform_make_sum(std::shared_ptr<parse_node> &subtree) {
        auto n = subtree->left();
        auto m = subtree->right();
        auto first = make_sum(n, m, 0, 0);
        auto second = make_sum(n, m, 1, 1);
        return make_pair_node(first, second);
    }

    std::shared_ptr<parse_node> transform_make_sub(std::shared_ptr<parse_node> &subtree) {
        auto n = subtree->left();
        auto m = subtree->right();
        auto expr1 = make_sub(n, m, 0, 0);
        auto expr2 = make_sub(n, m, 1, 1);
        return make_pair_node(expr1, expr2);
    }
    void apply_skip_data_rules_helper(std::stack<std::shared_ptr<parse_node>*>& nodes) {
        while (not nodes.empty()) {
            auto& p = *nodes.top();
            if (p->children.size() == 2) {
                auto op = p->value;
                 if (op == "=") {
                 	p = transform_equal(p);
                 } else if (op == "<" or op == "<=") {
                 	p = transform_less_or_lesseq(p, op);
                 } else if (op == ">" or op == ">=") {
                 	p = transform_greater_or_greatereq(p, op);
                 } else if (op == "+") {
                 	p = transform_make_sum(p);
                 } else if (op == "-") {
                 	p = transform_make_sub(p);
                 } else {
                 	// just skip like AND or OR
                 }
            }
            nodes.pop();
        }
    }

    void apply_skip_data_rules() {
        assert(!!this->root);
        this->root = this->root->reduce();
        if (root->value == "NONE") {
            root->value = "";
            return;
        }
        std::stack< std::shared_ptr<parse_node> *> nodes;
        get_prefix_nodes(this->root, nodes);
        apply_skip_data_rules_helper(nodes);
    }

    template <typename predicate>
    void drop_helper(std::shared_ptr<parse_node> &p, predicate pred) {
        if (p == nullptr) {
            return;
        } else {
            if (pred(p.get())) {
                p = std::make_shared<operator_node>("NONE");
            }
            if (p->children.size() > 0)
                drop_helper(p->children[0], pred);
            if (p->children.size() > 1)
                drop_helper(p->children[1], pred);
        }
    }

    void drop(std::vector<std::string> const &column_names) {
        for (auto &col_name : column_names) {
            drop_helper(this->root, [&col_name](parse_node *p) {
                return p->value == col_name;
            });
        }
    }
    void split_inequality_join_into_join_and_filter(std::string& join_out, std::string& filter_out) {
        assert(!!this->root);
        assert(this->root.get()->type == parse_node_type::OPERATOR);

        if (this->root.get()->value == "=") {
            // this would be a regular single equality join
            join_out = this->rebuildExpression();  // the join_out is the same as the original input
            filter_out = "";					   // no filter out
        }
        else if (this->root.get()->value == "AND") {
            int num_equalities = 0;
            for (auto&& c : this->root.get()->children) {
                if (c.get()->value == "=") {
                    num_equalities++;
                }
            }
            if (num_equalities ==
                this->root.get()
                        ->children.size()) {  // all are equalities. this would be a regular multiple equality join
                join_out = this->rebuildExpression();  // the join_out is the same as the original input
                filter_out = "";					   // no filter out
            }
            else if (num_equalities > 0) {			   // i can split this into an equality join and a filter
                if (num_equalities == 1) {  // if there is only one equality, then the root for join_out wont be an AND,
                    // and we will just have this equality as the root
                    if (this->root.get()->children.size() == 2) {
                        for (auto&& c : this->root.get()->children) {
                            if (c.get()->value == "=") {
                                join_out = detail::rebuild_helper(c.get());
                            }
                            else {
                                filter_out = detail::rebuild_helper(c.get());
                            }
                        }
                    }
                    else {
                        auto filter_root =  std::make_shared<operator_node>( "AND" );
                        for (auto&& c : this->root.get()->children) {
                            if (c.get()->value == "=") {
                                join_out = detail::rebuild_helper(c.get());
                            }
                            else {
                                filter_root->children.push_back(c);
                            }
                        }
                        filter_out = detail::rebuild_helper(filter_root.get());
                    }
                }
                else if (num_equalities == this->root.get()->children.size() -
                                           1) {  // only one that does not have an inequality and therefore will be
                    // in the filter (without an and at the root)
                    auto join_out_root = std::make_shared<operator_node>("AND");
                    for (auto&& c : this->root.get()->children) {
                        if (c.get()->value == "=") {
                            join_out_root->children.push_back(c);
                        }
                        else {
                            filter_out = detail::rebuild_helper(c.get());
                        }
                    }
                    join_out = detail::rebuild_helper(join_out_root.get());
                }
                else {
                    auto join_out_root = std::make_shared<operator_node> ( "AND" );
                    auto filter_root = std::make_shared<operator_node> ( "AND" );
                    for (auto&& c : this->root.get()->children) {
                        if (c.get()->value == "=") {
                            join_out_root->children.push_back(std::shared_ptr<parse_node>(c.get()));
                        }
                        else {
                            filter_root->children.push_back(std::shared_ptr<parse_node>(c.get()));
                        }
                    }
                    join_out = detail::rebuild_helper(join_out_root.get());
                    filter_out = detail::rebuild_helper(filter_root.get());
                }
            }
            else {  // this is not supported. Throw error
                std::string original_join_condition = this->rebuildExpression();
                throw std::runtime_error(
                        "Join condition is currently not supported. Join received: " + original_join_condition);
            }
        }
        else {  // this is not supported. Throw error
            std::string original_join_condition = this->rebuildExpression();
            throw std::runtime_error(
                    "Join condition is currently not supported. Join received: " + original_join_condition);
        }
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
