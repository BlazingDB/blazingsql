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
#include "utilities/CommonOperations.h"
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
    enum placement_type {
      AUTO,
      BEGIN,
      MIDDLE,
      END
    };

    operator_node(const std::string& value) : node{ node_type::OPERATOR, value }, label(value) {};

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

    placement_type placement = placement_type::AUTO;
    std::string label;
    bool parentheses_wrap = true;
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

        return node->value + (!operands.empty() ? (" " + operands) : "");
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
        } else if(StringUtil::beginsWith(node.value, "CAST")) {
            return transform_cast_literal(node);
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

    node * transform_cast_literal(operator_node& cast_node) {
        assert(cast_node.children.size() == 1);

        operator_type cast_op = map_to_operator_type(cast_node.value);

        auto operand = cast_node.children[0].get();
        if (operand->type == node_type::LITERAL) {
            // Special case for calcite expressions like `CAST(4:INTEGER):INTEGER`

            auto literal = static_cast<literal_node *>(operand);
            cudf::data_type new_type(get_output_type(cast_op, literal->type().id()));

            // Ensure that the types are compatible
            ral::utilities::get_common_type(literal->type(), new_type, true);

            return new literal_node(literal->value, new_type);
        }

        return &cast_node;
    }
};

class lexer
{
public:
    constexpr static char VARIABLE_REGEX_STR[] = R"(\$\d+)";
    constexpr static char NULL_REGEX_STR[] = R"(null)";
    constexpr static char BOOLEAN_REGEX_STR[] = R"(true|false)";
    constexpr static char NUMBER_REGEX_STR[] = R"([-+]?\d*\.?\d+([eE][-+]?\d+)?)";
    constexpr static char TIMESTAMP_D_REGEX_STR[] = R"(\d{4}-\d{2}-\d{2})";
    constexpr static char TIMESTAMP_S_REGEX_STR[] = R"(\d{4}-\d{2}-\d{2}(?:[ T]?\d{2}:\d{2}:\d{2}))";
    constexpr static char TIMESTAMP_MS_REGEX_STR[] = R"(\d{4}-\d{2}-\d{2}(?:[ T]?\d{2}:\d{2}:\d{2}.\d{3}))";
    constexpr static char TIMESTAMP_US_REGEX_STR[] = R"(\d{4}-\d{2}-\d{2}(?:[ T]?\d{2}:\d{2}:\d{2}.\d{6}))";
    constexpr static char TIMESTAMP_NS_REGEX_STR[] = R"(\d{4}-\d{2}-\d{2}(?:[ T]?\d{2}:\d{2}:\d{2}.\d{9}))";
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
        Timestamp_d,
        Timestamp_s,
        Timestamp_ms,
        Timestamp_us,
        Timestamp_ns,
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
    std::regex timestamp_d_regex{"^" + std::string(lexer::TIMESTAMP_D_REGEX_STR)};
    std::regex timestamp_s_regex{"^" + std::string(lexer::TIMESTAMP_S_REGEX_STR)};
    std::regex timestamp_ms_regex{"^" + std::string(lexer::TIMESTAMP_MS_REGEX_STR)};
    std::regex timestamp_us_regex{"^" + std::string(lexer::TIMESTAMP_US_REGEX_STR)};
    std::regex timestamp_ns_regex{"^" + std::string(lexer::TIMESTAMP_US_REGEX_STR)};
    std::regex string_regex{"^" + std::string(lexer::STRING_REGEX_STR)};
};

cudf::data_type infer_type_from_literal_token(const lexer::token & token);

cudf::data_type type_from_type_token(const lexer::token & token);

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
