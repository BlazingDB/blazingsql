#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <cudf/column/column_view.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>
#include <cudf/filling.hpp>
#include <cudf/strings/combine.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/substring.hpp>
#include <cudf/strings/convert/convert_booleans.hpp>
#include <cudf/strings/convert/convert_datetime.hpp>
#include <cudf/strings/convert/convert_floats.hpp>
#include <cudf/strings/convert/convert_integers.hpp>
#include <cudf/unary.hpp>
#include <regex>
#include <algorithm>

#include "LogicalProject.h"
#include "CalciteExpressionParsing.h"
#include "parser/expression_tree.hpp"
#include "error.hpp"
#include "utilities/CommonOperations.h"
#include "utilities/transform.hpp"
#include "Interpreter/interpreter_cpp.h"

namespace ral {
namespace processor {

// forward declaration
std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(const cudf::table_view & table, const std::vector<std::string> & expressions);

namespace strings {

std::string like_expression_to_regex_str(const std::string & like_exp) {
	if(like_exp.empty()) {
		return like_exp;
	}

	bool match_start = like_exp[0] != '%';
	bool match_end = like_exp[like_exp.size() - 1] != '%';

	std::string re = like_exp;
	static const std::regex any_string_re{R"(([^\\]?|\\{2})%)"};
	re = std::regex_replace(re, any_string_re, "$1(?:.*?)");

	static const std::regex any_char_re{R"(([^\\]?|\\{2})_)"};
	re = std::regex_replace(re, any_char_re, "$1(?:.)");

	return (match_start ? "^" : "") + re + (match_end ? "$" : "");
}

struct cast_to_str_functor {
    template<typename T, std::enable_if_t<cudf::is_boolean<T>()> * = nullptr>
    std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
        return cudf::strings::from_booleans(col);
    }

    template<typename T, std::enable_if_t<std::is_integral<T>::value && !cudf::is_boolean<T>()> * = nullptr>
    std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
        return cudf::strings::from_integers(col);
    }

    template<typename T, std::enable_if_t<std::is_floating_point<T>::value> * = nullptr>
    std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
        return cudf::strings::from_floats(col);
    }

    template<typename T, std::enable_if_t<cudf::is_timestamp<T>()> * = nullptr>
    std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
        return cudf::strings::from_timestamps(col, std::is_same<cudf::timestamp_D, T>::value ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S");
    }

    template<typename T, std::enable_if_t<cudf::is_compound<T>() or cudf::is_duration<T>()> * = nullptr>
    std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
        return nullptr;
    }
};

std::unique_ptr<cudf::column> evaluate_string_functions(const cudf::table_view & table,
                                                        operator_type op,
                                                        const std::vector<std::string> & arg_tokens)
{
    std::unique_ptr<cudf::column> computed_col;

    switch (op)
    {
    case operator_type::BLZ_STR_LIKE:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "LIKE operator not supported for string literals");

        std::unique_ptr<cudf::column> computed_column;
        cudf::column_view column;
        if (is_var_column(arg_tokens[0])) {
            column = table.column(get_index(arg_tokens[0]));
        } else {
            auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
            assert(evaluated_col.size() == 1);
            computed_column = evaluated_col[0]->release();
            column = computed_column->view();
        }

        std::string regex = like_expression_to_regex_str(arg_tokens[1].substr(1, arg_tokens[1].size() - 2));

        computed_col = cudf::strings::contains_re(column, regex);
        break;
    }
    case operator_type::BLZ_STR_SUBSTRING:
    {
        assert(arg_tokens.size() == 2 || arg_tokens.size() == 3);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "SUBSTRING function not supported for string literals");

        if (is_var_column(arg_tokens[0]) && is_literal(arg_tokens[1]) && (arg_tokens.size() == 3 ? is_literal(arg_tokens[2]) : true)) {
            cudf::column_view column = table.column(get_index(arg_tokens[0]));
            int32_t start = std::max(std::stoi(arg_tokens[1]), 1) - 1;
            int32_t length = arg_tokens.size() == 3 ? std::stoi(arg_tokens[2]) : -1;
            int32_t end = length >= 0 ? start + length : 0;

            computed_col = cudf::strings::slice_strings(column, start, cudf::numeric_scalar<int32_t>(end, length >= 0));            
        } else {
            // TODO: create a version of cudf::strings::slice_strings that uses start and length columns
            // so we can remove all the calculations for start and end

            std::unique_ptr<cudf::column> computed_string_column;
            cudf::column_view column;
            if (is_var_column(arg_tokens[0])) {
                column = table.column(get_index(arg_tokens[0]));                
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
                RAL_EXPECTS(evaluated_col.size() == 1 && evaluated_col[0]->view().type().id() == cudf::type_id::STRING, "Expression does not evaluate to a string column");

                computed_string_column = evaluated_col[0]->release();
                column = computed_string_column->view();                
            }

            std::unique_ptr<cudf::column> computed_start_column;
            cudf::column_view start_column;
            if (is_var_column(arg_tokens[1])) {
                computed_start_column = std::make_unique<cudf::column>(table.column(get_index(arg_tokens[1])));
            } else if(is_literal(arg_tokens[1])) {
                int32_t start = std::max(std::stoi(arg_tokens[1]), 1);

                cudf::numeric_scalar<int32_t> start_scalar(start);
                computed_start_column = cudf::make_column_from_scalar(start_scalar, table.num_rows());
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[1]});
                RAL_EXPECTS(evaluated_col.size() == 1 && is_type_integer(evaluated_col[0]->view().type().id()), "Expression does not evaluate to an integer column");

                computed_start_column = evaluated_col[0]->release();
            }
            cudf::mutable_column_view mutable_view = computed_start_column->mutable_view();
            ral::utilities::transform_start_to_zero_based_indexing(mutable_view);
            start_column = computed_start_column->view();

            std::unique_ptr<cudf::column> computed_end_column;
            cudf::column_view end_column;
            if (arg_tokens.size() == 3) {
                if (is_var_column(arg_tokens[2])) {
                    computed_end_column = std::make_unique<cudf::column>(table.column(get_index(arg_tokens[2])));
                } else if(is_literal(arg_tokens[2])) {
                    std::unique_ptr<cudf::scalar> end_scalar = get_scalar_from_string(arg_tokens[2], start_column.type());
                    computed_end_column = cudf::make_column_from_scalar(*end_scalar, table.num_rows());
                } else {
                    auto evaluated_col = evaluate_expressions(table, {arg_tokens[2]});
                    RAL_EXPECTS(evaluated_col.size() == 1 && is_type_integer(evaluated_col[0]->view().type().id()), "Expression does not evaluate to an integer column");

                    computed_end_column = evaluated_col[0]->release();
                }

                // lets make sure that the start and end are the same type
                if (!(start_column.type() == computed_end_column->type())){
                    cudf::data_type common_type = ral::utilities::get_common_type(start_column.type(), computed_end_column->type(), true);
                    if (!(start_column.type() == common_type)){
                        computed_start_column = cudf::cast(start_column, common_type);
                        start_column = computed_start_column->view();
                    }
                    if (!(computed_end_column->type() == common_type)){
                        computed_end_column = cudf::cast(computed_end_column->view(), common_type);
                    }
                }
                cudf::mutable_column_view mutable_view = computed_end_column->mutable_view();
                ral::utilities::transform_length_to_end(mutable_view, start_column);
                end_column = computed_end_column->view();
            } else {
                std::unique_ptr<cudf::scalar> end_scalar = get_max_integer_scalar(start_column.type());
                computed_end_column = cudf::make_column_from_scalar(*end_scalar, table.num_rows());
                end_column = computed_end_column->view();
            }
            
            computed_col = cudf::strings::slice_strings(column, start_column, end_column);
        }
        break;
    }
    case operator_type::BLZ_STR_CONCAT:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(!(is_string(arg_tokens[0]) && is_string(arg_tokens[1])), "CONCAT operator between literals is not supported");

        if (is_var_column(arg_tokens[0]) && is_var_column(arg_tokens[1])) {
            cudf::column_view column1 = table.column(get_index(arg_tokens[0]));
            cudf::column_view column2 = table.column(get_index(arg_tokens[1]));

            computed_col = cudf::strings::concatenate(cudf::table_view{{column1, column2}});
        } else {
            std::unique_ptr<cudf::column> temp_col1;
            cudf::column_view column1;
            if (is_var_column(arg_tokens[0])) {
                column1 = table.column(get_index(arg_tokens[0]));
            } else if(is_literal(arg_tokens[0])) {
                std::string literal_str = arg_tokens[0].substr(1, arg_tokens[0].size() - 2);
                cudf::string_scalar str_scalar(literal_str);
                temp_col1 = cudf::make_column_from_scalar(str_scalar, table.num_rows());
                column1 = temp_col1->view();
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
                assert(evaluated_col.size() == 1);
                temp_col1 = evaluated_col[0]->release();
                column1 = temp_col1->view();
            }

            std::unique_ptr<cudf::column> temp_col2;
            cudf::column_view column2;
            if (is_var_column(arg_tokens[1])) {
                column2 = table.column(get_index(arg_tokens[1]));
            } else if(is_literal(arg_tokens[1])) {
                std::string literal_str = arg_tokens[1].substr(1, arg_tokens[1].size() - 2);
                cudf::string_scalar str_scalar(literal_str);
                temp_col2 = cudf::make_column_from_scalar(str_scalar, table.num_rows());
                column2 = temp_col2->view();
            } else {
                auto evaluated_col = evaluate_expressions(table, {arg_tokens[1]});
                assert(evaluated_col.size() == 1);
                temp_col2 = evaluated_col[0]->release();
                column2 = temp_col2->view();
            }

            computed_col = cudf::strings::concatenate(cudf::table_view{{column1, column2}});
        }
        break;
    }
    case operator_type::BLZ_CAST_VARCHAR:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(!is_literal(arg_tokens[0]), "CAST operator not supported for literals");

        std::unique_ptr<cudf::column> computed_column;
        cudf::column_view column;
        if (is_var_column(arg_tokens[0])) {
            column = table.column(get_index(arg_tokens[0]));
        } else {
            auto evaluated_col = evaluate_expressions(table, {arg_tokens[0]});
            assert(evaluated_col.size() == 1);
            computed_column = evaluated_col[0]->release();
            column = computed_column->view();
        }
        if (column.type() == cudf::data_type{cudf::type_id::STRING}){
            // this should not happen, but sometimes calcite produces inefficient plans that ask to cast a string column to a "VARCHAR NOT NULL"
            computed_col = std::make_unique<cudf::column>(column);
        } else {
            computed_col = cudf::type_dispatcher(column.type(), cast_to_str_functor{}, column);
        }        
        break;
    }
    case operator_type::BLZ_CAST_TINYINT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT8});
        }
        break;
    }
    case operator_type::BLZ_CAST_SMALLINT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT16});
        }
        break;
    }
    case operator_type::BLZ_CAST_INTEGER:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT32});
        }
        break;
    }
    case operator_type::BLZ_CAST_BIGINT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT64});
        }
        break;
    }
    case operator_type::BLZ_CAST_FLOAT:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_floats(column, cudf::data_type{cudf::type_id::FLOAT32});
        }
        break;
    }
    case operator_type::BLZ_CAST_DOUBLE:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_floats(column, cudf::data_type{cudf::type_id::FLOAT64});
        }
        break;
    }
    case operator_type::BLZ_CAST_DATE:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, "%Y-%m-%d");
        }
        break;
    }
    case operator_type::BLZ_CAST_TIMESTAMP:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            // Will be handled by interops
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS}, "%Y-%m-%d %H:%M:%S");
        }
        break;
    }
    }

    return computed_col;
}

std::unique_ptr<cudf::column> evaluate_string_case_when_else(const cudf::table_view & table,
                                                            const std::string & condition_expr,
                                                            const std::string & expr1,
                                                            const std::string & expr2)
{
    if ((!is_string(expr1) && !is_var_column(expr1)) || (!is_string(expr2) && !is_var_column(expr2))) {
        return nullptr;
    }

    if ((is_var_column(expr1) && table.column(get_index(expr1)).type().id() != cudf::type_id::STRING)
        || (is_var_column(expr2) && table.column(get_index(expr2)).type().id() != cudf::type_id::STRING)) {
        return nullptr;
    }

    RAL_EXPECTS(!is_literal(condition_expr), "CASE operator not supported for condition expression literals");

    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table;
    cudf::column_view boolean_mask_view;
    if (is_var_column(condition_expr)) {
        boolean_mask_view = table.column(get_index(condition_expr));
    } else {
        evaluated_table = evaluate_expressions(table, {condition_expr});
        RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

        boolean_mask_view = evaluated_table[0]->view();
    }

    std::unique_ptr<cudf::column> computed_col;
    if (is_string(expr1) && is_string(expr2)) {
        std::unique_ptr<cudf::scalar> lhs = get_scalar_from_string(expr1, cudf::data_type{cudf::type_id::STRING});
        std::unique_ptr<cudf::scalar> rhs = get_scalar_from_string(expr2, cudf::data_type{cudf::type_id::STRING});
        computed_col = cudf::copy_if_else(*lhs, *rhs, boolean_mask_view);
    } else if (is_string(expr1)) {
        std::unique_ptr<cudf::scalar> lhs = get_scalar_from_string(expr1, cudf::data_type{cudf::type_id::STRING});
        cudf::column_view rhs = table.column(get_index(expr2));
        computed_col = cudf::copy_if_else(*lhs, rhs, boolean_mask_view);
    } else if (is_string(expr2)) {
        cudf::column_view lhs = table.column(get_index(expr1));
        std::unique_ptr<cudf::scalar> rhs = get_scalar_from_string(expr2, cudf::data_type{cudf::type_id::STRING});
        computed_col = cudf::copy_if_else(lhs, *rhs, boolean_mask_view);
    } else {
        cudf::column_view lhs = table.column(get_index(expr1));
        cudf::column_view rhs = table.column(get_index(expr2));
        computed_col = cudf::copy_if_else(lhs, rhs, boolean_mask_view);
    }

    return computed_col;
}

} // namespace strings

class function_evaluator_transformer : public parser::node_transformer {
public:
    function_evaluator_transformer(const cudf::table_view & table) : table{table} {}

    parser::node * transform(parser::operad_node& node) override { return &node; }

    parser::node * transform(parser::operator_node& node) override {
        operator_type op = map_to_operator_type(node.value);

        std::unique_ptr<cudf::column> computed_col;
        std::vector<std::string> arg_tokens;
        if (op == operator_type::BLZ_FIRST_NON_MAGIC) {
            // special case for CASE WHEN ELSE END for strings
            assert(node.children[0]->type == parser::node_type::OPERATOR);
            assert(map_to_operator_type(node.children[0]->value) == operator_type::BLZ_MAGIC_IF_NOT);

            const parser::node * magic_if_not_node = node.children[0].get();
            const parser::node * condition_node = magic_if_not_node->children[0].get();
            const parser::node * expr_node_1 = magic_if_not_node->children[1].get();
            const parser::node * expr_node_2 = node.children[1].get();

            std::string conditional_exp = parser::detail::rebuild_helper(condition_node);

            arg_tokens = {conditional_exp, expr_node_1->value, expr_node_2->value};
            computed_col = strings::evaluate_string_case_when_else(cudf::table_view{{table, computed_columns_view()}}, conditional_exp, expr_node_1->value, expr_node_2->value);
        } else {
            arg_tokens.reserve(node.children.size());
            for (auto &&c : node.children) {
                arg_tokens.push_back(parser::detail::rebuild_helper(c.get()));
            }

            computed_col = strings::evaluate_string_functions(cudf::table_view{{table, computed_columns_view()}}, op, arg_tokens);
        }

        if (computed_col) {
            // Discard temp columns used in operations
            for (auto &&token : arg_tokens) {
                if (!is_var_column(token)) continue;

                cudf::size_type idx = get_index(token);
                if (idx >= table.num_columns()) {
                    computed_columns.erase(computed_columns.begin() + (idx - table.num_columns()));
                }
            }

            std::string computed_var_token = "$" + std::to_string(table.num_columns() + computed_columns.size());
            computed_columns.push_back(std::move(computed_col));

            return new parser::variable_node(computed_var_token);
        }

        return &node;
    }

    cudf::table_view computed_columns_view() {
        std::vector<cudf::column_view> computed_views(computed_columns.size());
        std::transform(std::cbegin(computed_columns), std::cend(computed_columns), computed_views.begin(), [](auto & col){
            return col->view();
        });
        return cudf::table_view{computed_views};
    }

    std::vector<std::unique_ptr<cudf::column>> release_computed_columns() { return std::move(computed_columns); }

private:
    cudf::table_view table;
    std::vector<std::unique_ptr<cudf::column>> computed_columns;
};

struct expr_output_type_visitor : public ral::parser::node_visitor
{
public:
	expr_output_type_visitor(const cudf::table_view & table) : table_{table} { }

	void visit(const ral::parser::operad_node& node) override {
		cudf::data_type output_type;
		if (is_literal(node.value)) {
			output_type = static_cast<const ral::parser::literal_node&>(node).type();
		} else {
            cudf::size_type idx = static_cast<const ral::parser::variable_node&>(node).index();
			output_type = table_.column(idx).type();

            // Also store the variable idx for later use
            variable_indices_.push_back(idx);
		}

		node_to_type_map_.insert({&node, output_type});
		expr_output_type_ = output_type;
	}

	void visit(const ral::parser::operator_node& node) override {
		cudf::data_type output_type;
		operator_type op = map_to_operator_type(node.value);
		if(is_binary_operator(op)) {
			output_type = cudf::data_type{get_output_type(op, node_to_type_map_.at(node.children[0].get()).id(), node_to_type_map_.at(node.children[1].get()).id())};
		} else if (is_unary_operator(op)) {
			output_type = cudf::data_type{get_output_type(op, node_to_type_map_.at(node.children[0].get()).id())};
		}else{
            output_type = cudf::data_type{get_output_type(op)};
        }

		node_to_type_map_.insert({&node, output_type});
		expr_output_type_ = output_type;
	}

	cudf::data_type get_expr_output_type() { return expr_output_type_; }

    const std::vector<cudf::size_type> & get_variable_indices() { return variable_indices_; }

private:
    cudf::data_type expr_output_type_;
    std::vector<cudf::size_type> variable_indices_;

	std::map<const ral::parser::node*, cudf::data_type> node_to_type_map_;
	cudf::table_view table_;
};

std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions) {
    using interops::column_index_type;

    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> out_columns(expressions.size());

    std::vector<bool> column_used(table.num_columns(), false);
    std::vector<std::pair<int, int>> out_idx_computed_idx_pair;

    std::vector<parser::parse_tree> expr_tree_vector;
    std::vector<cudf::mutable_column_view> interpreter_out_column_views;

    function_evaluator_transformer evaluator{table};
    for(size_t i = 0; i < expressions.size(); i++){
        std::string expression = replace_calcite_regex(expressions[i]);
        expression = expand_if_logical_op(expression);
        parser::parse_tree tree;
        tree.build(expression);
        tree.transform_to_custom_op();
        tree.transform(evaluator);

        if (tree.root().type == parser::node_type::LITERAL) {
            cudf::data_type literal_type = static_cast<const ral::parser::literal_node&>(tree.root()).type();
            std::unique_ptr<cudf::scalar> literal_scalar = get_scalar_from_string(tree.root().value, literal_type);
            out_columns[i] = std::make_unique<ral::frame::BlazingColumnOwner>(cudf::make_column_from_scalar(*literal_scalar, table.num_rows()));
        } else if (tree.root().type == parser::node_type::VARIABLE) {
            cudf::size_type idx = static_cast<const ral::parser::variable_node&>(tree.root()).index();
            if (idx < table.num_columns()) {
                out_columns[i] = std::make_unique<ral::frame::BlazingColumnOwner>(std::make_unique<cudf::column>(table.column(idx)));
            } else {
                out_idx_computed_idx_pair.push_back({i, idx - table.num_columns()});
            }
        } else {
        	expr_output_type_visitor visitor{cudf::table_view{{table, evaluator.computed_columns_view()}}};
	        tree.visit(visitor);

            cudf::data_type expr_out_type = visitor.get_expr_output_type();

            auto new_column = cudf::make_fixed_width_column(expr_out_type, table.num_rows(), cudf::mask_state::UNINITIALIZED);
            interpreter_out_column_views.push_back(new_column->mutable_view());
            out_columns[i] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(new_column));

            // Keep track of which columns are used in the expression
            for(auto&& idx : visitor.get_variable_indices()) {
                if (idx < table.num_columns()) {
                    column_used[idx] = true;
                }
            }

            expr_tree_vector.emplace_back(std::move(tree));
        }
    }

    auto computed_columns = evaluator.release_computed_columns();
    for (auto &&p : out_idx_computed_idx_pair) {
        out_columns[p.first] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(computed_columns[p.second]));
    }

    // Get the needed columns indices in order and keep track of the mapped indices
    std::map<column_index_type, column_index_type> col_idx_map;
    std::vector<cudf::size_type> input_col_indices;
    for(size_t i = 0; i < column_used.size(); i++) {
        if(column_used[i]) {
            col_idx_map.insert({i, col_idx_map.size()});
            input_col_indices.push_back(i);
        }
    }

    std::vector<std::unique_ptr<cudf::column>> filtered_computed_columns;
    std::vector<cudf::column_view> filtered_computed_views;
    for(size_t i = 0; i < computed_columns.size(); i++) {
        if(computed_columns[i]) {
            // If computed_columns[i] has not been moved to out_columns
            // then it will be used as input in interops
            col_idx_map.insert({table.num_columns() + i, col_idx_map.size()});
            filtered_computed_views.push_back(computed_columns[i]->view());
            filtered_computed_columns.push_back(std::move(computed_columns[i]));
        }
    }

    cudf::table_view interops_input_table{{table.select(input_col_indices), cudf::table_view{filtered_computed_views}}};

    std::vector<column_index_type> left_inputs;
    std::vector<column_index_type> right_inputs;
    std::vector<column_index_type> outputs;
    std::vector<column_index_type> final_output_positions;
    std::vector<operator_type> operators;
    std::vector<std::unique_ptr<cudf::scalar>> left_scalars;
    std::vector<std::unique_ptr<cudf::scalar>> right_scalars;

    for (size_t i = 0; i < expr_tree_vector.size(); i++) {
        final_output_positions.push_back(interops_input_table.num_columns() + i);

        interops::add_expression_to_interpreter_plan(expr_tree_vector[i],
                                                    col_idx_map,
                                                    interops_input_table.num_columns() + interpreter_out_column_views.size(),
                                                    interops_input_table.num_columns() + i,
                                                    left_inputs,
                                                    right_inputs,
                                                    outputs,
                                                    operators,
                                                    left_scalars,
                                                    right_scalars);
    }

    // TODO: Find a proper solution for plan with input or output index greater than 63
	auto max_left_it = std::max_element(left_inputs.begin(), left_inputs.end());
	auto max_right_it = std::max_element(right_inputs.begin(), right_inputs.end());
	auto max_out_it = std::max_element(outputs.begin(), outputs.end());
    if (!expr_tree_vector.empty() && std::max(std::max(*max_left_it, *max_right_it), *max_out_it) >= 64) {
        out_columns.clear();
        computed_columns.clear();

        size_t const half_size = expressions.size() / 2;
        std::vector<std::string> split_lo(expressions.begin(), expressions.begin() + half_size);
        std::vector<std::string> split_hi(expressions.begin() + half_size, expressions.end());
        auto out_cols_lo = evaluate_expressions(table, split_lo);
        auto out_cols_hi = evaluate_expressions(table, split_hi);

        std::move(out_cols_hi.begin(), out_cols_hi.end(), std::back_inserter(out_cols_lo));
        return std::move(out_cols_lo);
    }
    // END

    if(!expr_tree_vector.empty()){
        cudf::mutable_table_view out_table_view(interpreter_out_column_views);

        interops::perform_interpreter_operation(out_table_view,
                                                interops_input_table,
                                                left_inputs,
                                                right_inputs,
                                                outputs,
                                                final_output_positions,
                                                operators,
                                                left_scalars,
                                                right_scalars,
                                                table.num_rows());
    }

    return std::move(out_columns);
}

std::unique_ptr<ral::frame::BlazingTable> process_project(
  std::unique_ptr<ral::frame::BlazingTable> blazing_table_in,
  const std::string & query_part,
  blazingdb::manager::Context * context) {

    std::string combined_expression = query_part.substr(
        query_part.find("(") + 1,
        (query_part.rfind(")") - query_part.find("(")) - 1
    );

    std::vector<std::string> named_expressions = get_expressions_from_expression_list(combined_expression);
    std::vector<std::string> expressions(named_expressions.size());
    std::vector<std::string> out_column_names(named_expressions.size());
    for(int i = 0; i < named_expressions.size(); i++) {
        const std::string & named_expr = named_expressions[i];

        std::string name = named_expr.substr(0, named_expr.find("=["));
        std::string expression = named_expr.substr(named_expr.find("=[") + 2 , (named_expr.size() - named_expr.find("=[")) - 3);

        expressions[i] = expression;
        out_column_names[i] = name;
    }

    return std::make_unique<ral::frame::BlazingTable>(evaluate_expressions(blazing_table_in->view(), expressions), out_column_names);
}

} // namespace processor
} // namespace ral
