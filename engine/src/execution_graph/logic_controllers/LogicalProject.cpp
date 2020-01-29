#include <cudf/column/column_view.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>
#include <cudf/strings/combine.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/substring.hpp>
#include <memory>
#include <regex>
#include <utility>

#include "LogicalProject.h"
#include "../../CalciteExpressionParsing.h"
#include "../../Interpreter/interpreter_cpp.h"
#include "../../parser/expression_tree.hpp"
#include "../../Utils.cuh"

namespace ral {
namespace processor {

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

std::unique_ptr<cudf::column> make_column_from_scalar(const std::string& str, cudf::size_type rows) {
    std::vector<char> chars{};
    std::vector<int32_t> offsets(1, 0);
    for(cudf::size_type k = 0; k < rows; k++) {
        chars.insert(chars.end(), std::cbegin(str), std::cend(str));
        offsets.push_back(offsets.back() + str.length());
    }

    return cudf::make_strings_column(chars, offsets);
}

std::unique_ptr<cudf::column> evaluate_string_functions(const cudf::table_view & table,
                                                        const std::string & op_token,
                                                        const std::vector<std::string> & arg_tokens)
{
    std::unique_ptr<cudf::column> computed_col;

    interops::operator_type op = map_to_operator_type(op_token);
    switch (op)
    {
    case interops::operator_type::BLZ_STR_LIKE:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]), "LIKE operator not supported for intermediate columns");
        
        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        std::string regex = like_expression_to_regex_str(arg_tokens[1].substr(1, arg_tokens[1].size() - 2));

        computed_col = cudf::strings::contains_re(column, regex);
        break;
    }
    case interops::operator_type::BLZ_STR_SUBSTRING:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]), "SUBSTRING operator not supported for intermediate columns");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        std::string literal_str = arg_tokens[1].substr(1, arg_tokens[1].size() - 2);
        size_t pos = literal_str.find(":");
        int start = std::max(std::stoi(literal_str.substr(0, pos)), 1) - 1;
        int end = pos != std::string::npos ? start + std::stoi(literal_str.substr(pos + 1)) : -1;

        computed_col = cudf::strings::slice_strings(column, start, end);
        break;
    }
    case interops::operator_type::BLZ_STR_CONCAT:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]) || is_string(arg_tokens[0]), "SUBSTRING operator not supported for intermediate columns");
        RAL_EXPECTS(is_var_column(arg_tokens[1]) || is_string(arg_tokens[1]), "SUBSTRING operator not supported for intermediate columns");
        RAL_EXPECTS(is_string(arg_tokens[0]) && is_string(arg_tokens[1]), "Operations between literals is not supported");

        if (is_var_column(arg_tokens[0]) && is_var_column(arg_tokens[1])) {
            cudf::column_view column1 = table.column(get_index(arg_tokens[0]));
            cudf::column_view column2 = table.column(get_index(arg_tokens[1]));

            computed_col = cudf::strings::concatenate(cudf::table_view{{column1, column2}});
        } else {
            std::unique_ptr<cudf::column> temp_col1;
            cudf::column_view column1;
            if (is_var_column(arg_tokens[0])) {
                column1 = table.column(get_index(arg_tokens[0]));
            } else {
                std::string literal_str = arg_tokens[0].substr(1, arg_tokens[0].size() - 2);
                temp_col1 = make_column_from_scalar(literal_str, table.num_rows());
                column1 = temp_col1->view();
            }
            
            std::unique_ptr<cudf::column> temp_col2;
            cudf::column_view column2;
            if (is_var_column(arg_tokens[1])) {
                column2 = table.column(get_index(arg_tokens[1]));
            } else {
                std::string literal_str = arg_tokens[1].substr(1, arg_tokens[1].size() - 2);
                temp_col2 = make_column_from_scalar(literal_str, table.num_rows());
                column2 = temp_col2->view();
            }

            computed_col = cudf::strings::concatenate(cudf::table_view{{column1, column2}});
        }
        break;
    }

    return computed_col;
    }
}

} // namespace strings

class function_evaluator_transformer : public parser::parse_node_transformer {
public:
    function_evaluator_transformer(const cudf::table_view & table) : table{table}, computed_table{std::make_unique<cudf::experimental::table>()} {}

    parser::parse_node * transform(const parser::operad_node& node) override { return const_cast<parser::operad_node *>(&node); }
    
    parser::parse_node * transform(const parser::operator_node& node) override {
        std::string op_token = node.value;
        std::vector<std::string> arg_tokens(node.children.size());
        std::transform(std::cbegin(node.children), std::cend(node.children), arg_tokens.begin(), [](auto & child){
            return child->value;
        });

        auto computed_col = strings::evaluate_string_functions(cudf::table_view{{table, computed_table->view()}}, op_token, arg_tokens);
        
        if (computed_col) {
            auto computed_columns = computed_table->release();

            // Discard temp columns used in operations
            for (auto &&token : arg_tokens) {
                if (!is_var_column(token)) continue;

                cudf::size_type idx = get_index(token);
                if (idx >= table.num_columns()) {
                    computed_columns.erase(computed_columns.begin() + (idx -table.num_columns()));
                }
            }
            
            computed_columns.push_back(std::move(computed_col));
            computed_table = std::make_unique<cudf::experimental::table>(std::move(computed_columns));
            
            std::string computed_var_token = "$" + (table.num_columns() + computed_table->num_columns());
            return new parser::operad_node(computed_var_token);
        }

        return const_cast<parser::operator_node *>(&node); 
    }

    cudf::table_view computed_table_view() { return computed_table->view(); }

private:
    cudf::table_view table;
    std::unique_ptr<cudf::experimental::table> computed_table;
};


std::unique_ptr<cudf::experimental::table> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions) {
    using interops::column_index_type;

    std::vector<std::unique_ptr<cudf::column>> out_columns(expressions.size());
    
    std::vector<bool> col_used_in_expression(table.num_columns(), false);

    std::vector<std::vector<std::string>> tokenized_expression_vector;
    std::vector<cudf::mutable_column_view> interpreter_out_column_views;

    function_evaluator_transformer evaluator{table};
    for(size_t i = 0; i < expressions.size(); i++){
        std::string expression = replace_calcite_regex(expressions[i]);
        parser::parse_tree parse_tree;
        parse_tree.build(expression);
        parse_tree.transform_to_custom_op();
        parse_tree.transform(evaluator);
        expression = parse_tree.rebuildExpression();

        if(contains_evaluation(expression)){
            cudf::type_id max_temp_type = cudf::type_id::EMPTY;
            cudf::type_id expr_out_type = get_output_type_expression(table, max_temp_type, expression);

            auto new_column = cudf::make_fixed_width_column(cudf::data_type{expr_out_type}, table.num_rows(), cudf::mask_state::UNINITIALIZED);
            interpreter_out_column_views.push_back(new_column->mutable_view());
            out_columns[i] = std::move(new_column);

            std::string cleaned_expression = clean_calcite_expression(expression);
            std::vector<std::string> tokens = get_tokens_in_reverse_order(cleaned_expression);
            fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(table, tokens);
            tokenized_expression_vector.push_back(tokens);

            // Keep track of which columns are used in the expression
            for(const auto & token : tokens) {
                if (!is_var_column(token)) continue;

                cudf::size_type idx = get_index(token);
                if (idx < table.num_columns()) {
                    col_used_in_expression[idx] = true;
                }
            }
        } else if (is_literal(expression)) {
            cudf::type_id col_type = infer_dtype_from_literal(expression);
            if(col_type == cudf::type_id::STRING){
                std::string scalar_str = expression.substr(1, expression.length() - 2);
                out_columns[i] = strings::make_column_from_scalar(scalar_str, table.num_rows());
            } else {
                out_columns[i] = cudf::make_fixed_width_column(cudf::data_type{col_type}, table.num_rows());
                std::unique_ptr<cudf::scalar> literal_scalar = get_scalar_from_string(expression);

                // TODO: verify that in-place fill works correctly, seems there is a bug currently
                out_columns[i] = cudf::experimental::fill(*out_columns[i], 0, out_columns[i]->size(), *literal_scalar);
            }
        } else {
            cudf::size_type index = get_index(expression);

            out_columns[i] = std::make_unique<cudf::column>(table.column(index));
        }
    }

    // Get the needed columns indices in order and keep track of the mapped indices
    std::map<column_index_type, column_index_type> col_idx_map;
    std::vector<cudf::size_type> input_col_indices;
    for(size_t i = 0; i < col_used_in_expression.size(); i++) {
        if(col_used_in_expression[i]) {
            col_idx_map.insert({i, col_idx_map.size()});
            input_col_indices.push_back(i);
        }
    }

    cudf::table_view filtered_table_view{{table.select(input_col_indices), evaluator.computed_table_view()}};

    std::vector<column_index_type> left_inputs;
    std::vector<column_index_type> right_inputs;
    std::vector<column_index_type> outputs;
    std::vector<column_index_type> final_output_positions;
    std::vector<interops::operator_type> operators;
    std::vector<std::unique_ptr<cudf::scalar>> left_scalars;
    std::vector<std::unique_ptr<cudf::scalar>> right_scalars;

    cudf::size_type cur_expression_out = 0;
    for(auto & tokens : tokenized_expression_vector){
        final_output_positions.push_back(filtered_table_view.num_columns() + final_output_positions.size());

        interops::add_expression_to_interpreter_plan(tokens,
                                                    filtered_table_view,
                                                    col_idx_map,
                                                    cur_expression_out,
                                                    interpreter_out_column_views.size(),
                                                    left_inputs,
                                                    right_inputs,
                                                    outputs,
                                                    final_output_positions,
                                                    operators,
                                                    left_scalars,
                                                    right_scalars);

        cur_expression_out++;
    }

    if(cur_expression_out > 0){
        cudf::mutable_table_view out_table_view(interpreter_out_column_views);

        interops::perform_interpreter_operation(out_table_view,
                                                filtered_table_view,
                                                left_inputs,
                                                right_inputs,
                                                outputs,
                                                final_output_positions,
                                                operators,
                                                left_scalars,
                                                right_scalars);
    }

    return std::make_unique<cudf::experimental::table>(std::move(out_columns));
}

std::unique_ptr<ral::frame::BlazingTable> process_project(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context) {

    std::string combined_expression = query_part.substr(
        query_part.find("(") + 1,
        (query_part.rfind(")") - query_part.find("(")) - 1
    );

    std::vector<std::string> named_expressions = get_expressions_from_expression_list(combined_expression);
    std::vector<std::string> expressions(named_expressions.size());
    std::vector<std::string> out_column_names(named_expressions.size());
    for(int i = 0; i < named_expressions.size(); i++) {
        const std::string & named_expr = named_expressions[i];
        
        std::string name = named_expr.substr(0, expressions[i].find("=["));
        std::string expression = named_expr.substr(named_expr.find("=[") + 2 , (named_expr.size() - named_expr.find("=[")) - 3);
        
        expressions[i] = expression;

        if(contains_evaluation(expression)){
            out_column_names[i] = expression;
        } else if (is_literal(expression)) {
            out_column_names[i] = expression;
        } else {
            out_column_names[i] = name;
        }
    }

    return std::make_unique<ral::frame::BlazingTable>(evaluate_expressions(table.view(), expressions), out_column_names);
}

} // namespace processor
} // namespace ral
