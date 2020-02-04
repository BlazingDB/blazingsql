#include <cudf/column/column_view.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>
#include <cudf/strings/combine.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/substring.hpp>
#include <cudf/strings/convert/convert_booleans.hpp>
#include <cudf/strings/convert/convert_datetime.hpp>
#include <cudf/strings/convert/convert_floats.hpp>
#include <cudf/strings/convert/convert_integers.hpp>
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

    template<typename T, std::enable_if_t<cudf::is_compound<T>()> * = nullptr>
    std::unique_ptr<cudf::column> operator()(const cudf::column_view & col) {
        return nullptr;
    }
};

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
        RAL_EXPECTS(is_var_column(arg_tokens[0]), "LIKE operator not supported for intermediate columns or literals");
        
        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        std::string regex = like_expression_to_regex_str(arg_tokens[1].substr(1, arg_tokens[1].size() - 2));

        computed_col = cudf::strings::contains_re(column, regex);
        break;
    }
    case interops::operator_type::BLZ_STR_SUBSTRING:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]), "SUBSTRING operator not supported for intermediate columns or literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        std::string literal_str = arg_tokens[1].substr(1, arg_tokens[1].size() - 2);
        size_t pos = literal_str.find(":");
        int start = std::max(std::stoi(literal_str.substr(0, pos)), 1) - 1;
        int length = pos != std::string::npos ? std::stoi(literal_str.substr(pos + 1)) : -1;
        int end = length >= 0 ? start + length : -1;

        computed_col = cudf::strings::slice_strings(column, start, end);
        break;
    }
    case interops::operator_type::BLZ_STR_CONCAT:
    {
        assert(arg_tokens.size() == 2);
        RAL_EXPECTS(is_var_column(arg_tokens[0]) || is_string(arg_tokens[0]), "SUBSTRING operator not supported for intermediate columns");
        RAL_EXPECTS(is_var_column(arg_tokens[1]) || is_string(arg_tokens[1]), "SUBSTRING operator not supported for intermediate columns");
        RAL_EXPECTS(!(is_string(arg_tokens[0]) && is_string(arg_tokens[1])), "Operations between literals is not supported");

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
    case interops::operator_type::BLZ_CAST_VARCHAR:
    {
        assert(arg_tokens.size() == 1);
        RAL_EXPECTS(is_var_column(arg_tokens[0]), "CAST operator not supported for intermediate columns or literals");

        cudf::column_view column = table.column(get_index(arg_tokens[0]));

        computed_col = cudf::experimental::type_dispatcher(column.type(), cast_to_str_functor{}, column);
        break;
    }
    case interops::operator_type::BLZ_CAST_INTEGER:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            break;
        }
        
        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT32});
        }
        break;
    }
    case interops::operator_type::BLZ_CAST_BIGINT:
    {
        assert(arg_tokens.size() == 1);
        
        if (!is_var_column(arg_tokens[0])) {
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_integers(column, cudf::data_type{cudf::type_id::INT64});
        }
        break;
    }
    case interops::operator_type::BLZ_CAST_FLOAT:
    {
        assert(arg_tokens.size() == 1);
        
        if (!is_var_column(arg_tokens[0])) {
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_floats(column, cudf::data_type{cudf::type_id::FLOAT32});
        }
        break;
    }
    case interops::operator_type::BLZ_CAST_DOUBLE:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_floats(column, cudf::data_type{cudf::type_id::FLOAT64});
        }
        break;
    }
    case interops::operator_type::BLZ_CAST_DATE:
    {
        assert(arg_tokens.size() == 1);

        if (!is_var_column(arg_tokens[0])) {
            break;
        }

        cudf::column_view column = table.column(get_index(arg_tokens[0]));
        if (column.type().id() == cudf::type_id::STRING) {
            computed_col = cudf::strings::to_timestamps(column, cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}, "%Y-%m-%d");
        }
        break;
    }
    case interops::operator_type::BLZ_CAST_TIMESTAMP:
    {
        assert(arg_tokens.size() == 1);
        
        if (!is_var_column(arg_tokens[0])) {
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

} // namespace strings

class function_evaluator_transformer : public parser::parse_node_transformer {
public:
    function_evaluator_transformer(const cudf::table_view & table) : table{table} {}

    parser::parse_node * transform(const parser::operad_node& node) override { return const_cast<parser::operad_node *>(&node); }
    
    parser::parse_node * transform(const parser::operator_node& node) override {
        std::string op_token = node.value;
        std::vector<std::string> arg_tokens(node.children.size());
        std::transform(std::cbegin(node.children), std::cend(node.children), arg_tokens.begin(), [](auto & child){
            return child->value;
        });

        std::vector<cudf::column_view> computed_views(computed_columns.size());
        std::transform(std::cbegin(computed_columns), std::cend(computed_columns), computed_views.begin(), [](auto & col){
            return col->view();
        });
        auto computed_col = strings::evaluate_string_functions(cudf::table_view{{table, cudf::table_view{computed_views}}}, op_token, arg_tokens);
        
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
            
            return new parser::operad_node(computed_var_token);
        }

        return const_cast<parser::operator_node *>(&node); 
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


std::unique_ptr<cudf::experimental::table> evaluate_expressions(
    const cudf::table_view & table,
    const std::vector<std::string> & expressions) {
    using interops::column_index_type;

    std::vector<std::unique_ptr<cudf::column>> out_columns(expressions.size());
    
    std::vector<bool> column_used(table.num_columns(), false);
    std::vector<std::pair<int, int>> out_idx_computed_idx_pair;

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
            cudf::type_id expr_out_type = get_output_type_expression(cudf::table_view{{table, evaluator.computed_columns_view()}}, expression);

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
                    column_used[idx] = true;
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
                RAL_EXPECTS(!!literal_scalar, "NULL literal not supported in projection");
                
                // TODO: verify that in-place fill works correctly, seems there is a bug currently
                out_columns[i] = cudf::experimental::fill(*out_columns[i], 0, out_columns[i]->size(), *literal_scalar);
            }
        } else {
            cudf::size_type idx = get_index(expression);
            if (idx < table.num_columns()) {
                out_columns[i] = std::make_unique<cudf::column>(table.column(idx));
            } else {
                out_idx_computed_idx_pair.push_back({i, idx - table.num_columns()});
            }
        }
    }

    auto computed_columns = evaluator.release_computed_columns();
    for (auto &&p : out_idx_computed_idx_pair) {
        out_columns[p.first] = std::move(computed_columns[p.second]);
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
    for(size_t i = 0; i < computed_columns.size(); i++) {
        if(computed_columns[i]) { 
            // If computed_columns[i] has not been moved to out_columns
            // then it will be used as input in interops
            col_idx_map.insert({table.num_columns() + i, col_idx_map.size()});
            filtered_computed_columns.push_back(std::move(computed_columns[i]));
        }
    }

    std::vector<cudf::column_view> filtered_computed_views(filtered_computed_columns.size());
    std::transform(std::cbegin(filtered_computed_columns), std::cend(filtered_computed_columns), filtered_computed_views.begin(), [](auto & col){
        return col->view();
    });

    cudf::table_view interops_input_table{{table.select(input_col_indices), cudf::table_view{filtered_computed_views}}};

    std::vector<column_index_type> left_inputs;
    std::vector<column_index_type> right_inputs;
    std::vector<column_index_type> outputs;
    std::vector<column_index_type> final_output_positions;
    std::vector<interops::operator_type> operators;
    std::vector<std::unique_ptr<cudf::scalar>> left_scalars;
    std::vector<std::unique_ptr<cudf::scalar>> right_scalars;

    cudf::size_type cur_expression_out = 0;
    for(auto & tokens : tokenized_expression_vector){
        final_output_positions.push_back(interops_input_table.num_columns() + final_output_positions.size());

        interops::add_expression_to_interpreter_plan(tokens,
                                                    interops_input_table,
                                                    col_idx_map,
                                                    cur_expression_out,
                                                    interpreter_out_column_views.size(),
                                                    left_inputs,
                                                    right_inputs,
                                                    outputs,
                                                    operators,
                                                    left_scalars,
                                                    right_scalars);

        cur_expression_out++;
    }

    if(cur_expression_out > 0){
        cudf::mutable_table_view out_table_view(interpreter_out_column_views);

        interops::perform_interpreter_operation(out_table_view,
                                                interops_input_table,
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

        std::string name = named_expr.substr(0, named_expr.find("=["));
        std::string expression = named_expr.substr(named_expr.find("=[") + 2 , (named_expr.size() - named_expr.find("=[")) - 3);

        expressions[i] = expression;
        out_column_names[i] = name;
    }

    return std::make_unique<ral::frame::BlazingTable>(evaluate_expressions(table.view(), expressions), out_column_names);
}

} // namespace processor
} // namespace ral
