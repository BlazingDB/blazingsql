#include "LogicalProject.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/filling.hpp>

#include "../../CalciteExpressionParsing.h"
#include "../../Interpreter/interpreter_cpp.h"

namespace ral {
namespace processor {

std::unique_ptr<ral::frame::BlazingTable> process_project(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context) {
  using interops::column_index_type;

    std::string combined_expression = query_part.substr(
        query_part.find("(") + 1,
        (query_part.rfind(")") - query_part.find("(")) - 1
    );

    std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);

    std::vector<bool> col_used_in_expression(table.view().num_columns(), false);

    std::vector<std::unique_ptr<cudf::column>> columns(expressions.size());
    std::vector<cudf::mutable_column_view> col_outputs_views;

    std::vector<std::string> names(expressions.size());

    std::vector<cudf::type_id> output_type_expressions(expressions.size()); //contains output types
                                                                             //for columns that are expressions, if they are not expressions we skip over it

    cudf::table_view filtered_table;
    std::vector<column_index_type> left_inputs;
    std::vector<column_index_type> right_inputs;
    std::vector<column_index_type> outputs;
    std::vector<column_index_type> final_output_positions;
    std::vector<gdf_binary_operator_exp> operators;
    std::vector<gdf_unary_operator> unary_operators;
    std::vector<std::unique_ptr<cudf::scalar>> left_scalars;
    std::vector<std::unique_ptr<cudf::scalar>> right_scalars;

    size_t num_total_outputs = 0;

    std::map<column_index_type, column_index_type> col_idx_map;

    // Get the needed columns indices in order and keep track of the mapped indices
    std::vector<cudf::size_type> input_col_indices;

    std::vector<std::vector<std::string>> tokenized_expression_vector;

    for(int i = 0; i < expressions.size(); i++){
        std::string expression = expressions[i].substr(
            expressions[i].find("=[") + 2 ,
            (expressions[i].size() - expressions[i].find("=[")) - 3
        );

        std::string name = expressions[i].substr(
            0, expressions[i].find("=[")
        );

        if(contains_evaluation(expression)){
            cudf::type_id max_temp_type = cudf::type_id::EMPTY;
            output_type_expressions[i] = get_output_type_expression(table, max_temp_type, expression);

            auto new_column = cudf::make_fixed_width_column(cudf::data_type{output_type_expressions[i]}, table.view().num_rows());
            col_outputs_views.push_back(new_column->mutable_view());
            columns[i] = std::move(new_column);

            //todo put this into its own function
            std::string cleaned_expression = clean_calcite_expression(expression);
            std::vector<std::string> tokens = get_tokens_in_reverse_order(cleaned_expression);
            fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(table.view(), tokens);
            tokenized_expression_vector.push_back(tokens);

            // Keep track of which columns are used in the expression
            for(const auto & token : tokens) {
                if(is_var_column(token)) {
                    cudf::size_type index = get_index(token);
                    col_used_in_expression[index] = true;
                }
            }

            names.push_back(cleaned_expression);
            num_total_outputs++;
        }
        else{
             // TODO percy this code is duplicated inside get_index, refactor get_index
            const std::string cleaned_expression = clean_calcite_expression(expression);
            const bool is_literal_col = is_literal(cleaned_expression);

            if (is_literal_col) {
                cudf::type_id col_type = infer_dtype_from_literal(cleaned_expression);

                if(col_type == GDF_STRING_CATEGORY){
                    //TODO strings
                } else {
                    std::unique_ptr<cudf::column> temp = cudf::make_numeric_column(cudf::data_type(cudf::type_id::INT8), table.view().num_rows());
                    std::unique_ptr<cudf::scalar> literal_scalar = get_scalar_from_string(cleaned_expression);
                    temp = cudf::experimental::fill(temp->mutable_view(), 0, temp->size(), *literal_scalar);
                    names.push_back(cleaned_expression);
                    columns[i] = std::move(temp);
                }
            } else {
                int index = get_index(expression);

                names.push_back(name);
                columns[i] = std::make_unique<cudf::column>(table.view().column(index));
            }
        }
    }

    for(size_t i = 0; i < col_used_in_expression.size(); i++) {
        if(col_used_in_expression[i]) {
            col_idx_map.insert({i, col_idx_map.size()});
            input_col_indices.push_back(i);
        }
    }

    std::vector<std::unique_ptr<cudf::column>> col_outputs;
    filtered_table = table.view().select(input_col_indices);

    size_t cur_expression_out = 0;
    for(auto & tokens : tokenized_expression_vector){
        final_output_positions.push_back(filtered_table.num_columns() + final_output_positions.size());

        interops::add_expression_to_interpreter_plan(tokens,
                                                    filtered_table,
                                                    col_idx_map,
                                                    cur_expression_out,
                                                    num_total_outputs,
                                                    left_inputs,
                                                    right_inputs,
                                                    outputs,
                                                    final_output_positions,
                                                    operators,
                                                    unary_operators,
                                                    left_scalars,
                                                    right_scalars);

        cur_expression_out++;
    }

    if(cur_expression_out>0){
        cudf::mutable_table_view ret_view(col_outputs_views);

        interops::perform_interpreter_operation(ret_view,
                                                filtered_table,
                                                left_inputs,
                                                right_inputs,
                                                outputs,
                                                final_output_positions,
                                                operators,
                                                unary_operators,
                                                left_scalars,
                                                right_scalars);
    }

  return std::make_unique<ral::frame::BlazingTable>( 
    std::make_unique<cudf::experimental::table>( std::move(columns) ), names
  );
}

} // namespace processor
} // namespace ral