
#include "SkipDataProcessor.h"

#include <cudf/column/column_factories.hpp>
#include "skip_data/expression_tree.hpp"
#include "CalciteExpressionParsing.h"
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "execution_graph/logic_controllers/BlazingColumnView.h"
#include "Utils.cuh"

#include <memory> // this is for std::static_pointer_cast
#include <string>
#include <vector>
#include <numeric>

namespace ral {
namespace skip_data {

// "BindableTableScan(table=[[main, customer]], filters=[[OR(AND(<($0, 15000), =($1, 5)), =($0, *($1, $1)), >=($1, 10), <=($2, 500))]], projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey, c_acctbal]])"
//      projects=[[0, 3, 5]]
// minmax_metadata_table => use these indices [[0, 3, 5]]
// minmax_metadata_table => minmax_metadata_table[[0, 1,  6, 7,  10, 11, size - 2, size - 1]]
std::pair<std::unique_ptr<ral::frame::BlazingTable>, bool> process_skipdata_for_table(
    const ral::frame::BlazingTableView & metadata_view, const std::vector<std::string> & names, std::string table_scan) {
     
    std::string filter_string;
    try {
        filter_string = get_named_expression(table_scan, "condition");
        if(filter_string.empty()) {
            filter_string = get_named_expression(table_scan, "filters");
        }
        if (filter_string.empty()) {
            return std::make_pair(nullptr, true);
        }
    } catch (...) {
        return std::make_pair(nullptr, true);
    }
    filter_string = replace_calcite_regex(filter_string);
    filter_string = expand_if_logical_op(filter_string);

    std::string projects = get_named_expression(table_scan, "projects");
    std::vector<int> column_indeces;
    if (projects == ""){
        column_indeces.resize(names.size());
        std::iota(column_indeces.begin(), column_indeces.end(), 0);
    } else {
        std::vector<std::string> column_index_strings = get_expressions_from_expression_list(projects, true);    
        for (int i = 0; i < column_index_strings.size(); i++){
            int index = std::stoi(column_index_strings[i]);
            column_indeces.push_back(index);        
        }   
    }

    cudf::size_type rows = metadata_view.num_rows();
    std::unique_ptr<cudf::column> temp_no_data = cudf::make_fixed_width_column(
        cudf::data_type{cudf::type_id::INT8}, rows,
        cudf::mask_state::UNINITIALIZED);
    
    std::vector<std::string> metadata_names = metadata_view.names();
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> metadata_columns = metadata_view.toBlazingColumns();
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> projected_metadata_cols;
    std::vector<bool> valid_metadata_columns;    
    for (int i = 0; i < column_indeces.size(); i++){
        int col_index = column_indeces[i];
        std::string metadata_min_name = "min_" + std::to_string(col_index) + '_' + names[col_index];
        std::string metadata_max_name = "max_" + std::to_string(col_index) + '_' + names[col_index];
        if (std::find(metadata_names.begin(), metadata_names.end(), metadata_min_name) != metadata_names.end() &&
                std::find(metadata_names.begin(), metadata_names.end(), metadata_max_name) != metadata_names.end()){
            valid_metadata_columns.push_back(true);

            auto it = std::find(metadata_names.begin(), metadata_names.end(), metadata_min_name);
            int min_col_index = std::distance(metadata_names.begin(), it);
            projected_metadata_cols.emplace_back(std::move(metadata_columns[min_col_index]));
            projected_metadata_cols.emplace_back(std::move(metadata_columns[min_col_index + 1]));            
        } else {
            valid_metadata_columns.push_back(false);
            projected_metadata_cols.emplace_back(std::move(std::make_unique<ral::frame::BlazingColumnView>(temp_no_data->view()))); // these are dummy columns that we wont actually use
            projected_metadata_cols.emplace_back(std::move(std::make_unique<ral::frame::BlazingColumnView>(temp_no_data->view())));
        }
    }
    
    // process filter_string to convert to skip data version
    expression_tree tree;
    if (tree.build(filter_string)){
        // lets drop all columns that do not have skip data
        for (size_t i = 0; i < valid_metadata_columns.size(); i++){ 
            if (!valid_metadata_columns[i]) { // if this column has no metadata lets drop it from the expression tree
                tree.drop({"$" + std::to_string(i)});
            }
        }
        tree.apply_skip_data_rules();
        filter_string =  tree.rebuildExpression();

    } else { // something happened and could not process
        return std::make_pair(nullptr, true);
    }

    if (filter_string.empty()) {
        return std::make_pair(nullptr, true);
    }
        
    // then we follow a similar pattern to process_filter
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = ral::processor::evaluate_expressions(std::move(projected_metadata_cols), {filter_string});

    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression in skip_data processing did not evaluate to a boolean mask");

    CudfTableView metadata_ids = metadata_view.view().select({metadata_view.num_columns()-2,metadata_view.num_columns()-1});
    std::vector<std::string> metadata_id_names{metadata_view.names()[metadata_view.num_columns()-2], metadata_view.names()[metadata_view.num_columns()-1]};
    ral::frame::BlazingTableView metadata_ids_view(metadata_ids, metadata_id_names);

    std::unique_ptr<ral::frame::BlazingTable> filtered_metadata_ids = ral::processor::applyBooleanFilter(metadata_ids_view, evaluated_table[0]->view());

    return std::make_pair(std::move(filtered_metadata_ids), false);
}


} // namespace skip_data
} // namespace ral