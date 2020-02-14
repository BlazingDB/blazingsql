
#include "SkipDataProcessor.h"

#include "skip_data/expression_tree.hpp"
#include "CalciteExpressionParsing.h"
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "Utils.cuh"
#include "utilities/DebuggingUtils.h"

#include <memory> // this is for std::static_pointer_cast
#include <string>
#include <vector>

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
    filter_string = clean_calcite_expression(filter_string);

    std::vector<bool> valid_metadata_columns(names.size(), false); 
    std::vector<std::string> metadata_names = metadata_view.names();
    for (int i = 0; i < names.size(); i++){
        std::string metadata_min_name = "min_" + std::to_string(i) + '_' + names[i];
        std::string metadata_max_name = "max_" + std::to_string(i) + '_' + names[i];
        if (std::find(metadata_names.begin(), metadata_names.end(), metadata_min_name) != metadata_names.end() &&
                std::find(metadata_names.begin(), metadata_names.end(), metadata_max_name) != metadata_names.end()){
            valid_metadata_columns[i] = true;            
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
    auto evaluated_table = ral::processor::evaluate_expressions(metadata_view.view(), {filter_string});

    RAL_EXPECTS(evaluated_table->num_columns() == 1 && evaluated_table->get_column(0).type().id() == cudf::type_id::BOOL8, "Expression in skip_data processing did not evaluate to a boolean mask");

    CudfTableView metadata_ids = metadata_view.view().select({metadata_view.num_columns()-2,metadata_view.num_columns()-1});
    std::vector<std::string> metadata_id_names{metadata_view.names()[metadata_view.num_columns()-2], metadata_view.names()[metadata_view.num_columns()-1]};
    ral::frame::BlazingTableView metadata_ids_view(metadata_ids, metadata_id_names);

    std::unique_ptr<ral::frame::BlazingTable> filtered_metadata_ids = ral::processor::applyBooleanFilter(metadata_view, evaluated_table->get_column(0));

    return std::make_pair(std::move(filtered_metadata_ids), false);

    // int totalNumNodes = context.getTotalNodes();
    // int totalNumRowgroups = filtered_metadata_ids->num_rows();
    // int localNodeIndex = context.getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode());

    // // lets determine the set of rowgroups this node will process
    // int remaining = totalNumRowgroups;
    // int curStart = 0;
    // int localStart = 0;
    // int localEnd = 0;
    // for (int nodeInd = 0; nodeInd < totalNumNodes; nodeInd++){
    //     int batch = remaining/(totalNumNodes-nodeInd);
    //     int curEnd = curStart + batch;
    //     remaining = remaining- batch;
    //     if (nodeInd == localNodeIndex){
    //         localStart = curStart;
    //         localEnd = curEnd;
    //         break;
    //     }
    //     curStart = curEnd;        
    // }
    // if (localEnd - localStart >= 0){
    //     return std::make_pair(filtered_metadata_ids, false);
    // }
    // return std::make_pair(create_empty_result(), true);
}


} // namespace skip_data
} // namespace ral