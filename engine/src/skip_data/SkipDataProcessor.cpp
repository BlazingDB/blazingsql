
#include "SkipDataProcessor.h"


#include "skip_data/expression_tree.hpp"
#include "DataFrame.h"
#include <GDFColumn.cuh>
#include "utilities/RalColumn.h"
#include "FileSystem/Uri.h"
#include "io/data_parser/metadata/parquet_metadata.h"
#include "io/data_provider/UriDataProvider.h"
#include "io/Schema.h"
#include "LogicalFilter.h"
#include <cudf/legacy/table.hpp>
#include "communication/CommunicationData.h"
#include "distribution/NodeColumns.h"
#include "distribution/primitives.h"
#include "CalciteInterpreter.h"
#include "CalciteExpressionParsing.h"
#include "utilities/CommonOperations.h"
#include "legacy/stream_compaction.hpp"

#include <memory> // this is for std::static_pointer_cast
#include <string>
#include <vector>

using namespace ral::distribution;

namespace ral {
namespace skip_data {

// "BindableTableScan(table=[[main, customer]], filters=[[OR(AND(<($0, 15000), =($1, 5)), =($0, *($1, $1)), >=($1, 10), <=($2, 500))]], projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey, c_acctbal]])"
//      projects=[[0, 3, 5]]
// minmax_metadata_table => use these indices [[0, 3, 5]]
// minmax_metadata_table => minmax_metadata_table[[0, 1,  6, 7,  10, 11, size - 2, size - 1]]
skipdata_output_t process_skipdata_for_table(ral::io::data_loader & input_loader, std::vector<gdf_column_cpp> new_minmax_metadata_table, std::string table_scan, const Context& context) {
     
    // convert minmax_metadata_table to blazing_frame minmax_metadata_frame which we will use to apply evaluate_expression
    blazing_frame minmax_metadata_frame;
    for (auto column : new_minmax_metadata_table){
        minmax_metadata_frame.add_column(column);
    }
     
    if (minmax_metadata_frame.get_width() == 0){
        return skipdata_output_t();
    } 

    std::string filter_string = get_filter_expression(table_scan);
    if (filter_string.empty()) {
        return skipdata_output_t();
    }
    filter_string = clean_calcite_expression(filter_string);

    // process filter_string to convert to skip data version
    expression_tree tree;
    if (tree.build(filter_string)){
        // lets drop all columns that do not have skip data
        for (size_t i = 0; i < minmax_metadata_frame.get_width()/2 - 1; i++){ // here we are assuming that minmax_metadata_table is 2N+2 columns
            if (minmax_metadata_frame.get_column(i*2).size() == 0){ // if this column has no metadata lets drop it from the expression tree
                tree.drop({"$" + std::to_string(i)});
            }
        }
        tree.apply_skip_data_rules();
        filter_string =  tree.prefix();

    } else { // something happened and could not process
        return skipdata_output_t();
    }
    if (filter_string.empty()) {
        return skipdata_output_t();
    }

    // then we follow a similar pattern to process_filter
    gdf_column_cpp stencil;
    gdf_dtype_extra_info extra_info;
    extra_info.category = nullptr;
    stencil.create_gdf_column(GDF_INT8, extra_info, minmax_metadata_frame.get_num_rows_in_table(0),nullptr,1, "");
    evaluate_expression(minmax_metadata_frame, filter_string, stencil);

    stencil.get_gdf_column()->dtype = GDF_BOOL8; // apply_boolean_mask expects the stencil to be a GDF_BOOL8 which for our purposes the way we are using the GDF_INT8 is the same as GDF_BOOL8

    // the last two columns of minmax_metadata_frame are the rowgroup identifying columns, which are the rowgroup id and the filepath
    std::vector<gdf_column_cpp> row_group_identifiers;
    for (int i = minmax_metadata_frame.get_width() - 2; i < minmax_metadata_frame.get_width();i++){
        row_group_identifiers.push_back(minmax_metadata_frame.get_column(i));
    }

    // we apply the filter to the rowgroup identifiers
    cudf::table inputToFilter = ral::utilities::create_table(row_group_identifiers);
    cudf::table filteredData = cudf::apply_boolean_mask(inputToFilter, *(stencil.get_gdf_column()));

    for(int i = 0; i < row_group_identifiers.size();i++){
        gdf_column* temp_col_view = filteredData.get_column(i);
        temp_col_view->col_name = nullptr; // lets do this because its not always set properly
        gdf_column_cpp temp;
        temp.create_gdf_column(temp_col_view);
        temp.set_name(row_group_identifiers[i].name());
        row_group_identifiers[i] = temp;
    }		

    int totalNumNodes = context.getTotalNodes();
    int totalNumRowgroups = row_group_identifiers[0].size();
    int localNodeIndex = context.getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode());

    // lets determine the set of rowgroups this node will process
    int remaining = totalNumRowgroups;
    int curStart = 0;
    int localStart = 0;
    int localEnd = 0;
    for (int nodeInd = 0; nodeInd < totalNumNodes; nodeInd++){
        int batch = remaining/(totalNumNodes-nodeInd);
        int curEnd = curStart + batch;
        remaining = remaining- batch;
        if (nodeInd == localNodeIndex){
            localStart = curStart;
            localEnd = curEnd;
            break;
        }
        curStart = curEnd;        
    }
    std::vector<int> distinctLocalUserReadableFilePaths;
    std::vector<std::vector<int>> localRowgroupSets;
    //TODO complete the vectors before! @alex

    return std::make_pair(distinctLocalUserReadableFilePaths, localRowgroupSets);
}


} // namespace skip_data
} // namespace ral
