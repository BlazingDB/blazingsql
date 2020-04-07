
#include "TableScan.h"

#include <blazingdb/manager/Context.h>

#include "LogicPrimitives.h"
#include "LogicalFilter.h"

#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Util/StringUtil.h>
#include "CodeTimer.h"
#include "Traits/RuntimeTraits.h"
#include "CalciteExpressionParsing.h"

#include <algorithm>
#include <regex>
#include <set>
#include <string>

namespace ral{

namespace processor{

std::unique_ptr<ral::frame::BlazingTable> process_table_scan(
  ral::io::data_loader& input_loader,
  const std::string & query_part,
  ral::io::Schema &schema,
  blazingdb::manager::experimental::Context * queryContext)  
{
	CodeTimer blazing_timer;
	blazing_timer.reset();

    std::vector<size_t> projections = get_projections(query_part);

    std::string aliases_string = get_named_expression(query_part, "aliases");
    std::vector<std::string> aliases_string_split =
        get_expressions_from_expression_list(aliases_string, true);

    // This is for the count(*) case, we don't want to load all the columns
    if(projections.size() == 0 && aliases_string_split.size() == 1) {
        projections.push_back(0);
    }

    std::unique_ptr<ral::frame::BlazingTable> input_table = nullptr;
    if(is_filtered_bindable_scan(query_part)) {
        input_table = input_loader.load_data(queryContext, projections, schema, query_part);
    }else{
        input_table = input_loader.load_data(queryContext, projections, schema,"");
    }
 
    std::vector<std::string> col_names = input_table->names();

    // Setting the aliases only when is not an empty set
    for(size_t col_idx = 0; col_idx < aliases_string_split.size(); col_idx++) {
        // TODO: Rommel, this check is needed when for example the scan has not projects but there are extra
        // aliases
        if(col_idx < input_table->num_columns()) {
            col_names[col_idx] = aliases_string_split[col_idx];
        }
    }
    if(input_table){ // the BlazingTable is not guaranteed to have something
        input_table->setNames(col_names);
    }
	input_table->setNames(col_names);

    int num_rows = input_table->num_rows();
    Library::Logging::Logger().logInfo(
        blazing_timer.logDuration(*queryContext, "evaluate_split_query load_data", "num rows", num_rows));
    blazing_timer.reset();

    queryContext->incrementQueryStep();
    return std::move(input_table);
}

} // end namespace processor

} // end namespace ral
