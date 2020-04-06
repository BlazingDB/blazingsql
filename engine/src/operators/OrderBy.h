#ifndef BLAZINGDB_RAL_ORDERBY_OPERATOR_H
#define BLAZINGDB_RAL_ORDERBY_OPERATOR_H

#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"


namespace ral {
namespace operators {
namespace experimental {

namespace {
  using blazingdb::manager::experimental::Context;
}  

std::unique_ptr<ral::frame::BlazingTable> process_sort(const ral::frame::BlazingTableView & table,
const std::string & query_part, Context * context);

/**---------------------------------------------------------------------------*
 * @brief Sorts the columns of the input table according the sortOrderTypes 
 * and sortColIndices.
 *
 * @param[in] table             table whose rows need to be compared for ordering
 * @param[in] sortColIndices    The vector of selected column indices to perform
 *                              the sort.
 * @param[in] sortOrderTypes    The expected sort order for each column. Size
 *                              must be equal to `sortColIndices.size()` or empty.
 *
 * @returns A BlazingTable with rows sorted.
 *---------------------------------------------------------------------------**/
std::unique_ptr<ral::frame::BlazingTable> logicalSort(
  const ral::frame::BlazingTableView & table, const std::vector<int> & sortColIndices, 
  const std::vector<int8_t> & sortOrderTypes);

/**---------------------------------------------------------------------------*
 * @brief In a distributed context, this function determines what the limit would be
 * for this local node. It does this be distributing and collecting the total number of 
 * rows in the table. Then knowing which node index this local node is, it can calculate
 * how many rows are ahead of the ones in this partition
 *
 * @param[in] contex
 * @param[in] local_num_rows    Number of rows of this partition
 * @param[in] limit_rows        Limit being applied to the whole query
 *
 * @returns The limit that would be applied to this partition
 *---------------------------------------------------------------------------**/
cudf::size_type determine_local_limit(Context * context,
	cudf::size_type local_num_rows, cudf::size_type limit_rows);

std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable>>
	sort_and_sample(const ral::frame::BlazingTableView & table, const std::string & query_part, Context * context);

std::vector<std::unique_ptr<ral::frame::BlazingTable>> partition_sort(const ral::frame::BlazingTableView & partitionPlan,
												const ral::frame::BlazingTableView & sortedTable,
												const std::string & query_part,
												Context * context);

std::unique_ptr<ral::frame::BlazingTable> merge(std::vector<ral::frame::BlazingTableView> partitions_to_merge, const std::string & query_part, Context * context);

}  // namespace experimental
}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_ORDERBY_OPERATOR_H
