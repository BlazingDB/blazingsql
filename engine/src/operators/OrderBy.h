#ifndef BLAZINGDB_RAL_ORDERBY_OPERATOR_H
#define BLAZINGDB_RAL_ORDERBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace operators {

namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_sort(std::string query_part);

void process_sort(blazing_frame & input, std::string query_part, Context * queryContext);

}  // namespace operators
}  // namespace ral



namespace ral {
namespace operators {
namespace experimental {

namespace {
  using blazingdb::manager::experimental::Context;
}  

std::unique_ptr<ral::frame::BlazingTable> process_sort(const ral::frame::BlazingTableView & table, 

const std::string & query_part, Context * context);

std::unique_ptr<ral::frame::BlazingTable>  distributed_sort(Context * context,
	const ral::frame::BlazingTableView & table, const std::vector<int> & sortColIndices, 
  const std::vector<int8_t> & sortOrderTypes);


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

std::unique_ptr<ral::frame::BlazingTable> logicalLimit(
  const ral::frame::BlazingTableView & table, cudf::size_type limitRows);


}  // namespace experimental
}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_ORDERBY_OPERATOR_H
