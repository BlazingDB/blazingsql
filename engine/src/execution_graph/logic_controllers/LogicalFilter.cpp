#include <stack>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
using namespace fmt::literals;

#include <cudf/table/table_view.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>
#include <cudf/partitioning.hpp>

#include "LogicalFilter.h"
#include "LogicalProject.h"
#include "CalciteExpressionParsing.h"
#include "parser/expression_utils.hpp"

#include "distribution/primitives.h"
#include "communication/CommunicationData.h"
#include "utilities/CommonOperations.h"

#include "error.hpp"

namespace ral {
namespace processor {
namespace {

const std::string LOGICAL_FILTER = "LogicalFilter";

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

} // namespace

bool is_logical_filter(const std::string & query_part) {
  return query_part.find(LOGICAL_FILTER) != std::string::npos;
}

std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  const ral::frame::BlazingTableView & table,
  const CudfColumnView & boolValues){
  auto filteredTable = cudf::apply_boolean_mask(
    table.view(),boolValues);
  return std::make_unique<ral::frame::BlazingTable>(std::move(
    filteredTable),table.names());
}

std::unique_ptr<ral::frame::BlazingTable> process_filter(
  const ral::frame::BlazingTableView & table_view,
  const std::string & query_part,
  blazingdb::manager::Context * context) {

	if(table_view.num_rows() == 0) {
		return std::make_unique<ral::frame::BlazingTable>(cudf::empty_like(table_view.view()), table_view.names());
	}

  std::string conditional_expression = get_named_expression(query_part, "condition");
	if(conditional_expression.empty()) {
		conditional_expression = get_named_expression(query_part, "filters");
	}

  std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(table_view.view(), {conditional_expression});

  RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  return applyBooleanFilter(table_view, evaluated_table[0]->view());
}


  namespace{
    typedef std::pair<blazingdb::transport::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
    typedef std::pair<blazingdb::transport::Node, ral::frame::BlazingTableView > NodeColumnView;
  }

  
bool check_if_has_nulls(CudfTableView const& input, std::vector<cudf::size_type> const& keys){
  auto keys_view = input.select(keys);
  if (keys_view.num_columns() != 0 && keys_view.num_rows() != 0 && cudf::has_nulls(keys_view)) {
      return true;
  }

  return false;
}

} // namespace processor
} // namespace ral
