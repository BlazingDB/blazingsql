#include <spdlog/spdlog.h>
#include <cudf/stream_compaction.hpp>
#include <cudf/copying.hpp>
#include "LogicalFilter.h"
#include "LogicalProject.h"
#include "parser/expression_utils.hpp"
#include "utilities/error.hpp"
#include "Util/StringUtil.h"

namespace ral {
namespace processor {
namespace {

const std::string LOGICAL_FILTER = "LogicalFilter";

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

std::string get_current_date_or_timestamp(std::string expression, blazingdb::manager::Context * context) {
    // We want `CURRENT_TIME` holds the same value as `CURRENT_TIMESTAMP`
	if (expression.find("CURRENT_TIME") != expression.npos) {
		expression = StringUtil::replace(expression, "CURRENT_TIME", "CURRENT_TIMESTAMP");
	}

	std::size_t date_pos = expression.find("CURRENT_DATE");
	std::size_t timestamp_pos = expression.find("CURRENT_TIMESTAMP");

	if (date_pos == expression.npos && timestamp_pos == expression.npos) {
		return expression;
	}

    // CURRENT_TIMESTAMP will return a `ms` format
	std::string	timestamp_str = context->getCurrentTimestamp().substr(0, 23);
    std::string str_to_replace = "CURRENT_TIMESTAMP";

	// In case CURRENT_DATE we want only the date value
	if (date_pos != expression.npos) {
		str_to_replace = "CURRENT_DATE";
        timestamp_str = timestamp_str.substr(0, 10);
	}

	return StringUtil::replace(expression, str_to_replace, timestamp_str);
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
	
	conditional_expression = get_current_date_or_timestamp(conditional_expression, context);
  
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
