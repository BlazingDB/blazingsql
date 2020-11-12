#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "GroupBy.h"
#include "parser/expression_utils.hpp"
#include "CalciteExpressionParsing.h"
#include "CodeTimer.h"
#include "distribution/primitives.h"
#include "utilities/CommonOperations.h"
#include <blazingdb/io/Util/StringUtil.h>
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <regex>

#include <cudf/sorting.hpp>
#include <cudf/replace.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/filling.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/reduction.hpp>

namespace ral {
namespace operators {

// TODO all these return types need to be revisited later. Right now we have issues with some aggregators that only
// support returning the same input type. Also pygdf does not currently support unsigned types (for example count should
// return and unsigned type)
cudf::type_id get_aggregation_output_type(cudf::type_id input_type, AggregateKind aggregation, bool have_groupby) {
	if(aggregation == AggregateKind::COUNT_VALID || aggregation == AggregateKind::COUNT_ALL) {
		return cudf::type_id::INT64;
	} else if(aggregation == AggregateKind::SUM || aggregation == AggregateKind::SUM0) {
		if(have_groupby)
			return input_type;  // current group by function can only handle this
		else {
			// we can assume it is numeric based on the oepration
			// to be safe we should enlarge to the greatest integer or float representation
			return is_type_float(input_type) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64;
		}
	} else if(aggregation == AggregateKind::MIN) {
		return input_type;
	} else if(aggregation == AggregateKind::MAX) {
		return input_type;
	} else if(aggregation == AggregateKind::MEAN) {
		return cudf::type_id::FLOAT64;
	// TODO percy cudf0.12 aggregation pass flag for COUNT_DISTINCT cases
//	} else if(aggregation == GDF_COUNT_DISTINCT) {
//		return cudf::type_id::INT64;
	} else {
		throw std::runtime_error(
			"In get_aggregation_output_type function: aggregation type not supported: " + aggregation);
	}
}

std::string aggregator_to_string(AggregateKind aggregation) {
	if(aggregation == AggregateKind::COUNT_VALID || aggregation == AggregateKind::COUNT_ALL) {
		return "count";
	} else if(aggregation == AggregateKind::SUM) {
		return "sum";
	} else if(aggregation == AggregateKind::SUM0) {
		return "sum0";
	} else if(aggregation == AggregateKind::MIN) {
		return "min";
	} else if(aggregation == AggregateKind::MAX) {
		return "max";
	} else if(aggregation == AggregateKind::MEAN) {
		return "avg";
	// TODO percy cudf0.12 aggregation pass flag for COUNT_DISTINCT cases
//	} else if(aggregation == GDF_COUNT_DISTINCT) {
//		return "count_distinct";
	} else {
		return "";  // FIXME: is really necessary?
	}
}

AggregateKind get_aggregation_operation(std::string expression_in) {

	std::string operator_string = get_aggregation_operation_string(expression_in);
	std::string expression = get_string_between_outer_parentheses(expression_in);
	if (expression == "" && operator_string == "COUNT"){
		return AggregateKind::COUNT_ALL;
	} else if(operator_string == "SUM") {
		return AggregateKind::SUM;
	} else if(operator_string == "$SUM0") {
		return AggregateKind::SUM0;
	} else if(operator_string == "AVG") {
		return AggregateKind::MEAN;
	} else if(operator_string == "MIN") {
		return AggregateKind::MIN;
	} else if(operator_string == "MAX") {
		return AggregateKind::MAX;
	} else if(operator_string == "COUNT") {
		return AggregateKind::COUNT_VALID;
	}

	throw std::runtime_error(
		"In get_aggregation_operation function: aggregation type not supported, " + operator_string);
}

cudf::aggregation::Kind convertAggregationCudf(AggregateKind input){
	if(input == AggregateKind::SUM){
		return cudf::aggregation::Kind::SUM;
	}else if(input == AggregateKind::MEAN){
		return cudf::aggregation::Kind::MEAN;
	}else if(input == AggregateKind::MIN){
		return cudf::aggregation::Kind::MIN;
	}else if(input == AggregateKind::MAX){
		return cudf::aggregation::Kind::MAX;
	}else if(input == AggregateKind::COUNT_VALID){
		return cudf::aggregation::Kind::COUNT_VALID;
	}else if(input == AggregateKind::COUNT_ALL){
		return cudf::aggregation::Kind::COUNT_ALL;
	}else if(input == AggregateKind::SUM0){
		return cudf::aggregation::Kind::SUM;
	}
	throw std::runtime_error(
		"In convertAggregationCudf function: AggregateKind type not supported");
}

std::vector<int> get_group_columns(std::string query_part) {
	std::string temp_column_string = get_named_expression(query_part, "group");
	if(temp_column_string.size() <= 2) {
		return std::vector<int>();
	}

	// Now we have somethig like {0, 1}
	temp_column_string = temp_column_string.substr(1, temp_column_string.length() - 2);
	std::vector<std::string> column_numbers_string = StringUtil::split(temp_column_string, ",");
	std::vector<int> group_column_indices(column_numbers_string.size());
	for(int i = 0; i < column_numbers_string.size(); i++) {
		group_column_indices[i] = std::stoull(column_numbers_string[i], 0);
	}
	return group_column_indices;
}

std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>,std::vector<std::string>>
	parseGroupByExpression(const std::string & queryString){

	auto rangeStart = queryString.find("(");
	auto rangeEnd = queryString.rfind(")") - rangeStart;
	std::string combined_expression = queryString.substr(rangeStart + 1, rangeEnd - 1);

	std::vector<int> group_column_indices = get_group_columns(combined_expression);

	// Get aggregations
	std::vector<std::string> aggregation_expressions;
	std::vector<std::string> aggregation_column_assigned_aliases;
	std::vector<std::string> expressions = get_expressions_from_expression_list(combined_expression);
	for(std::string expr : expressions) {
		std::string expression = std::regex_replace(expr, std::regex("^ +| +$|( ) +"), "$1");
		if(expression.find("group=") == std::string::npos) {
			aggregation_expressions.push_back(expression);

			// if the aggregation has an alias, lets capture it here, otherwise we'll figure out what to call the
			// aggregation based on its input
			if(expression.find("EXPR$") == 0)
				aggregation_column_assigned_aliases.push_back("");
			else
				aggregation_column_assigned_aliases.push_back(expression.substr(0, expression.find("=[")));
		}
	}
	std::vector<AggregateKind> aggregation_types;
	std::vector<std::string> aggregation_input_expressions;
	for(std::string expression : aggregation_expressions) {
		aggregation_types.push_back(get_aggregation_operation(expression));
		aggregation_input_expressions.push_back(get_string_between_outer_parentheses(expression));
	}
	return std::make_tuple(std::move(group_column_indices), std::move(aggregation_input_expressions),
		std::move(aggregation_types), std::move(aggregation_column_assigned_aliases));
}


std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>,	std::vector<std::string>>
	modGroupByParametersForMerge(const std::vector<int> & group_column_indices,
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & merging_column_names) {

	std::vector<AggregateKind> mod_aggregation_types = aggregation_types;
	std::vector<std::string> mod_aggregation_input_expressions(aggregation_types.size());
	std::vector<std::string> mod_aggregation_column_assigned_aliases(mod_aggregation_types.size());
	std::vector<int> mod_group_column_indices(group_column_indices.size());
	std::iota(mod_group_column_indices.begin(), mod_group_column_indices.end(), 0);
	for (int i = 0; i < mod_aggregation_types.size(); i++){
		if (mod_aggregation_types[i] == AggregateKind::COUNT_ALL || mod_aggregation_types[i] == AggregateKind::COUNT_VALID){
			mod_aggregation_types[i] = AggregateKind::SUM; // if we have a COUNT, we want to SUM the output of the counts from other nodes
		}
		mod_aggregation_input_expressions[i] = std::to_string(i + mod_group_column_indices.size()); // we just want to aggregate the input columns, so we are setting the indices here
		mod_aggregation_column_assigned_aliases[i] = merging_column_names[i + mod_group_column_indices.size()];
	}
	return std::make_tuple(std::move(mod_group_column_indices), std::move(mod_aggregation_input_expressions),
		std::move(mod_aggregation_types), std::move(mod_aggregation_column_assigned_aliases));
}

using namespace ral::distribution;

std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
		const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices) {

	std::unique_ptr<cudf::table> output = cudf::drop_duplicates(table.view(),
		group_column_indices,
		cudf::duplicate_keep_option::KEEP_FIRST);

	return std::make_unique<ral::frame::BlazingTable>( std::move(output), table.names() );
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_input_expressions,
		const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases){

	std::vector<std::unique_ptr<cudf::scalar>> reductions;
	std::vector<std::string> agg_output_column_names;
	for (int i = 0; i < aggregation_types.size(); i++){
		if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
			std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
			auto numeric_s = static_cast< cudf::scalar_type_t<int64_t>* >(scalar.get());
			numeric_s->set_value((int64_t)(table.view().num_rows()));
			reductions.emplace_back(std::move(scalar));
		} else {
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> aggregation_input_scope_holder;
			CudfColumnView aggregation_input;
			if(is_var_column(aggregation_input_expressions[i]) || is_number(aggregation_input_expressions[i])) {
				aggregation_input = table.view().column(get_index(aggregation_input_expressions[i]));
			} else {
				aggregation_input_scope_holder = ral::processor::evaluate_expressions(table.view(), {aggregation_input_expressions[i]});
				aggregation_input = aggregation_input_scope_holder[0]->view();
			}

			if( aggregation_types[i] == AggregateKind::COUNT_VALID) {
				std::unique_ptr<cudf::scalar> scalar = cudf::make_numeric_scalar(cudf::data_type(cudf::type_id::INT64));
				auto numeric_s = static_cast< cudf::scalar_type_t<int64_t>* >(scalar.get());
				numeric_s->set_value((int64_t)(aggregation_input.size() - aggregation_input.null_count()));
				reductions.emplace_back(std::move(scalar));
			} else {
				std::unique_ptr<cudf::aggregation> agg =
					std::make_unique<cudf::aggregation>(convertAggregationCudf(aggregation_types[i]));
				cudf::type_id output_type = get_aggregation_output_type(aggregation_input.type().id(), aggregation_types[i], false);
				std::unique_ptr<cudf::scalar> reduction_out = cudf::reduce(aggregation_input, agg, cudf::data_type(output_type));
				if (aggregation_types[i] == AggregateKind::SUM0 && !reduction_out->is_valid()){ // if this aggregation was a SUM0, and it was not valid, we want it to be a valid 0 instead
					std::unique_ptr<cudf::scalar> zero_scalar = get_scalar_from_string("0", reduction_out->type()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
					reductions.emplace_back(std::move(zero_scalar));
				} else {
					reductions.emplace_back(std::move(reduction_out));
				}
			}
		}

		// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
		if(aggregation_column_assigned_aliases[i] == "") {
			if(aggregation_input_expressions[i] == "" && aggregation_types[i] == AggregateKind::COUNT_ALL) { // this is a COUNT(*)
				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(*)");
			} else {
				agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table.names().at(get_index(aggregation_input_expressions[i])) + ")");
			}
		} else {
			agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
		}
	}
	// convert scalars into columns
	std::vector<std::unique_ptr<cudf::column>> output_columns;
	for (int i = 0; i < reductions.size(); i++){
		std::unique_ptr<cudf::column> temp = cudf::make_column_from_scalar(*(reductions[i]), 1);
		output_columns.emplace_back(std::move(temp));
	}
	return std::make_unique<ral::frame::BlazingTable>(std::move(std::make_unique<CudfTable>(std::move(output_columns))), agg_output_column_names);
}

std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
		const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_input_expressions, const std::vector<AggregateKind> & aggregation_types,
		const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices) {

	// lets get the unique expressions. This is how many aggregation requests we will need
	std::vector<std::string> unique_expressions = aggregation_input_expressions;
	std::sort( unique_expressions.begin(), unique_expressions.end() );
	auto it = std::unique( unique_expressions.begin(), unique_expressions.end() );
	unique_expressions.resize( std::distance(unique_expressions.begin(),it) );

	// We will iterate over the unique expressions and create an aggregation request for each one.
	// We do it this way, because you could have something like min(colA), max(colA), sum(colA).
	// These three aggregations would all be in one request because they have the same input
	std::vector< std::unique_ptr<ral::frame::BlazingColumn> > aggregation_inputs_scope_holder;
	std::vector<cudf::groupby::aggregation_request> requests;
	std::vector<int> agg_out_indices;
	std::vector<std::string> agg_output_column_names;
	for (size_t u = 0; u < unique_expressions.size(); u++){
		std::string expression = unique_expressions[u];

		CudfColumnView aggregation_input; // this is the input from which we will crete the aggregation request
		bool got_aggregation_input = false;
		std::vector<std::unique_ptr<cudf::aggregation>> agg_ops_for_request;
		for (size_t i = 0; i < aggregation_input_expressions.size(); i++){
			if (expression == aggregation_input_expressions[i]){

				int column_index = -1;
				// need to calculate or determine the aggregation input only once
				if (!got_aggregation_input) {
					if(expression == "" && aggregation_types[i] == AggregateKind::COUNT_ALL ) { // this is COUNT(*). Lets just pick the first column
						aggregation_input = table.view().column(0);
					} else if(is_var_column(expression) || is_number(expression)) {
						column_index = get_index(expression);
						aggregation_input = table.view().column(column_index);
					} else {
						std::vector< std::unique_ptr<ral::frame::BlazingColumn> > computed_columns = ral::processor::evaluate_expressions(table.view(), {expression});
						aggregation_inputs_scope_holder.insert(aggregation_inputs_scope_holder.end(), std::make_move_iterator(computed_columns.begin()), std::make_move_iterator(computed_columns.end()));
						aggregation_input = aggregation_inputs_scope_holder.back()->view();
					}
					got_aggregation_input = true;
				}
				agg_ops_for_request.push_back(std::make_unique<cudf::aggregation>(convertAggregationCudf(aggregation_types[i])));
				agg_out_indices.push_back(i);  // this is to know what is the desired order of aggregations output

				// if the aggregation was given an alias lets use it, otherwise we'll name it based on the aggregation and input
				if(aggregation_column_assigned_aliases[i] == "") {
					if(aggregation_types[i] == AggregateKind::COUNT_ALL) {  // COUNT(*) case
						agg_output_column_names.push_back("COUNT(*)");
					} else {
						if (column_index == -1){
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + expression + ")");
						} else {
							agg_output_column_names.push_back(aggregator_to_string(aggregation_types[i]) + "(" + table.names().at(column_index) + ")");
						}
					}
				} else {
					agg_output_column_names.push_back(aggregation_column_assigned_aliases[i]);
				}
			}
		}
			requests.push_back(cudf::groupby::aggregation_request {.values = aggregation_input, .aggregations = std::move(agg_ops_for_request)});
	}

	CudfTableView keys = table.view().select(group_column_indices);
	cudf::groupby::groupby group_by_obj(keys, cudf::null_policy::INCLUDE);
	std::pair<std::unique_ptr<cudf::table>, std::vector<cudf::groupby::aggregation_result>> result = group_by_obj.aggregate( requests );

	// output table is grouped columns and then aggregated columns
	std::vector< std::unique_ptr<cudf::column> > output_columns = result.first->release();
	output_columns.resize(agg_out_indices.size() + group_column_indices.size());

	// lets collect all the aggregated results from the results structure and then add them to output_columns
	std::vector< std::unique_ptr<cudf::column> > agg_cols_out;
	for (int i = 0; i < result.second.size(); i++){
		for (int j = 0; j < result.second[i].results.size(); j++){
			agg_cols_out.emplace_back(std::move(result.second[i].results[j]));
		}
	}
	for (int i = 0; i < agg_out_indices.size(); i++){
		if (aggregation_types[agg_out_indices[i]] == AggregateKind::SUM0 && agg_cols_out[i]->null_count() > 0){
			std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string("0", agg_cols_out[i]->type()); // this does not need to be from a string, but this is a convenient way to make the scalar i need
			std::unique_ptr<cudf::column> temp = cudf::replace_nulls(agg_cols_out[i]->view(), *scalar );
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(temp);
		} else {
			output_columns[agg_out_indices[i] + group_column_indices.size()] = std::move(agg_cols_out[i]);
		}
	}
	std::unique_ptr<CudfTable> output_table = std::make_unique<CudfTable>(std::move(output_columns));

	// lets put together the output names
	std::vector<std::string> output_names;
	for (int i = 0; i < group_column_indices.size(); i++){
		output_names.push_back(table.names()[group_column_indices[i]]);
	}
	output_names.resize(agg_out_indices.size() + group_column_indices.size());
	for (int i = 0; i < agg_out_indices.size(); i++){
		output_names[agg_out_indices[i] + group_column_indices.size()] = agg_output_column_names[i];
	}

	return std::make_unique<BlazingTable>(std::move(output_table), output_names);
}

}  // namespace operators
}  // namespace ral
