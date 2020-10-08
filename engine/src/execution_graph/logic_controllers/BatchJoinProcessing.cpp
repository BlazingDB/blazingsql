#include "BatchJoinProcessing.h"
#include <string>
#include "parser/expression_tree.hpp"
#include "error.hpp"

namespace ral {
namespace batch {


std::tuple<std::string, std::string, std::string, std::string> parseExpressionToGetTypeAndCondition(const std::string & expression) {
	std::string modified_expression, condition, filter_statement, join_type, new_join_statement;
	modified_expression = expression;
	StringUtil::findAndReplaceAll(modified_expression, "IS NOT DISTINCT FROM", "=");
	split_inequality_join_into_join_and_filter(modified_expression, new_join_statement, filter_statement);

	// Getting the condition and type of join
	condition = get_named_expression(new_join_statement, "condition");
	join_type = get_named_expression(new_join_statement, "joinType");

	if (condition == "true") {
		join_type = CROSS_JOIN;
	}		
	return std::make_tuple(modified_expression, condition, filter_statement, join_type);
}

void parseJoinConditionToColumnIndices(const std::string & condition, std::vector<int> & columnIndices) {
	// TODO: right now this only works for equijoins
	// since this is all that is implemented at the time

	// TODO: for this to work properly we can only do multi column join
	// when we have ands, when we have hors we hvae to perform the joisn seperately then
	// do a unique merge of the indices

	// right now with pred push down the join codnition takes the filters as the second argument to condition

	std::string clean_expression = clean_calcite_expression(condition);
	int operator_count = 0;
	std::stack<std::string> operand;
	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	for(std::string token : tokens) {
		if(is_operator_token(token)) {
			if(token == "=") {
				// so far only equijoins are supported in libgdf
				operator_count++;
			} else if(token != "AND") {
				throw std::runtime_error("In evaluate_join function: unsupported non-equijoins operator");
			}
		} else {
			operand.push(token);
		}
	}

	columnIndices.resize(2 * operator_count);
	for(size_t i = 0; i < operator_count; i++) {
		int right_index = get_index(operand.top());
		operand.pop();
		int left_index = get_index(operand.top());
		operand.pop();

		if(right_index < left_index) {
			std::swap(left_index, right_index);
		}

		columnIndices[2 * i] = left_index;
		columnIndices[2 * i + 1] = right_index;
	}
}

/*
This function will take a join_statement and if it contains anything that is not an equijoin, it will try to break it up into an equijoin (new_join_statement) and a filter (filter_statement)
If its just an equijoin, then the new_join_statement will just be join_statement and filter_statement will be empty

Examples:
Basic case:
join_statement = LogicalJoin(condition=[=($3, $0)], joinType=[inner])
new_join_statement = LogicalJoin(condition=[=($3, $0)], joinType=[inner])
filter_statement = ""

Simple case:
join_statement = LogicalJoin(condition=[AND(=($3, $0), >($5, $2))], joinType=[inner])
new_join_statement = LogicalJoin(condition=[=($3, $0)], joinType=[inner])
filter_statement = LogicalFilter(condition=[>($5, $2)])

Complex case:
join_statement = LogicalJoin(condition=[AND(=($7, $0), OR(AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5)))], joinType=[inner])
new_join_statement = LogicalJoin(condition=[=($7, $0)], joinType=[inner])
filter_statement = LogicalFilter(condition=[OR(AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5))])

Error case:
join_statement = LogicalJoin(condition=[OR(=($7, $0), AND($8, $9, $2, $3), AND($8, $9, $4, $5), AND($8, $9, $6, $5))], joinType=[inner])
Should throw an error

Error case:
join_statement = LogicalJoin(condition=[AND(<($7, $0), >($7, $1)], joinType=[inner])
Should throw an error

*/
void split_inequality_join_into_join_and_filter(const std::string & join_statement, std::string & new_join_statement, std::string & filter_statement){
	new_join_statement = join_statement;
	filter_statement = "";

	std::string condition = get_named_expression(join_statement, "condition");
	condition = replace_calcite_regex(condition);
	std::string join_type = get_named_expression(join_statement, "joinType");

	ral::parser::parse_tree tree;
	tree.build(condition);

	std::string new_join_statement_expression, filter_statement_expression;
	assert(tree.root().type == ral::parser::node_type::OPERATOR or
		tree.root().type == ral::parser::node_type::LITERAL); // condition=[true] (for cross join)

	if (tree.root().value == "=") {
		// this would be a regular single equality join
		new_join_statement_expression = condition;  // the join_out is the same as the original input
		filter_statement_expression = "";					   // no filter out
	} else if (tree.root().value == "AND") {
		int num_equalities = 0;
		for (auto&& c : tree.root().children) {
			if (c->value == "=") {
				num_equalities++;
			}
		}
		if (num_equalities == tree.root().children.size()) {  // all are equalities. this would be a regular multiple equality join
			new_join_statement_expression = condition;  // the join_out is the same as the original input
			filter_statement_expression = "";					   // no filter out
		} else if (num_equalities > 0) {			   // i can split this into an equality join and a filter
			if (num_equalities == 1) {  // if there is only one equality, then the root_ for join_out wont be an AND,
				// and we will just have this equality as the root_
				if (tree.root().children.size() == 2) {
					for (auto&& c : tree.root().children) {
						if (c->value == "=") {
							new_join_statement_expression = ral::parser::detail::rebuild_helper(c.get());
						} else {
							filter_statement_expression = ral::parser::detail::rebuild_helper(c.get());
						}
					}
				} else {
					auto filter_root = std::make_unique<ral::parser::operator_node>("AND");
					for (auto&& c : tree.root().children) {
						if (c->value == "=") {
							new_join_statement_expression = ral::parser::detail::rebuild_helper(c.get());
						} else {
							filter_root->children.push_back(std::unique_ptr<ral::parser::node>(c->clone()));
						}
					}
					filter_statement_expression = ral::parser::detail::rebuild_helper(filter_root.get());
				}
			} else if (num_equalities == tree.root().children.size() - 1) {
				// only one that does not have an inequality and therefore will be
				// in the filter (without an and at the root_)
				auto join_out_root = std::make_unique<ral::parser::operator_node>("AND");
				for (auto&& c : tree.root().children) {
					if (c->value == "=") {
						join_out_root->children.push_back(std::unique_ptr<ral::parser::node>(c->clone()));
					} else {
						filter_statement_expression = ral::parser::detail::rebuild_helper(c.get());
					}
				}
				new_join_statement_expression = ral::parser::detail::rebuild_helper(join_out_root.get());
			} else {
				auto join_out_root = std::make_unique<ral::parser::operator_node>("AND");
				auto filter_root = std::make_unique<ral::parser::operator_node>("AND");
				for (auto&& c : tree.root().children) {
					if (c->value == "=") {
						join_out_root->children.push_back(std::unique_ptr<ral::parser::node>(c->clone()));
					} else {
						filter_root->children.push_back(std::unique_ptr<ral::parser::node>(c->clone()));
					}
				}
				new_join_statement_expression = ral::parser::detail::rebuild_helper(join_out_root.get());
				filter_statement_expression = ral::parser::detail::rebuild_helper(filter_root.get());
			}
		} else {
			RAL_FAIL("Join condition is currently not supported");
		}
	} else if (tree.root().value == "true") { // cross join case
		new_join_statement_expression = "true";
	}
	 else {
		RAL_FAIL("Join condition is currently not supported");
	}

	new_join_statement = "LogicalJoin(condition=[" + new_join_statement_expression + "], joinType=[" + join_type + "])";
	if (filter_statement_expression != ""){
		filter_statement = "LogicalFilter(condition=[" + filter_statement_expression + "])";
	} else {
		filter_statement = "";
	}
}

} // namespace batch
} // namespace ral
