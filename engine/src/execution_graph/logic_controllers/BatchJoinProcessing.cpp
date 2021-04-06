#include <string>
#include "BatchJoinProcessing.h"
#include "ExceptionHandling/BlazingThread.h"
#include "parser/expression_tree.hpp"
#include "CodeTimer.h"
#include <cudf/partitioning.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include "execution_graph/logic_controllers/taskflow/executor.h"
#include "execution_graph/logic_controllers/CPUCacheData.h"

namespace ral {
namespace batch {

std::tuple<std::string, std::string, std::string, std::string> parseExpressionToGetTypeAndCondition(const std::string & expression) {
	std::string modified_expression, condition, filter_statement, join_type, new_join_statement;
	modified_expression = expression;
	StringUtil::findAndReplaceAll(modified_expression, "IS NOT DISTINCT FROM", "IS_NOT_DISTINCT_FROM");
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
			if(token == "=" || token == "IS_NOT_DISTINCT_FROM") {
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
	for(int i = 0; i < operator_count; i++) {
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

cudf::null_equality parseJoinConditionToEqualityTypes(const std::string & condition) {
	// TODO: right now this only works for equijoins
	// since this is all that is implemented at the time

	std::string clean_expression = clean_calcite_expression(condition);
	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);

	std::vector<cudf::null_equality> joinEqualityTypes;

	for(std::string token : tokens) {
		if(is_operator_token(token)) {
			// so far only equijoins are supported in libgdf
			if(token == "="){
				joinEqualityTypes.push_back(cudf::null_equality::UNEQUAL);
			} else if(token == "IS_NOT_DISTINCT_FROM") {
				joinEqualityTypes.push_back(cudf::null_equality::EQUAL);
			} else if(token != "AND") {
				throw std::runtime_error("In evaluate_join function: unsupported non-equijoins operator");
			}
		}
	}

	if(joinEqualityTypes.empty()){
		throw std::runtime_error("In evaluate_join function: unrecognized joining type operators");
	}

	// TODO: There may be cases where there is a mixture of
	// equality operators. We do not support it for now,
	// since that is not supported in cudf.
	// We only rely on the first equality type found.
	// Related issue: https://github.com/BlazingDB/blazingsql/issues/1421

	bool all_types_are_equal = std::all_of(joinEqualityTypes.begin(), joinEqualityTypes.end(), [&](const cudf::null_equality & elem) {return elem == joinEqualityTypes.front();});
	if(!all_types_are_equal){
		throw std::runtime_error("In evaluate_join function: unsupported different equijoins operators");
	}

	return joinEqualityTypes[0];
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

	if (tree.root().value == "=" || tree.root().value == "IS_NOT_DISTINCT_FROM") {
		// this would be a regular single equality join
		new_join_statement_expression = condition;  // the join_out is the same as the original input
		filter_statement_expression = "";					   // no filter out
	} else if (tree.root().value == "AND") {
		size_t num_equalities = 0;
		for (auto&& c : tree.root().children) {
			if (c->value == "=" || c->value == "IS_NOT_DISTINCT_FROM") {
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
						if (c->value == "=" || c->value == "IS_NOT_DISTINCT_FROM") {
							new_join_statement_expression = ral::parser::detail::rebuild_helper(c.get());
						} else {
							filter_statement_expression = ral::parser::detail::rebuild_helper(c.get());
						}
					}
				} else {
					auto filter_root = std::make_unique<ral::parser::operator_node>("AND");
					for (auto&& c : tree.root().children) {
						if (c->value == "=" || c->value == "IS_NOT_DISTINCT_FROM") {
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
					if (c->value == "=" || c->value == "IS_NOT_DISTINCT_FROM") {
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
					if (c->value == "=" || c->value == "IS_NOT_DISTINCT_FROM") {
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

// BEGIN PartwiseJoin

PartwiseJoin::PartwiseJoin(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel{kernel_id, queryString, context, kernel_type::PartwiseJoinKernel} {
	this->query_graph = query_graph;
	this->input_.add_port("input_a", "input_b");

	this->max_left_ind = -1;
	this->max_right_ind = -1;

	ral::cache::cache_settings cache_machine_config;
	cache_machine_config.type = ral::cache::CacheType::SIMPLE;
	cache_machine_config.context = context->clone();

	std::string left_array_cache_name = std::to_string(this->get_id()) + "_left_array";
	this->leftArrayCache = 	ral::cache::create_cache_machine(cache_machine_config, left_array_cache_name);
	std::string right_array_cache_name = std::to_string(this->get_id()) + "_right_array";
	this->rightArrayCache = ral::cache::create_cache_machine(cache_machine_config, right_array_cache_name);

	std::tie(this->expression, this->condition, this->filter_statement, this->join_type) = parseExpressionToGetTypeAndCondition(this->expression);

	if (this->filter_statement != "" && this->join_type != INNER_JOIN){
		throw std::runtime_error("Outer joins with inequalities are not currently supported");
	}
	if (this->join_type == RIGHT_JOIN) {
		throw std::runtime_error("Right Outer Joins are not currently supported");
	}
}

std::unique_ptr<ral::cache::CacheData> PartwiseJoin::load_left_set(){
	this->max_left_ind++;
	auto cache_data = this->left_input->pullCacheData();
	RAL_EXPECTS(cache_data != nullptr, "In PartwiseJoin: The left input cache data cannot be null");

	return cache_data;
}

std::unique_ptr<ral::cache::CacheData> PartwiseJoin::load_right_set(){
	this->max_right_ind++;
	auto cache_data = this->right_input->pullCacheData();
	RAL_EXPECTS(cache_data != nullptr, "In PartwiseJoin: The right input cache data cannot be null");

	return cache_data;
}

void PartwiseJoin::mark_set_completed(int left_ind, int right_ind){
	assert(left_ind >=0 && right_ind >=0 );
	if (completion_matrix.size() <= static_cast<size_t>(left_ind)){
		size_t old_row_size = completion_matrix.size();
		completion_matrix.resize(left_ind + 1);
		if (old_row_size > 0) {
			size_t column_size = completion_matrix[0].size();
			for (size_t i = old_row_size; i < completion_matrix.size(); i++) {
				completion_matrix[i].resize(column_size, false);
			}
		}
	}
	if (completion_matrix[left_ind].size() <= static_cast<size_t>(right_ind)){ // if we need to resize, lets resize the whole matrix, making sure that the default is false
		for (std::size_t i = 0; i < completion_matrix.size(); i++){
			completion_matrix[i].resize(right_ind + 1, false);
		}
	}
	completion_matrix[left_ind][right_ind] = true;
}

// This function checks to see if there is a set from our current completion_matix (data we have already loaded once)
// that we have not completed that uses one of our current indices, otherwise it returns [-1, -1]
std::tuple<int, int> PartwiseJoin::check_for_another_set_to_do_with_data_we_already_have(int left_ind, int right_ind) {
	assert(left_ind == -1 || right_ind == -1);

	auto left_indices = leftArrayCache->get_all_indexes();
	auto right_indices = rightArrayCache->get_all_indexes();

	if (left_ind >= 0 ) {
		assert(left_ind >= completion_matrix.size());
		return {left_ind, (!right_indices.empty() ? right_indices[0] : 0)};
	}

	if (right_ind >= 0 ) {
		assert(right_ind >= completion_matrix[0].size());
		return {(!left_indices.empty() ? left_indices[0] : 0), right_ind};
	}

	for (auto &&i : left_indices) {
		for (auto &&j : right_indices) {
			if (!completion_matrix[i][j]) {
				return {i, j};
			}
		}
	}

	return {-1, -1};
}

// This function returns the first not completed set, otherwise it returns [-1, -1]
std::tuple<int, int> PartwiseJoin::check_for_set_that_has_not_been_completed(){
	for (size_t i = 0; i < completion_matrix.size(); i++){
		for (size_t j = 0; j < completion_matrix[i].size(); j++){
			if (!completion_matrix[i][j]){
				return {i, j};
			}
		}
	}
	return {-1, -1};
}

// this function makes sure that the columns being joined are of the same type so that we can join them properly
void PartwiseJoin::computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const std::vector<cudf::data_type> & right_types){
	std::vector<cudf::data_type> left_join_types, right_join_types;
	for (size_t i = 0; i < this->left_column_indices.size(); i++){
		left_join_types.push_back(left_types[this->left_column_indices[i]]);
		right_join_types.push_back(right_types[this->right_column_indices[i]]);
	}
	bool strict = true;
	this->join_column_common_types = ral::utilities::get_common_types(left_join_types, right_join_types, strict);
	this->normalize_left = !std::equal(this->join_column_common_types.cbegin(), this->join_column_common_types.cend(),
												left_join_types.cbegin(), left_join_types.cend());
	this->normalize_right = !std::equal(this->join_column_common_types.cbegin(), this->join_column_common_types.cend(),
												right_join_types.cbegin(), right_join_types.cend());
}

std::unique_ptr<ral::frame::BlazingTable> PartwiseJoin::join_set(
	const ral::frame::BlazingTableView & table_left,
	const ral::frame::BlazingTableView & table_right)
{
	std::unique_ptr<CudfTable> result_table;

	if (this->join_type == CROSS_JOIN) {
		result_table = cudf::cross_join(
			table_left.view(),
			table_right.view());
	} else {
		bool has_nulls_left = ral::processor::check_if_has_nulls(table_left.view(), left_column_indices);
		bool has_nulls_right = ral::processor::check_if_has_nulls(table_right.view(), right_column_indices);
		if(this->join_type == INNER_JOIN) {
			cudf::null_equality equalityType = parseJoinConditionToEqualityTypes(this->condition);
			result_table = cudf::inner_join(
				table_left.view(),
				table_right.view(),
				this->left_column_indices,
				this->right_column_indices,
				equalityType);
		} else if(this->join_type == LEFT_JOIN) {
			//Removing nulls on right key columns before joining
			std::unique_ptr<CudfTable> table_right_dropna;
			bool has_nulls_right = ral::processor::check_if_has_nulls(table_right.view(), right_column_indices);
			if(has_nulls_right){
				table_right_dropna = cudf::drop_nulls(table_right.view(), right_column_indices);
			}

			result_table = cudf::left_join(
				table_left.view(),
				has_nulls_right ? table_right_dropna->view() : table_right.view(),
				this->left_column_indices,
				this->right_column_indices);
		} else if(this->join_type == OUTER_JOIN) {
			result_table = cudf::full_join(
				table_left.view(),
				table_right.view(),
				this->left_column_indices,
				this->right_column_indices,
				(has_nulls_left && has_nulls_right) ? cudf::null_equality::UNEQUAL : cudf::null_equality::EQUAL);
		} else {
			RAL_FAIL("Unsupported join operator");
		}
	}

	return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), this->result_names);
}

ral::execution::task_result PartwiseJoin::do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputs,
	std::shared_ptr<ral::cache::CacheMachine> /*output*/,
	cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {
	CodeTimer eventTimer;

	auto & left_batch = inputs[0];
	auto & right_batch = inputs[1];


	try{
		if (this->normalize_left){
			ral::utilities::normalize_types(left_batch, this->join_column_common_types, this->left_column_indices);
		}
		if (this->normalize_right){
			ral::utilities::normalize_types(right_batch, this->join_column_common_types, this->right_column_indices);
		}

		auto log_input_num_rows = left_batch->num_rows() + right_batch->num_rows();
		auto log_input_num_bytes = left_batch->sizeInBytes() + right_batch->sizeInBytes();

		std::unique_ptr<ral::frame::BlazingTable> joined = join_set(left_batch->toBlazingTableView(), right_batch->toBlazingTableView());

		auto log_output_num_rows = joined->num_rows();
		auto log_output_num_bytes = joined->sizeInBytes();

		if (filter_statement != "") {
			auto filter_table = ral::processor::process_filter(joined->toBlazingTableView(), filter_statement, this->context.get());
			eventTimer.stop();

			log_output_num_rows = filter_table->num_rows();
			log_output_num_bytes = filter_table->sizeInBytes();

			this->add_to_output_cache(std::move(filter_table));
		} else{
			eventTimer.stop();
			this->add_to_output_cache(std::move(joined));
		}

	}catch(const rmm::bad_alloc& e){
		return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
	}catch(const std::exception& e){
		return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
	}

	try{
		this->leftArrayCache->put(std::stoi(args.at("left_idx")), std::move(left_batch));
		this->rightArrayCache->put(std::stoi(args.at("right_idx")), std::move(right_batch));
	}catch(const rmm::bad_alloc& e){
		//can still recover if the input was not a GPUCacheData
		return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
	}catch(const std::exception& e){
		return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
	}

	return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus PartwiseJoin::run() {
	CodeTimer timer;

	left_input = this->input_.get_cache("input_a");
	right_input = this->input_.get_cache("input_b");

	bool done = false;

	while (!done) {
		std::unique_ptr<ral::cache::CacheData> left_cache_data, right_cache_data;
		int left_ind, right_ind;
		if (max_left_ind == -1 && max_right_ind == -1){
			// before we load anything, lets make sure each side has data to process
			left_input->wait_for_next();
			right_input->wait_for_next();

			left_cache_data = load_left_set();
			right_cache_data = load_right_set();

			left_ind = this->max_left_ind = 0; // we have loaded just once. This is the highest index for now
			right_ind = this->max_right_ind = 0; // we have loaded just once. This is the highest index for now

			// parsing more of the expression here because we need to have the number of columns of the tables
			std::vector<int> column_indices;
			parseJoinConditionToColumnIndices(this->condition, column_indices);
			for(std::size_t i = 0; i < column_indices.size();i++){
				if(column_indices[i] >= static_cast<int>(left_cache_data->num_columns())){
					this->right_column_indices.push_back(column_indices[i] - left_cache_data->num_columns());
				}else{
					this->left_column_indices.push_back(column_indices[i]);
				}
			}

			std::vector<std::string> left_names = left_cache_data->names();
			std::vector<std::string> right_names = right_cache_data->names();
			this->result_names.reserve(left_names.size() + right_names.size());
			this->result_names.insert(this->result_names.end(), left_names.begin(), left_names.end());
			this->result_names.insert(this->result_names.end(), right_names.begin(), right_names.end());

			computeNormalizationData(left_cache_data->get_schema(), right_cache_data->get_schema());
		} else {
			// Not first load, so we have joined a set pair. Now lets see if there is another set pair we can do, but keeping one of the two sides we already have
			std::tie(left_ind, right_ind) = check_for_another_set_to_do_with_data_we_already_have();
			if (left_ind >= 0 && right_ind >= 0) {
				left_cache_data = this->leftArrayCache->get_or_wait_CacheData(left_ind);
				right_cache_data = this->rightArrayCache->get_or_wait_CacheData(right_ind);
			} else {
				if (this->left_input->wait_for_next()){
					left_cache_data = load_left_set();
					left_ind = this->max_left_ind;
				}
				if (this->right_input->wait_for_next()){
					right_cache_data = load_right_set();
					right_ind = this->max_right_ind;
				}
				if (left_ind >= 0 && right_ind >= 0) {
					// We pulled new data from left and right
				} else if (left_ind >= 0)	{
					std::tie(std::ignore, right_ind) = check_for_another_set_to_do_with_data_we_already_have(left_ind, -1);
					right_cache_data = this->rightArrayCache->get_or_wait_CacheData(right_ind);
				} else if (right_ind >= 0)	{
					std::tie(left_ind, std::ignore) = check_for_another_set_to_do_with_data_we_already_have(-1, right_ind);
					left_cache_data = this->leftArrayCache->get_or_wait_CacheData(left_ind);
				} else {
					// Both inputs are finished, check any remaining pairs to process
					std::tie(left_ind, right_ind) = check_for_set_that_has_not_been_completed();
					if (left_ind >= 0 && right_ind >= 0) {
						left_cache_data = this->leftArrayCache->get_or_wait_CacheData(left_ind);
						right_cache_data = this->rightArrayCache->get_or_wait_CacheData(right_ind);
					} else {
						done = true;
					}
				}
			}
		}
		if (!done) {
			std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
			inputs.push_back(std::move(left_cache_data));
			inputs.push_back(std::move(right_cache_data));

			ral::execution::executor::get_instance()->add_task(
									std::move(inputs),
									this->output_cache(),
									this,
									{{"left_idx", std::to_string(left_ind)}, {"right_idx", std::to_string(right_ind)}});

			mark_set_completed(left_ind, right_ind);
		}
	}

	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                                        "query_id"_a=context->getContextToken(),
                                                        "step"_a=context->getQueryStep(),
                                                        "substep"_a=context->getQuerySubstep(),
                                                        "info"_a="Compute Aggregate Kernel tasks created",
                                                        "duration"_a=timer.elapsed_time(),
                                                        "kernel_id"_a=this->get_id());
	}

	std::unique_lock<std::mutex> lock(kernel_mutex);
	kernel_cv.wait(lock,[this]{
			return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
	});

	if(auto ep = ral::execution::executor::get_instance()->last_exception()){
		std::rethrow_exception(ep);
	}

	if(logger) {
		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="PartwiseJoin Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());
	}

	// these are intra kernel caches. We want to make sure they are empty before we finish.
	this->leftArrayCache->clear();
	this->rightArrayCache->clear();

	return kstatus::proceed;
}

std::string PartwiseJoin::get_join_type() {
	return join_type;
}

// END PartwiseJoin

// BEGIN JoinPartitionKernel

JoinPartitionKernel::JoinPartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: distributing_kernel{kernel_id, queryString, context, kernel_type::JoinPartitionKernel} {
	this->query_graph = query_graph;
	set_number_of_message_trackers(2); //default for left and right partitions

	this->input_.add_port("input_a", "input_b");
	this->output_.add_port("output_a", "output_b");

	std::tie(this->expression, this->condition, this->filter_statement, this->join_type) = parseExpressionToGetTypeAndCondition(this->expression);
}

// this function makes sure that the columns being joined are of the same type so that we can join them properly
void JoinPartitionKernel::computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const	std::vector<cudf::data_type> & right_types){
	std::vector<cudf::data_type> left_join_types, right_join_types;
	for (size_t i = 0; i < this->left_column_indices.size(); i++){
		left_join_types.push_back(left_types[this->left_column_indices[i]]);
		right_join_types.push_back(right_types[this->right_column_indices[i]]);
	}
	bool strict = true;
	this->join_column_common_types = ral::utilities::get_common_types(left_join_types, right_join_types, strict);
	this->normalize_left = !std::equal(this->join_column_common_types.cbegin(), this->join_column_common_types.cend(),
												left_join_types.cbegin(), left_join_types.cend());
	this->normalize_right = !std::equal(this->join_column_common_types.cbegin(), this->join_column_common_types.cend(),
												right_join_types.cbegin(), right_join_types.cend());
}

std::pair<bool, bool> JoinPartitionKernel::determine_if_we_are_scattering_a_small_table(const ral::cache::CacheData& left_cache_data,
	const ral::cache::CacheData& right_cache_data){

    if(logger){
        logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="determine_if_we_are_scattering_a_small_table start",
                                    "duration"_a="",
                                    "kernel_id"_a=this->get_id());
    }

	std::pair<bool, uint64_t> left_num_rows_estimate = this->query_graph->get_estimated_input_rows_to_cache(this->kernel_id, "input_a");
	if(logger){
        logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a=left_num_rows_estimate.first ? "left_num_rows_estimate was valid" : "left_num_rows_estimate was invalid",
                                    "duration"_a="",
                                    "kernel_id"_a=this->get_id(),
                                    "rows"_a=left_num_rows_estimate.second);
	}

	std::pair<bool, uint64_t> right_num_rows_estimate = this->query_graph->get_estimated_input_rows_to_cache(this->kernel_id, "input_b");
	if(logger){
        logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a=right_num_rows_estimate.first ? "right_num_rows_estimate was valid" : "right_num_rows_estimate was invalid",
                                    "duration"_a="",
                                    "kernel_id"_a=this->get_id(),
                                    "rows"_a=right_num_rows_estimate.second);
	}

	double left_batch_rows = (double)left_cache_data.num_rows();
	double left_batch_bytes = (double)left_cache_data.sizeInBytes();
	double right_batch_rows = (double)right_cache_data.num_rows();
	double right_batch_bytes = (double)right_cache_data.sizeInBytes();
	int64_t left_bytes_estimate;
	if (!left_num_rows_estimate.first){
		// if we cant get a good estimate of current bytes, then we will set to -1 to signify that
		left_bytes_estimate = -1;
	} else {
		left_bytes_estimate = left_batch_rows == 0 ? 0 : (int64_t)(left_batch_bytes*(((double)left_num_rows_estimate.second)/left_batch_rows));
	}
	int64_t right_bytes_estimate;
	if (!right_num_rows_estimate.first){
		// if we cant get a good estimate of current bytes, then we will set to -1 to signify that
		right_bytes_estimate = -1;
	} else {
		right_bytes_estimate = right_batch_rows == 0 ? 0 : (int64_t)(right_batch_bytes*(((double)right_num_rows_estimate.second)/right_batch_rows));
	}

	context->incrementQuerySubstep();

	auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
	int self_node_idx = context->getNodeIndex(self_node);
	auto nodes_to_send = context->getAllOtherNodes(self_node_idx);

	ral::cache::MetadataDictionary extra_metadata;
	extra_metadata.add_value(ral::cache::JOIN_LEFT_BYTES_METADATA_LABEL, std::to_string(left_bytes_estimate));
	extra_metadata.add_value(ral::cache::JOIN_RIGHT_BYTES_METADATA_LABEL, std::to_string(right_bytes_estimate));

	std::vector<std::string> determination_messages_to_wait_for;
	std::vector<std::string> target_ids;
	for (auto & node_to_send : nodes_to_send) {
		target_ids.push_back(node_to_send.id());
		determination_messages_to_wait_for.push_back(
			"determine_if_we_are_scattering_a_small_table_" + std::to_string(this->context->getContextToken()) + "_" +	std::to_string(this->get_id()) +	"_" +	node_to_send.id());
	}
	send_message(nullptr,
			false, //specific_cache
			"", //cache_id
			target_ids, //target_ids
			"determine_if_we_are_scattering_a_small_table_", //message_id_prefix
			true, //always_add
			false, //wait_for
			0, //message_tracker_idx
			extra_metadata);

	if(logger){
        logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="determine_if_we_are_scattering_a_small_table about to collectLeftRightTableSizeBytes",
                                    "duration"_a="",
                                    "kernel_id"_a=this->get_id());
	}

	std::vector<int64_t> nodes_num_bytes_left(this->context->getTotalNodes());
	std::vector<int64_t> nodes_num_bytes_right(this->context->getTotalNodes());

	for (auto & message_id : determination_messages_to_wait_for) {
		auto message = this->query_graph->get_input_message_cache()->pullCacheData(message_id);
		auto *message_with_metadata = dynamic_cast<ral::cache::CPUCacheData*>(message.get());
		int node_idx = context->getNodeIndex(context->getNode(message_with_metadata->getMetadata().get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]));
		nodes_num_bytes_left[node_idx] = std::stoll(message_with_metadata->getMetadata().get_values()[ral::cache::JOIN_LEFT_BYTES_METADATA_LABEL]);
		nodes_num_bytes_right[node_idx] = std::stoll(message_with_metadata->getMetadata().get_values()[ral::cache::JOIN_RIGHT_BYTES_METADATA_LABEL]);
	}
	nodes_num_bytes_left[self_node_idx] = left_bytes_estimate;
	nodes_num_bytes_right[self_node_idx] = right_bytes_estimate;
	std::string collectLeftRightTableSizeBytesInfo = "nodes_num_bytes_left: ";
	for (auto num_bytes : nodes_num_bytes_left){
		collectLeftRightTableSizeBytesInfo += std::to_string(num_bytes) + ", ";
	}
	collectLeftRightTableSizeBytesInfo += "; nodes_num_bytes_right: ";
	for (auto num_bytes : nodes_num_bytes_right){
		collectLeftRightTableSizeBytesInfo += std::to_string(num_bytes) + ", ";
	}
	if(logger){
        logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="determine_if_we_are_scattering_a_small_table collected " + collectLeftRightTableSizeBytesInfo,
                                    "duration"_a="",
                                    "kernel_id"_a=this->get_id());
	}

	bool any_unknowns_left = std::any_of(nodes_num_bytes_left.begin(), nodes_num_bytes_left.end(), [](int64_t bytes){return bytes == -1;});
	bool any_unknowns_right = std::any_of(nodes_num_bytes_right.begin(), nodes_num_bytes_right.end(), [](int64_t bytes){return bytes == -1;});

	int64_t total_bytes_left = std::accumulate(nodes_num_bytes_left.begin(), nodes_num_bytes_left.end(), int64_t(0));
	int64_t total_bytes_right = std::accumulate(nodes_num_bytes_right.begin(), nodes_num_bytes_right.end(), int64_t(0));

	bool scatter_left = false;
	bool scatter_right = false;
	if (any_unknowns_left || any_unknowns_right){
		// with CROSS_JOIN we want to scatter or or the other, no matter what, even with unknowns
		if (this->join_type == CROSS_JOIN){
			if(total_bytes_left < total_bytes_right) {
				scatter_left = true;
			} else {
				scatter_right = true;
			}
			return std::make_pair(scatter_left, scatter_right);
		} else {
			return std::make_pair(false, false); // we wont do any small table scatter if we have unknowns
		}
	}

	int num_nodes = context->getTotalNodes();

	int64_t estimate_regular_distribution = (total_bytes_left + total_bytes_right) * (num_nodes - 1) / num_nodes;
	int64_t estimate_scatter_left = (total_bytes_left) * (num_nodes - 1);
	int64_t estimate_scatter_right = (total_bytes_right) * (num_nodes - 1);

	unsigned long long max_join_scatter_mem_overhead = 500000000;  // 500Mb  how much extra memory consumption per node are we ok with
	std::map<std::string, std::string> config_options = context->getConfigOptions();
	auto it = config_options.find("MAX_JOIN_SCATTER_MEM_OVERHEAD");
	if (it != config_options.end()){
		max_join_scatter_mem_overhead = std::stoull(config_options["MAX_JOIN_SCATTER_MEM_OVERHEAD"]);
	}

	// with CROSS_JOIN we want to scatter or or the other
	if (this->join_type == CROSS_JOIN){
		if(estimate_scatter_left < estimate_scatter_right) {
			scatter_left = true;
		} else {
			scatter_right = true;
		}
	// with LEFT_JOIN we cant scatter the left side
	} else if (this->join_type == LEFT_JOIN) {
		if(estimate_scatter_right < estimate_regular_distribution &&
					static_cast<unsigned long long>(total_bytes_right) < max_join_scatter_mem_overhead) {
			scatter_right = true;
		}
	} else {
		if(estimate_scatter_left < estimate_regular_distribution ||
			estimate_scatter_right < estimate_regular_distribution) {
			if(estimate_scatter_left < estimate_scatter_right &&
				static_cast<unsigned long long>(total_bytes_left) < max_join_scatter_mem_overhead) {
				scatter_left = true;
			} else if(estimate_scatter_right < estimate_scatter_left &&
					static_cast<unsigned long long>(total_bytes_right) < max_join_scatter_mem_overhead) {
				scatter_right = true;
			}
		}
	}
	return std::make_pair(scatter_left, scatter_right);
}

void JoinPartitionKernel::perform_standard_hash_partitioning(
	std::unique_ptr<ral::cache::CacheData> left_cache_data,
	std::unique_ptr<ral::cache::CacheData> right_cache_data,
	std::shared_ptr<ral::cache::CacheMachine> left_input,
	std::shared_ptr<ral::cache::CacheMachine> right_input){

	this->context->incrementQuerySubstep();

	// parsing more of the expression here because we need to have the number of columns of the tables
	std::vector<int> column_indices;
	parseJoinConditionToColumnIndices(condition, column_indices);
	for(std::size_t i = 0; i < column_indices.size();i++){
		if(column_indices[i] >= static_cast<int>(left_cache_data->num_columns())){
			this->right_column_indices.push_back(column_indices[i] - left_cache_data->num_columns());
		}else{
			this->left_column_indices.push_back(column_indices[i]);
		}
	}

	computeNormalizationData(left_cache_data->get_schema(), right_cache_data->get_schema());

	BlazingThread left_thread([this, &left_input, &left_cache_data](){
		while(left_cache_data != nullptr) {
			std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
			inputs.push_back(std::move(left_cache_data));

			ral::execution::executor::get_instance()->add_task(
										std::move(inputs),
										this->output_cache("output_a"),
										this,
										{{"operation_type", "hash_partition"}, {"side", "left"}});

			left_cache_data = left_input->pullCacheData();
		}
	});

	BlazingThread right_thread([this, &right_input, &right_cache_data](){
		while(right_cache_data != nullptr) {
			std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
			inputs.push_back(std::move(right_cache_data));

			ral::execution::executor::get_instance()->add_task(
										std::move(inputs),
										this->output_cache("output_b"),
										this,
										{{"operation_type", "hash_partition"}, {"side", "right"}});

			right_cache_data = right_input->pullCacheData();
		}
	});

	left_thread.join();
	right_thread.join();

	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="JoinPartitionKernel Kernel tasks created",
                                "kernel_id"_a=this->get_id());
    }

	std::unique_lock<std::mutex> lock(kernel_mutex);
	kernel_cv.wait(lock,[this]{
		return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
	});

	if(auto ep = ral::execution::executor::get_instance()->last_exception()){
		std::rethrow_exception(ep);
	}

	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="JoinPartitionKernel Kernel tasks executed",
                                "kernel_id"_a=this->get_id());
    }

	send_total_partition_counts("", "output_a", LEFT_TABLE_IDX);
	send_total_partition_counts("", "output_b", RIGHT_TABLE_IDX);

	int total_count_left = get_total_partition_counts(LEFT_TABLE_IDX); //left

	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="JoinPartitionKernel Kernel got total_partition_counts left: " + std::to_string(total_count_left),
                                "kernel_id"_a=this->get_id());
    }
	this->output_.get_cache("output_a")->wait_for_count(total_count_left);

	int total_count_right = get_total_partition_counts(RIGHT_TABLE_IDX); //right
	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="JoinPartitionKernel Kernel got total_partition_counts right " + std::to_string(total_count_right),
                                "kernel_id"_a=this->get_id());
    }
	this->output_.get_cache("output_b")->wait_for_count(total_count_right);
}

void JoinPartitionKernel::small_table_scatter_distribution(std::unique_ptr<ral::cache::CacheData> small_cache_data,
	std::unique_ptr<ral::cache::CacheData> big_cache_data,
	std::shared_ptr<ral::cache::CacheMachine> small_input,
	std::shared_ptr<ral::cache::CacheMachine> big_input){

	this->context->incrementQuerySubstep();

	// In this function we are assuming that one and only one of the two bools in scatter_left_right is true
	assert((scatter_left_right.first || scatter_left_right.second) && not (scatter_left_right.first && scatter_left_right.second));

	std::string small_output_cache_name = scatter_left_right.first ? "output_a" : "output_b";
	int small_table_idx = scatter_left_right.first ? LEFT_TABLE_IDX : RIGHT_TABLE_IDX;
	std::string big_output_cache_name = scatter_left_right.first ? "output_b" : "output_a";
	int big_table_idx = scatter_left_right.first ? RIGHT_TABLE_IDX : LEFT_TABLE_IDX;

	BlazingThread left_thread([this, &small_input, &small_cache_data, small_output_cache_name](){
		while(small_cache_data != nullptr ) {
			std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
			inputs.push_back(std::move(small_cache_data));

			ral::execution::executor::get_instance()->add_task(
							std::move(inputs),
							this->output_cache(small_output_cache_name),
							this,
							{{"operation_type", "small_table_scatter"}});

			small_cache_data = small_input->pullCacheData();
		}
	});

	BlazingThread right_thread([this, &big_input, &big_cache_data, big_output_cache_name, big_table_idx](){

		while (big_cache_data != nullptr) {
			
			bool added = this->add_to_output_cache(std::move(big_cache_data), big_output_cache_name);
			if (added) {
				auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
				increment_node_count(self_node.id(), big_table_idx);
			}

			big_cache_data = big_input->pullCacheData();
		}
	});

	left_thread.join();
	right_thread.join();

	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="JoinPartitionKernel Kernel tasks created",
                                "kernel_id"_a=this->get_id());
    }

	std::unique_lock<std::mutex> lock(kernel_mutex);
	kernel_cv.wait(lock,[this]{
		return this->tasks.empty() || ral::execution::executor::get_instance()->has_exception();
	});

	if(auto ep = ral::execution::executor::get_instance()->last_exception()){
		std::rethrow_exception(ep);
	}

	if(logger) {
        logger->debug("{query_id}|{step}|{substep}|{info}||kernel_id|{kernel_id}||",
                                "query_id"_a=context->getContextToken(),
                                "step"_a=context->getQueryStep(),
                                "substep"_a=context->getQuerySubstep(),
                                "info"_a="JoinPartitionKernel Kernel tasks executed",
                                "kernel_id"_a=this->get_id());
    }

	send_total_partition_counts(
		"", //message_prefix
		small_output_cache_name, //cache_id
		small_table_idx //message_tracker_idx
	);

	int total_count = get_total_partition_counts(small_table_idx);

	this->output_cache(small_output_cache_name)->wait_for_count(total_count);	
}

ral::execution::task_result JoinPartitionKernel::do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputs,
	std::shared_ptr<ral::cache::CacheMachine> /*output*/,
	cudaStream_t /*stream*/, const std::map<std::string, std::string>& args) {
	bool input_consumed = false;
	try{
		auto& operation_type = args.at("operation_type");
		auto & input = inputs[0];
		if (operation_type == "small_table_scatter") {
			input_consumed = true;
			std::string small_output_cache_name = scatter_left_right.first ? "output_a" : "output_b";
			int small_table_idx = scatter_left_right.first ? LEFT_TABLE_IDX : RIGHT_TABLE_IDX;

			broadcast(std::move(input),
				this->output_.get_cache(small_output_cache_name).get(),
				"", //message_id_prefix
				small_output_cache_name, //cache_id
				small_table_idx //message_tracker_idx
			);
		} else if (operation_type == "hash_partition") {
			bool normalize_types;
			int table_idx;
			std::string cache_id;
			std::vector<cudf::size_type> column_indices;
			if(args.at("side") == "left"){
				normalize_types = this->normalize_left;
				table_idx = LEFT_TABLE_IDX;
				cache_id = "output_a";
				column_indices = this->left_column_indices;
			} else {
				normalize_types = this->normalize_right;
				table_idx = RIGHT_TABLE_IDX;
				cache_id = "output_b";
				column_indices = this->right_column_indices;
			}

			if (normalize_types) {
				ral::utilities::normalize_types(input, join_column_common_types, column_indices);
			}

			auto batch_view = input->view();
			std::unique_ptr<cudf::table> hashed_data;
			std::vector<cudf::table_view> partitioned;
			if (input->num_rows() > 0) {
				// When is cross_join. `column_indices` is equal to 0, so we need all `batch` columns to apply cudf::hash_partition correctly
				if (column_indices.size() == 0) {
					column_indices.resize(input->num_columns());
					std::iota(std::begin(column_indices), std::end(column_indices), 0);
				}

				int num_partitions = context->getTotalNodes();
				std::vector<cudf::size_type> hased_data_offsets;
				std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch_view, column_indices, num_partitions);
				assert(hased_data_offsets.begin() != hased_data_offsets.end());

				// the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
				std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
				partitioned = cudf::split(hashed_data->view(), split_indexes);
			} else {
				for(int i = 0; i < context->getTotalNodes(); i++){
					partitioned.push_back(batch_view);
				}
			}

			std::vector<ral::frame::BlazingTableView> partitions;
			for(auto partition : partitioned) {
				partitions.push_back(ral::frame::BlazingTableView(partition, input->names()));
			}

			scatter(partitions,
				this->output_.get_cache(cache_id).get(),
				"", //message_id_prefix
				cache_id, //cache_id
				table_idx  //message_tracker_idx
			);
		} else { // not an option! error
			if (logger) {
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In JoinPartitionKernel::do_process Invalid operation_type: {}"_format(operation_type),
											"duration"_a="");
			}

			return {ral::execution::task_status::FAIL, std::string("In JoinPartitionKernel::do_process Invalid operation_type"), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
		}
	}catch(const rmm::bad_alloc& e){
		return {ral::execution::task_status::RETRY, std::string(e.what()), input_consumed ? std::vector< std::unique_ptr<ral::frame::BlazingTable> > () : std::move(inputs)};
	}catch(const std::exception& e){
		return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
	}
	return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus JoinPartitionKernel::run() {
	CodeTimer timer;

	auto left_input = this->input_.get_cache("input_a");
	auto right_input = this->input_.get_cache("input_b");

	auto left_cache_data = left_input->pullCacheData();
	auto right_cache_data = right_input->pullCacheData();

	if (left_cache_data == nullptr || left_cache_data->num_columns() == 0){
		while (left_input->wait_for_next()){
			left_cache_data = left_input->pullCacheData();
			if (left_cache_data != nullptr && left_cache_data->num_columns() > 0){
				break;
			}
		}
	}
	if (left_cache_data == nullptr || left_cache_data->num_columns() == 0){
		RAL_FAIL("In JoinPartitionKernel left side is empty and cannot determine join column indices");
	}

	if (this->join_type != OUTER_JOIN){
		// can't scatter a full outer join
		scatter_left_right = determine_if_we_are_scattering_a_small_table(*left_cache_data, *right_cache_data);
	}

	if (scatter_left_right.first) {
	    if(logger){
            logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="JoinPartition Scattering left table",
                                        "duration"_a="",
                                        "kernel_id"_a=this->get_id());
	    }

		small_table_scatter_distribution(std::move(left_cache_data), std::move(right_cache_data),
					left_input, right_input);
	} else if (scatter_left_right.second) {
	    if(logger){
            logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="JoinPartition Scattering right table",
                                        "duration"_a="",
                                        "kernel_id"_a=this->get_id());
	    }

		small_table_scatter_distribution(std::move(right_cache_data), std::move(left_cache_data),
					right_input, left_input);
	} else {
	    if(logger){
            logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="JoinPartition Standard hash partition",
                                        "duration"_a="",
                                        "kernel_id"_a=this->get_id());
	    }

		perform_standard_hash_partitioning(std::move(left_cache_data), std::move(right_cache_data),
			left_input, right_input);
	}

	if(logger){
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="JoinPartition Kernel Completed",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
	}

	return kstatus::proceed;
}

std::string JoinPartitionKernel::get_join_type() {
	return join_type;
}

// END JoinPartitionKernel

} // namespace batch
} // namespace ral
