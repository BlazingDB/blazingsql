#include <string>
#include "BatchJoinProcessing.h"
#include "ExceptionHandling/BlazingThread.h"
#include "parser/expression_tree.hpp"
#include "CodeTimer.h"
#include <cudf/partitioning.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>

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

// BEGIN PartwiseJoin

PartwiseJoin::PartwiseJoin(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel{kernel_id, queryString, context, kernel_type::PartwiseJoinKernel}, left_sequence{nullptr, this}, right_sequence{nullptr, this} {
	this->query_graph = query_graph;
	this->input_.add_port("input_a", "input_b");

	this->max_left_ind = -1;
	this->max_right_ind = -1;

	this->left_sequence.set_source(this->input_.get_cache("input_a"));
	this->right_sequence.set_source(this->input_.get_cache("input_b"));

	ral::cache::cache_settings cache_machine_config;
	cache_machine_config.type = ral::cache::CacheType::SIMPLE;
	cache_machine_config.context = context->clone();

	this->leftArrayCache = 	ral::cache::create_cache_machine(cache_machine_config);
	this->rightArrayCache = ral::cache::create_cache_machine(cache_machine_config);

	std::tie(this->expression, this->condition, this->filter_statement, this->join_type) = parseExpressionToGetTypeAndCondition(this->expression);
}

std::unique_ptr<ral::frame::BlazingTable> PartwiseJoin::load_left_set(){
	this->max_left_ind++;
	std::unique_ptr<ral::frame::BlazingTable> table = this->left_sequence.next();
	if (not left_schema && table != nullptr) {
		left_schema = std::make_unique<TableSchema>(table->get_schema(),  table->names());
	}
	if (table == nullptr) {
		return ral::frame::createEmptyBlazingTable(left_schema->column_types, left_schema->column_names);
	}
	return std::move(table);
}

std::unique_ptr<ral::frame::BlazingTable> PartwiseJoin::load_right_set(){
	this->max_right_ind++;
	std::unique_ptr<ral::frame::BlazingTable> table = this->right_sequence.next();
	if (not right_schema && table != nullptr) {
		right_schema = std::make_unique<TableSchema>(table->get_schema(),  table->names());
	}
	if (table == nullptr) {
		return ral::frame::createEmptyBlazingTable(right_schema->column_types, right_schema->column_names);
	}
	return std::move(table);
}

void PartwiseJoin::mark_set_completed(int left_ind, int right_ind){
	if (completion_matrix.size() <= left_ind){
		size_t old_row_size = completion_matrix.size();
		completion_matrix.resize(left_ind + 1);
		if (old_row_size > 0) {
			size_t column_size = completion_matrix[0].size();
			for (size_t i = old_row_size; i < completion_matrix.size(); i++) {
				completion_matrix[i].resize(column_size, false);
			}
		}
	}
	if (completion_matrix[left_ind].size() <= right_ind){ // if we need to resize, lets resize the whole matrix, making sure that the default is false
		for (std::size_t i = 0; i < completion_matrix.size(); i++){
			completion_matrix[i].resize(right_ind + 1, false);
		}
	}
	completion_matrix[left_ind][right_ind] = true;
}

// This function checks to see if there is a set from our current completion_matix (data we have already loaded once)
// that we have not completed that uses one of our current indices, otherwise it returns [-1, -1]
std::tuple<int, int> PartwiseJoin::check_for_another_set_to_do_with_data_we_already_have(int left_ind, int right_ind){
	if (completion_matrix.size() > left_ind && completion_matrix[left_ind].size() > right_ind){
		// left check first keeping the left_ind
		for (std::size_t i = 0; i < completion_matrix[left_ind].size(); i++){
			if (i != right_ind && !completion_matrix[left_ind][i]){
				return std::make_tuple(left_ind, (int)i);
			}
		}
		// now lets check keeping the right_ind
		for (std::size_t i = 0; i < completion_matrix.size(); i++){
			if (i != left_ind && !completion_matrix[i][right_ind]){
				return std::make_tuple((int)i, right_ind);
			}
		}
		return std::make_tuple(-1, -1);
	} else {
		return std::make_tuple(-1, -1);
	}
}

// This function returns the first not completed set, otherwise it returns [-1, -1]
std::tuple<int, int> PartwiseJoin::check_for_set_that_has_not_been_completed(){
	for (int i = 0; i < completion_matrix.size(); i++){
		for (int j = 0; j < completion_matrix[i].size(); j++){
			if (!completion_matrix[i][j]){
				return std::make_tuple(i, j);
			}
		}
	}
	return std::make_tuple(-1, -1);
}

// This function makes sure that the columns being joined are of the same type so that we can join them properly
void PartwiseJoin::computeNormalizationData(const std::vector<cudf::data_type> & left_types, const std::vector<cudf::data_type> & right_types){
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
	std::vector<std::pair<cudf::size_type, cudf::size_type>> columns_in_common;

	if (this->join_type == CROSS_JOIN) {
		result_table = cudf::cross_join(
			table_left.view(),
			table_right.view());
	} else {
		if(this->join_type == INNER_JOIN) {
			//Removing nulls on key columns before joining
			std::unique_ptr<CudfTable> table_left_dropna;
			std::unique_ptr<CudfTable> table_right_dropna;
			bool has_nulls_left = ral::processor::check_if_has_nulls(table_left.view(), left_column_indices);
			bool has_nulls_right = ral::processor::check_if_has_nulls(table_right.view(), right_column_indices);
			if(has_nulls_left){
				table_left_dropna = cudf::drop_nulls(table_left.view(), left_column_indices);
			}
			if(has_nulls_right){
				table_right_dropna = cudf::drop_nulls(table_right.view(), right_column_indices);
			}

			result_table = cudf::inner_join(
				has_nulls_left ? table_left_dropna->view() : table_left.view(),
				has_nulls_right ? table_right_dropna->view() : table_right.view(),
				this->left_column_indices,
				this->right_column_indices,
				columns_in_common);

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
				this->right_column_indices,
				columns_in_common);
		} else if(this->join_type == OUTER_JOIN) {
			result_table = cudf::full_join(
				table_left.view(),
				table_right.view(),
				this->left_column_indices,
				this->right_column_indices,
				columns_in_common);
		} else {
			RAL_FAIL("Unsupported join operator");
		}
	}

	return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), this->result_names);
}

kstatus PartwiseJoin::run() {
	CodeTimer timer;

	bool ordered = false;
	this->left_sequence = BatchSequence(this->input_.get_cache("input_a"), this, ordered);
	this->right_sequence = BatchSequence(this->input_.get_cache("input_b"), this, ordered);

	std::unique_ptr<ral::frame::BlazingTable> left_batch = nullptr;
	std::unique_ptr<ral::frame::BlazingTable> right_batch = nullptr;
	bool done = false;
	bool produced_output = false;
	int left_ind = 0;
	int right_ind = 0;

	while (!done) {
		try {
			if (left_batch == nullptr && right_batch == nullptr){ // first load

				// before we load anything, lets make sure each side has data to process
				this->left_sequence.wait_for_next();
				this->right_sequence.wait_for_next();

				left_batch = load_left_set();
				right_batch = load_right_set();
				this->max_left_ind = 0; // we have loaded just once. This is the highest index for now
				this->max_right_ind = 0; // we have loaded just once. This is the highest index for now

				// parsing more of the expression here because we need to have the number of columns of the tables
				std::vector<int> column_indices;
				parseJoinConditionToColumnIndices(this->condition, column_indices);
				for(int i = 0; i < column_indices.size();i++){
					if(column_indices[i] >= left_batch->num_columns()){
						this->right_column_indices.push_back(column_indices[i] - left_batch->num_columns());
					}else{
						this->left_column_indices.push_back(column_indices[i]);
					}
				}
				std::vector<std::string> left_names = left_batch->names();
				std::vector<std::string> right_names = right_batch->names();
				this->result_names.reserve(left_names.size() + right_names.size());
				this->result_names.insert(this->result_names.end(), left_names.begin(), left_names.end());
				this->result_names.insert(this->result_names.end(), right_names.begin(), right_names.end());

				computeNormalizationData(left_batch->get_schema(), right_batch->get_schema());

			} else { // Not first load, so we have joined a set pair. Now lets see if there is another set pair we can do, but keeping one of the two sides we already have

				int new_left_ind, new_right_ind;
				std::tie(new_left_ind, new_right_ind) = check_for_another_set_to_do_with_data_we_already_have(left_ind, right_ind);
				if (new_left_ind >= 0 || new_right_ind >= 0) {
					if (new_left_ind != left_ind) { // if we are switching out left
						this->leftArrayCache->put(left_ind, std::move(left_batch));
						left_ind = new_left_ind;
						left_batch = this->leftArrayCache->get_or_wait(left_ind);
					} else { // if we are switching out right
						this->rightArrayCache->put(right_ind, std::move(right_batch));
						right_ind = new_right_ind;
						right_batch = this->rightArrayCache->get_or_wait(right_ind);
					}
				} else {
					// lets try first to just grab the next one that is already available and waiting, but we keep one of the two sides we already have
					if (this->left_sequence.has_next_now()){
						this->leftArrayCache->put(left_ind, std::move(left_batch));
						left_batch = load_left_set();
						left_ind = this->max_left_ind;
					} else if (this->right_sequence.has_next_now()){
						this->rightArrayCache->put(right_ind, std::move(right_batch));
						right_batch = load_right_set();
						right_ind = this->max_right_ind;
					} else {
						// lets see if there are any in are matrix that have not been completed
						std::tie(new_left_ind, new_right_ind) = check_for_set_that_has_not_been_completed();
						if (new_left_ind >= 0 && new_right_ind >= 0) {
							this->leftArrayCache->put(left_ind, std::move(left_batch));
							left_ind = new_left_ind;
							left_batch = this->leftArrayCache->get_or_wait(left_ind);
							this->rightArrayCache->put(right_ind, std::move(right_batch));
							right_ind = new_right_ind;
							right_batch = this->rightArrayCache->get_or_wait(right_ind);
						} else {
							// nothing else for us to do buy wait and see if there are any left to do
							if (this->left_sequence.wait_for_next()){
								this->leftArrayCache->put(left_ind, std::move(left_batch));
								left_batch = load_left_set();
								left_ind = this->max_left_ind;
							} else if (this->right_sequence.wait_for_next()){
								this->rightArrayCache->put(right_ind, std::move(right_batch));
								right_batch = load_right_set();
								right_ind = this->max_right_ind;
							} else {
								done = true;
							}
						}
					}
				}
			}
			if (!done) {
				CodeTimer eventTimer(false);
				eventTimer.start();

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

				produced_output = true;
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

				events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
							"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
							"query_id"_a=context->getContextToken(),
							"kernel_id"_a=this->get_id(),
							"input_num_rows"_a=log_input_num_rows,
							"input_num_bytes"_a=log_input_num_bytes,
							"output_num_rows"_a=log_output_num_rows,
							"output_num_bytes"_a=log_output_num_bytes,
							"event_type"_a="compute",
							"timestamp_begin"_a=eventTimer.start_time(),
							"timestamp_end"_a=eventTimer.end_time());

				mark_set_completed(left_ind, right_ind);
			}

		} catch(const std::exception& e) {
			// TODO add retry here
			logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
										"query_id"_a=context->getContextToken(),
										"step"_a=context->getQueryStep(),
										"substep"_a=context->getQuerySubstep(),
										"info"_a="In PartwiseJoin kernel left_idx[{}] right_ind[{}] for {}. What: {}"_format(left_ind, right_ind, expression, e.what()),
										"duration"_a="");
			throw;
		}
	}

	if (!produced_output){
		logger->warn("{query_id}|{step}|{substep}|{info}|{duration}||||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="PartwiseJoin kernel did not produce an output",
									"duration"_a="");
		// WSM TODO put an empty output into output cache
	}

	logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="PartwiseJoin Kernel Completed",
								"duration"_a=timer.elapsed_time(),
								"kernel_id"_a=this->get_id());

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

void JoinPartitionKernel::partition_table(const std::string & kernel_id,
			Context* local_context,
			std::vector<cudf::size_type> column_indices,
			std::unique_ptr<ral::frame::BlazingTable> batch,
			BatchSequence & sequence,
			bool normalize_types,
			const std::vector<cudf::data_type> & join_column_common_types,
			ral::cache::CacheMachine* output,
			const std::string & cache_id,
			spdlog::logger* logger,
			int table_idx)
{
	bool done = false;
	// num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future.
	// If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
	int num_partitions = local_context->getTotalNodes();
	std::unique_ptr<CudfTable> hashed_data;
	std::vector<cudf::size_type> hased_data_offsets;
	int batch_count = 0;
	auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
	while (!done) {
		try {
			if (normalize_types) {
				ral::utilities::normalize_types(batch, join_column_common_types, column_indices);
			}

			auto batch_view = batch->view();
			std::vector<CudfTableView> partitioned;
			if (batch->num_rows() > 0) {
				// When is cross_join. `column_indices` is equal to 0, so we need all `batch` columns to apply cudf::hash_partition correctly
				if (column_indices.size() == 0) {
					column_indices.resize(batch->num_columns());
					std::iota(std::begin(column_indices), std::end(column_indices), 0);
				}

				std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch_view, column_indices, num_partitions);

				assert(hased_data_offsets.begin() != hased_data_offsets.end());
				// the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
				std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
				partitioned = cudf::split(hashed_data->view(), split_indexes);
			} else {
				for(int nodeIndex = 0; nodeIndex < local_context->getTotalNodes(); nodeIndex++ ){
					partitioned.push_back(batch_view);
				}
			}

			std::vector<ral::frame::BlazingTableView> partitions;
			for(auto partition : partitioned) {
				partitions.push_back(ral::frame::BlazingTableView(partition, batch->names()));
			}

			scatter(partitions,
				output,
				"", //message_id_prefix
				cache_id, //cache_id
				table_idx  //message_tracker_idx
			);

			if (sequence.wait_for_next()){
				batch = sequence.next();
				batch_count++;
			} else {
				done = true;
			}
		} catch(const std::exception& e) {
			// TODO add retry here
			std::string err = "ERROR: in partition_table batch_count " + std::to_string(batch_count) + " Error message: " + std::string(e.what());

			logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
										"query_id"_a=local_context->getContextToken(),
										"step"_a=local_context->getQueryStep(),
										"substep"_a=local_context->getQuerySubstep(),
										"info"_a=err,
										"duration"_a="");
			throw;
		}
	}

	send_total_partition_counts("", cache_id, table_idx);
}

std::pair<bool, bool> JoinPartitionKernel::determine_if_we_are_scattering_a_small_table(const ral::frame::BlazingTableView & left_batch_view,
	const ral::frame::BlazingTableView & right_batch_view ){

	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="determine_if_we_are_scattering_a_small_table start",
								"duration"_a="",
								"kernel_id"_a=this->get_id());

	std::pair<bool, uint64_t> left_num_rows_estimate = this->query_graph->get_estimated_input_rows_to_cache(this->kernel_id, "input_a");
	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="left_num_rows_estimate was " + left_num_rows_estimate.first ? " valid" : " invalid",
								"duration"_a="",
								"kernel_id"_a=this->get_id(),
								"rows"_a=left_num_rows_estimate.second);

	std::pair<bool, uint64_t> right_num_rows_estimate = this->query_graph->get_estimated_input_rows_to_cache(this->kernel_id, "input_b");
	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}|rows|{rows}",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="right_num_rows_estimate was " + right_num_rows_estimate.first ? " valid" : " invalid",
								"duration"_a="",
								"kernel_id"_a=this->get_id(),
								"rows"_a=right_num_rows_estimate.second);

	double left_batch_rows = (double)left_batch_view.num_rows();
	double left_batch_bytes = (double)ral::utilities::get_table_size_bytes(left_batch_view);
	double right_batch_rows = (double)right_batch_view.num_rows();
	double right_batch_bytes = (double)ral::utilities::get_table_size_bytes(right_batch_view);
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
	for (auto i = 0; i < nodes_to_send.size(); i++)	{
		target_ids.push_back(nodes_to_send[i].id());
		determination_messages_to_wait_for.push_back(
			"determine_if_we_are_scattering_a_small_table_" + std::to_string(this->context->getContextToken()) + "_" +	std::to_string(this->get_id()) +	"_" +	nodes_to_send[i].id());
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

	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="determine_if_we_are_scattering_a_small_table about to collectLeftRightTableSizeBytes",
								"duration"_a="",
								"kernel_id"_a=this->get_id());

	std::vector<int64_t> nodes_num_bytes_left(this->context->getTotalNodes());
	std::vector<int64_t> nodes_num_bytes_right(this->context->getTotalNodes());

	for (auto i = 0; i < determination_messages_to_wait_for.size(); i++)	{
		auto message = this->query_graph->get_input_message_cache()->pullCacheData(determination_messages_to_wait_for[i]);
		auto message_with_metadata = static_cast<ral::cache::GPUCacheDataMetaData*>(message.get());
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
	logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="determine_if_we_are_scattering_a_small_table collected " + collectLeftRightTableSizeBytesInfo,
								"duration"_a="",
								"kernel_id"_a=this->get_id());

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
					total_bytes_right < max_join_scatter_mem_overhead) {
			scatter_right = true;
		}
	} else {
		if(estimate_scatter_left < estimate_regular_distribution ||
			estimate_scatter_right < estimate_regular_distribution) {
			if(estimate_scatter_left < estimate_scatter_right &&
				total_bytes_left < max_join_scatter_mem_overhead) {
				scatter_left = true;
			} else if(estimate_scatter_right < estimate_scatter_left &&
					total_bytes_right < max_join_scatter_mem_overhead) {
				scatter_right = true;
			}
		}
	}
	return std::make_pair(scatter_left, scatter_right);
}

void JoinPartitionKernel::perform_standard_hash_partitioning(const std::string & condition,
	std::unique_ptr<ral::frame::BlazingTable> left_batch,
	std::unique_ptr<ral::frame::BlazingTable> right_batch,
	BatchSequence left_sequence,
	BatchSequence right_sequence){

	this->context->incrementQuerySubstep();

	// parsing more of the expression here because we need to have the number of columns of the tables
	std::vector<int> column_indices;
	parseJoinConditionToColumnIndices(condition, column_indices);
	for(int i = 0; i < column_indices.size();i++){
		if(column_indices[i] >= left_batch->num_columns()){
			this->right_column_indices.push_back(column_indices[i] - left_batch->num_columns());
		}else{
			this->left_column_indices.push_back(column_indices[i]);
		}
	}

	computeNormalizationData(left_batch->get_schema(), right_batch->get_schema());

	auto self_node = ral::communication::CommunicationData::getInstance().getSelfNode();

	BlazingMutableThread distribute_left_thread(&JoinPartitionKernel::partition_table, this, std::to_string(this->get_id()), this->context.get(),
		this->left_column_indices, std::move(left_batch), std::ref(left_sequence), this->normalize_left, this->join_column_common_types,
		this->output_.get_cache("output_a").get(),
		"output_a",
		this->logger.get(),
		LEFT_TABLE_IDX);

	BlazingMutableThread distribute_right_thread(&JoinPartitionKernel::partition_table, this, std::to_string(this->get_id()), this->context.get(),
		this->right_column_indices, std::move(right_batch), std::ref(right_sequence), this->normalize_right, this->join_column_common_types,
		this->output_.get_cache("output_b").get(),
		"output_b",
		this->logger.get(),
		RIGHT_TABLE_IDX);

	distribute_left_thread.join();
	distribute_right_thread.join();

	int total_count_left = get_total_partition_counts(LEFT_TABLE_IDX); //left
	this->output_.get_cache("output_a")->wait_for_count(total_count_left);

	int total_count_right = get_total_partition_counts(RIGHT_TABLE_IDX); //right
	this->output_.get_cache("output_b")->wait_for_count(total_count_right);
}

void JoinPartitionKernel::small_table_scatter_distribution(std::unique_ptr<ral::frame::BlazingTable> small_table_batch,
	std::unique_ptr<ral::frame::BlazingTable> big_table_batch,
	BatchSequence small_table_sequence,
	BatchSequenceBypass big_table_sequence,
	const std::pair<bool, bool> & scatter_left_right){

	this->context->incrementQuerySubstep();

	// In this function we are assuming that one and only one of the two bools in scatter_left_right is true
	assert((scatter_left_right.first || scatter_left_right.second) && not (scatter_left_right.first && scatter_left_right.second));

	std::string small_output_cache_name = scatter_left_right.first ? "output_a" : "output_b";
	int small_table_idx = scatter_left_right.first ? LEFT_TABLE_IDX : RIGHT_TABLE_IDX;
	std::string big_output_cache_name = scatter_left_right.first ? "output_b" : "output_a";
	int big_table_idx = scatter_left_right.first ? RIGHT_TABLE_IDX : LEFT_TABLE_IDX;

	BlazingThread distribute_small_table_thread([this, &small_table_batch, &small_table_sequence, small_output_cache_name, small_table_idx](){
		bool done = false;
		int batch_count = 0;
		auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
		while (!done) {
			try {
				if(small_table_batch != nullptr ) {
					broadcast(std::move(small_table_batch),
						this->output_.get_cache(small_output_cache_name).get(),
						"", //message_id_prefix
						small_output_cache_name, //cache_id
						small_table_idx //message_tracker_idx
					);
				}

				if (small_table_sequence.wait_for_next()){
					small_table_batch = small_table_sequence.next();
					batch_count++;
				} else {
					done = true;
				}
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
											"query_id"_a=this->context->getContextToken(),
											"step"_a=this->context->getQueryStep(),
											"substep"_a=this->context->getQuerySubstep(),
											"info"_a="In JoinPartitionKernel scatter_small_table batch_count [{}]. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="",
											"kernel_id"_a=this->get_id());
				throw;
			}
		}

		send_total_partition_counts(
			"", //message_prefix
			small_output_cache_name, //cache_id
			small_table_idx //message_tracker_idx
		);
	});

	auto self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
	bool added = this->add_to_output_cache(std::move(big_table_batch), big_output_cache_name);
	if (added) {
		increment_node_count(self_node.id(), big_table_idx);
	}
	BlazingThread big_table_passthrough_thread([this, &big_table_sequence, big_output_cache_name, big_table_idx, self_node](){
		while (big_table_sequence.wait_for_next()) {
			auto batch = big_table_sequence.next();
			bool added = this->add_to_output_cache(std::move(batch), big_output_cache_name);
			if (added) {
				increment_node_count(self_node.id(), big_table_idx);
			}
		}
	});

	distribute_small_table_thread.join();

	int total_count = get_total_partition_counts(small_table_idx);
	this->output_cache(small_output_cache_name)->wait_for_count(total_count);

	big_table_passthrough_thread.join();
}

kstatus JoinPartitionKernel::run() {
	CodeTimer timer;

	bool ordered = false;
	BatchSequence left_sequence(this->input_.get_cache("input_a"), this, ordered);
	BatchSequence right_sequence(this->input_.get_cache("input_b"), this, ordered);

	std::unique_ptr<ral::frame::BlazingTable> left_batch = left_sequence.next();
	std::unique_ptr<ral::frame::BlazingTable> right_batch = right_sequence.next();

	if (left_batch == nullptr || left_batch->num_columns() == 0){
		while (left_sequence.wait_for_next()){
			left_batch = left_sequence.next();
			if (left_batch != nullptr && left_batch->num_columns() > 0){
				break;
			}
		}
	}
	if (left_batch == nullptr || left_batch->num_columns() == 0){
		RAL_FAIL("In JoinPartitionKernel left side is empty and cannot determine join column indices");
	}

	std::pair<bool, bool> scatter_left_right;
	if (this->join_type == OUTER_JOIN){ // cant scatter a full outer join
		scatter_left_right = std::make_pair(false, false);
	} else {
		scatter_left_right = determine_if_we_are_scattering_a_small_table(left_batch->toBlazingTableView(),
																			right_batch->toBlazingTableView());
	}
	// scatter_left_right = std::make_pair(false, false); // Do this for debugging if you want to disable small table join optmization
	if (scatter_left_right.first){
		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="JoinPartition Scattering left table",
									"duration"_a="",
									"kernel_id"_a=this->get_id());

		BatchSequenceBypass big_table_sequence(this->input_.get_cache("input_b"), this);
		small_table_scatter_distribution( std::move(left_batch), std::move(right_batch),
					std::move(left_sequence), std::move(big_table_sequence), scatter_left_right);
	} else if (scatter_left_right.second) {
		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="JoinPartition Scattering right table",
									"duration"_a="",
									"kernel_id"_a=this->get_id());

		BatchSequenceBypass big_table_sequence(this->input_.get_cache("input_a"), this);
		small_table_scatter_distribution( std::move(right_batch), std::move(left_batch),
					std::move(right_sequence), std::move(big_table_sequence), scatter_left_right);
	} else {
		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="JoinPartition Standard hash partition",
									"duration"_a="",
									"kernel_id"_a=this->get_id());

		perform_standard_hash_partitioning(condition, std::move(left_batch), std::move(right_batch),
			std::move(left_sequence), std::move(right_sequence));
	}

	logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
								"query_id"_a=context->getContextToken(),
								"step"_a=context->getQueryStep(),
								"substep"_a=context->getQuerySubstep(),
								"info"_a="JoinPartition Kernel Completed",
								"duration"_a=timer.elapsed_time(),
								"kernel_id"_a=this->get_id());

	return kstatus::proceed;
}

std::string JoinPartitionKernel::get_join_type() {
	return join_type;
}

// END JoinPartitionKernel

} // namespace batch
} // namespace ral
