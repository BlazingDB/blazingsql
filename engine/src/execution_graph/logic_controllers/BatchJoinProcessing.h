#pragma once

#include <tuple>
#include "BatchProcessing.h"
#include "taskflow/distributing_kernel.h"

namespace ral {
namespace batch {

using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using Context = blazingdb::manager::Context;
using namespace fmt::literals;

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";
const std::string CROSS_JOIN = "cross";

const int LEFT_TABLE_IDX = 0;
const int RIGHT_TABLE_IDX = 1;
struct TableSchema {
	std::vector<cudf::data_type> column_types;
	std::vector<std::string> column_names;
	TableSchema(std::vector<cudf::data_type> column_types, std::vector<std::string> column_names)
		: column_types{column_types}, column_names{column_names}
	{}
};

/* This function takes in the relational algrebra expression and returns:
- modified relational algrbra expression
- join condition
- filter_statement
- join type */
std::tuple<std::string, std::string, std::string, std::string> parseExpressionToGetTypeAndCondition(const std::string & expression);

void parseJoinConditionToColumnIndices(const std::string & condition, std::vector<int> & columnIndices);

void split_inequality_join_into_join_and_filter(const std::string & join_statement, std::string & new_join_statement, std::string & filter_statement);

class PartwiseJoin : public kernel {
public:
	PartwiseJoin(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	std::unique_ptr<TableSchema> left_schema{nullptr};
	std::unique_ptr<TableSchema> right_schema{nullptr};

	std::unique_ptr<ral::frame::BlazingTable> load_left_set();
	std::unique_ptr<ral::frame::BlazingTable> load_right_set();

	void mark_set_completed(int left_ind, int right_ind);

	// This function checks to see if there is a set from our current completion_matix (data we have already loaded once)
	// that we have not completed that uses one of our current indices, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_another_set_to_do_with_data_we_already_have(int left_ind, int right_ind);

	// This function returns the first not completed set, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_set_that_has_not_been_completed();

	// This function makes sure that the columns being joined are of the same type so that we can join them properly
	void computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const std::vector<cudf::data_type> & right_types);

	std::unique_ptr<ral::frame::BlazingTable> join_set(
		const ral::frame::BlazingTableView & table_left,
		const ral::frame::BlazingTableView & table_right);

	virtual kstatus run();
	std::string get_join_type();

private:
	BatchSequence left_sequence, right_sequence;

	int max_left_ind;
	int max_right_ind;
	std::vector<std::vector<bool>> completion_matrix;
	std::shared_ptr<ral::cache::CacheMachine> leftArrayCache;
	std::shared_ptr<ral::cache::CacheMachine> rightArrayCache;

	// parsed expression related parameters
	std::string join_type;
	std::string condition;
	std::string filter_statement;
	std::vector<cudf::size_type> left_column_indices, right_column_indices;
	std::vector<cudf::data_type> join_column_common_types;
	bool normalize_left, normalize_right;
	std::vector<std::string> result_names;
};


class JoinPartitionKernel : public distributing_kernel {
public:
	JoinPartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

	// this function makes sure that the columns being joined are of the same type so that we can join them properly
	void computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const	std::vector<cudf::data_type> & right_types);

	void partition_table(const std::string & kernel_id,
				Context* local_context,
				std::vector<cudf::size_type> column_indices,
				std::unique_ptr<ral::frame::BlazingTable> batch,
				BatchSequence & sequence,
				bool normalize_types,
				const std::vector<cudf::data_type> & join_column_common_types,
				ral::cache::CacheMachine* output,
				const std::string & cache_id,
				spdlog::logger* logger,
				int table_idx);

	std::pair<bool, bool> determine_if_we_are_scattering_a_small_table(const ral::frame::BlazingTableView & left_batch_view,
		const ral::frame::BlazingTableView & right_batch_view );

	void perform_standard_hash_partitioning(const std::string & condition,
		std::unique_ptr<ral::frame::BlazingTable> left_batch,
		std::unique_ptr<ral::frame::BlazingTable> right_batch,
		BatchSequence left_sequence,
		BatchSequence right_sequence);

	void small_table_scatter_distribution(std::unique_ptr<ral::frame::BlazingTable> small_table_batch,
		std::unique_ptr<ral::frame::BlazingTable> big_table_batch,
		BatchSequence small_table_sequence,
		BatchSequenceBypass big_table_sequence,
		const std::pair<bool, bool> & scatter_left_right);

	virtual kstatus run();
	std::string get_join_type();

private:
	// parsed expression related parameters
	std::string join_type;
	std::string condition;
	std::string filter_statement;
	std::vector<cudf::size_type> left_column_indices, right_column_indices;
	std::vector<cudf::data_type> join_column_common_types;
	bool normalize_left, normalize_right;
};

} // namespace batch
} // namespace ral

/*
single node
- PartwiseJoin (two inputs, one output)

multi node
- JoinPartitionKernel (two inputs, two outputs)
- PartwiseJoin (two inputs, one output)
*/
