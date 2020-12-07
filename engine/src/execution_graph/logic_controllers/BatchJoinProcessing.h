#pragma once

#include <tuple>

#include "BatchProcessing.h"
#include "ExceptionHandling/BlazingThread.h"
#include "taskflow/distributing_kernel.h"
/*#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "distribution/primitives.h"
#include "taskflow/distributing_kernel.h"
#include "error.hpp"
#include "blazingdb/concurrency/BlazingThread.h"*/
#include "CodeTimer.h"
#include <cudf/stream_compaction.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/join.hpp>

namespace ral {
namespace batch {
using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using namespace fmt::literals;

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";
const std::string CROSS_JOIN = "cross";

const int LEFT_TABLE_IDX = 0;
const int RIGHT_TABLE_IDX = 1;

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

	std::unique_ptr<ral::frame::BlazingTable> join_set(
		const ral::frame::BlazingTableView & table_left,
		const ral::frame::BlazingTableView & table_right);

	void do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

	std::string get_join_type();

private:
	std::unique_ptr<ral::cache::CacheData> load_left_set();
	std::unique_ptr<ral::cache::CacheData> load_right_set();

	void mark_set_completed(int left_ind, int right_ind);

	std::vector<int> get_indexes_from_message_id(const std::vector<std::string> & message_ids);

	// This function checks to see if there is a set from our current completion_matix (data we have already loaded once)
	// that we have not completed that uses one of our current indices, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_another_set_to_do_with_data_we_already_have(int left_ind = -1, int right_ind = -1);

	// This function returns the first not completed set, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_set_that_has_not_been_completed();

  // this function makes sure that the columns being joined are of the same type so that we can join them properly
	void computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const std::vector<cudf::data_type> & right_types);

private:
	std::shared_ptr<ral::cache::CacheMachine> left_input;
	std::shared_ptr<ral::cache::CacheMachine> right_input;

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

	void do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

	std::string get_join_type();

private:
	// this function makes sure that the columns being joined are of the same type so that we can join them properly
	void computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const	std::vector<cudf::data_type> & right_types);

	std::pair<bool, bool> determine_if_we_are_scattering_a_small_table(const ral::cache::CacheData& left_cache_data,
		const ral::cache::CacheData& right_cache_data);

	void perform_standard_hash_partitioning(
		std::unique_ptr<ral::cache::CacheData> left_cache_data,
		std::unique_ptr<ral::cache::CacheData> right_cache_data,
		std::shared_ptr<ral::cache::CacheMachine> left_input,
		std::shared_ptr<ral::cache::CacheMachine> right_input);

	void small_table_scatter_distribution(std::unique_ptr<ral::cache::CacheData> small_cache_data,
		std::unique_ptr<ral::cache::CacheData> big_cache_data,
		std::shared_ptr<ral::cache::CacheMachine> small_input,
		std::shared_ptr<ral::cache::CacheMachine> big_input);

private:
	std::pair<bool, bool> scatter_left_right = {false, false};

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
