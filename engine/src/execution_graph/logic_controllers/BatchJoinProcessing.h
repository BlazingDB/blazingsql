#pragma once

#include <tuple>

#include "BatchProcessing.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "distribution/primitives.h"
#include "error.hpp"
#include "blazingdb/concurrency/BlazingThread.h"
#include "CodeTimer.h"
#include <cudf/stream_compaction.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/join.hpp>
#include "error.hpp"

namespace ral {
namespace batch {
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
	PartwiseJoin(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
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

	bool can_you_throttle_my_input() {
		return false;  // join has its own sort of limiter, so its not good to try to apply another limiter
	}

	std::unique_ptr<TableSchema> left_schema{nullptr};
 	std::unique_ptr<TableSchema> right_schema{nullptr};

    std::unique_ptr<ral::frame::BlazingTable> load_left_set(){

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

	std::unique_ptr<ral::frame::BlazingTable> load_right_set(){
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

   void mark_set_completed(int left_ind, int right_ind){
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
	std::tuple<int, int> check_for_another_set_to_do_with_data_we_already_have(int left_ind, int right_ind){
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
			std::cout<<"ERROR out of range in check_for_another_set_to_do_with_data_we_already_have"<<std::endl;
			return std::make_tuple(-1, -1);
		}
	}

    	// This function returns the first not completed set, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_set_that_has_not_been_completed(){
		for (int i = 0; i < completion_matrix.size(); i++){
			for (int j = 0; j < completion_matrix[i].size(); j++){
                if (!completion_matrix[i][j]){
                    return std::make_tuple(i, j);
                }
            }
		}
		return std::make_tuple(-1, -1);
	}

  // this function makes sure that the columns being joined are of the same type so that we can join them properly
	void computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const std::vector<cudf::data_type> & right_types){
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

	std::unique_ptr<ral::frame::BlazingTable> join_set(
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

    virtual kstatus run() {
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
						// printf("joined table\n");
						// ral::utilities::print_blazing_table_view(joined->toBlazingTableView());
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

		return kstatus::proceed;
	}

	std::string get_join_type() {
		return join_type;
	}

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


class JoinPartitionKernel : public kernel {
public:
	JoinPartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::JoinPartitionKernel} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
		this->output_.add_port("output_a", "output_b");

		std::tie(this->expression, this->condition, this->filter_statement, this->join_type) = parseExpressionToGetTypeAndCondition(this->expression);
	}

	bool can_you_throttle_my_input() {
		return true;
	}

	// this function makes sure that the columns being joined are of the same type so that we can join them properly
	void computeNormalizationData(const	std::vector<cudf::data_type> & left_types, const	std::vector<cudf::data_type> & right_types){
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

	static void partition_table(const std::string & kernel_id,
				Context* local_context,
				std::vector<cudf::size_type> column_indices,
				std::unique_ptr<ral::frame::BlazingTable> batch,
				BatchSequence & sequence,
				bool normalize_types,
				const std::vector<cudf::data_type> & join_column_common_types,
				ral::cache::CacheMachine* output,
				ral::cache::CacheMachine* graph_output,
				const std::string & cache_id,
				std::map<std::string, int>& node_count,
				std::vector<std::string>& messages_to_wait_for,
				spdlog::logger* logger,
				int table_idx)
	{
		using ColumnDataPartitionMessage = ral::communication::messages::ColumnDataPartitionMessage;

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
				std::vector<ral::distribution::NodeColumnView > partitions_to_send;
				for(int nodeIndex = 0; nodeIndex < local_context->getTotalNodes(); nodeIndex++ ){
					ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
					if (local_context->getNode(nodeIndex) == self_node){
						// hash_partition followed by split does not create a partition that we can own, so we need to clone it.
						// if we dont clone it, hashed_data will go out of scope before we get to use the partition
						// also we need a BlazingTable to put into the cache, we cant cache views.
						std::unique_ptr<ral::frame::BlazingTable> partition_table_clone = partition_table_view.clone();

						output->addToCache(std::move(partition_table_clone), cache_id + "_" + kernel_id,true);
						node_count[self_node.id()]++;
					} else {
						partitions_to_send.emplace_back(
							std::make_pair(local_context->getNode(nodeIndex), partition_table_view));
					}
				}

				ral::cache::MetadataDictionary metadata;
				metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, kernel_id);
				metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(local_context->getContextToken()));
				metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "true");
				metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
				for (auto i = 0; i < partitions_to_send.size(); i++) {
					blazingdb::transport::Node dest_node;
					ral::frame::BlazingTableView table_view;
					std::tie(dest_node, table_view) = partitions_to_send[i];
					if(dest_node == self_node ) {
						continue;
					}

					metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, dest_node.id());
					metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);

					node_count[dest_node.id()]++;
					graph_output->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(table_view.clone(), metadata),"",true);
				}

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

		auto nodes = local_context->getAllNodes();
		for(std::size_t i = 0; i < nodes.size(); ++i) {

			if(!(nodes[i] == self_node)) {
				ral::cache::MetadataDictionary metadata;
				metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, kernel_id);
				metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(local_context->getContextToken()));
				metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
				metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, cache_id);
				metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
				metadata.add_value(ral::cache::MESSAGE_ID,std::to_string(table_idx) + "partition_" +
				 																					metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																									metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																									metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
				metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
				metadata.add_value(ral::cache::PARTITION_COUNT, std::to_string(node_count[nodes[i].id()]));
				messages_to_wait_for.push_back(std::to_string(table_idx) + "partition_" +
				 																					metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																									metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																									nodes[i].id());
				graph_output->addCacheData(
						std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata),"",true);
			}
		}

	}

	std::pair<bool, bool> determine_if_we_are_scattering_a_small_table(const ral::frame::BlazingTableView & left_batch_view,
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

		// ral::distribution::distributeLeftRightTableSizeBytes(context.get(), left_bytes_estimate, right_bytes_estimate);

		auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
		int self_node_idx = context->getNodeIndex(self_node);
		auto nodes_to_send = context->getAllOtherNodes(self_node_idx);
		std::string worker_ids_metadata;
		std::vector<std::string> messages_to_wait_for;
		for (auto i = 0; i < nodes_to_send.size(); i++)	{
			worker_ids_metadata += nodes_to_send[i].id();
			messages_to_wait_for.push_back(
				"determine_if_we_are_scattering_a_small_table_" + std::to_string(this->context->getContextToken()) + "_" +	std::to_string(this->get_id()) +	"_" +	nodes_to_send[i].id());

			if (i < nodes_to_send.size() - 1) {
				worker_ids_metadata += ",";
			}
		}

		ral::cache::MetadataDictionary metadata;
		metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
		metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
		metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
		metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
		metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id() );
		metadata.add_value(ral::cache::MESSAGE_ID, "determine_if_we_are_scattering_a_small_table_" +
																							metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																							metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																							metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
		metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, worker_ids_metadata);
		metadata.add_value(ral::cache::JOIN_LEFT_BYTES_METADATA_LABEL, std::to_string(left_bytes_estimate));
		metadata.add_value(ral::cache::JOIN_RIGHT_BYTES_METADATA_LABEL, std::to_string(right_bytes_estimate));
		ral::cache::CacheMachine* output_cache = this->query_graph->get_output_message_cache();
		output_cache->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata),"",true);

		logger->trace("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="determine_if_we_are_scattering_a_small_table about to collectLeftRightTableSizeBytes",
									"duration"_a="",
									"kernel_id"_a=this->get_id());

		std::vector<int64_t> nodes_num_bytes_left(this->context->getTotalNodes());
		std::vector<int64_t> nodes_num_bytes_right(this->context->getTotalNodes());


		int64_t prev_total_rows = 0;
		for (auto i = 0; i < messages_to_wait_for.size(); i++)	{
			auto message = this->query_graph->get_input_message_cache()->pullCacheData(messages_to_wait_for[i]);
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

	void perform_standard_hash_partitioning(const std::string & condition,
		std::unique_ptr<ral::frame::BlazingTable> left_batch,
		std::unique_ptr<ral::frame::BlazingTable> right_batch,
		BatchSequence left_sequence,
		BatchSequence right_sequence){
		using ColumnDataPartitionMessage = ral::communication::messages::ColumnDataPartitionMessage;

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

		std::map<std::string, int> node_count_left;
		std::vector<std::string> messages_to_wait_for_left;
		BlazingMutableThread distribute_left_thread(&JoinPartitionKernel::partition_table, std::to_string(this->get_id()), this->context.get(),
			this->left_column_indices, std::move(left_batch), std::ref(left_sequence), this->normalize_left, this->join_column_common_types,
			this->output_.get_cache("output_a").get(),
			this->query_graph->get_output_message_cache(),
			"output_a",
			std::ref(node_count_left),
			std::ref(messages_to_wait_for_left),
			this->logger.get(),
		  LEFT_TABLE_IDX);

		// BlazingThread left_consumer([context = this->context, this](){
		// 	ExternalBatchColumnDataSequence<ColumnDataPartitionMessage> external_input_left(this->context, this->get_message_id(), this);
		// 	std::unique_ptr<ral::frame::BlazingHostTable> host_table;

		// 	while (host_table = external_input_left.next()) {
		// 		this->add_to_output_cache(std::move(host_table), "output_a");
		// 	}
		// });

		distribute_left_thread.join();
		int total_count_left = node_count_left[self_node.id()];
		for (auto message : messages_to_wait_for_left){
			auto meta_message = this->query_graph->get_input_message_cache()->pullCacheData(message);
			total_count_left += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
		}
		this->output_.get_cache("output_a")->wait_for_count(total_count_left);

		std::map<std::string, int> node_count_right;
		std::vector<std::string> messages_to_wait_for_right;
		BlazingMutableThread distribute_right_thread(&JoinPartitionKernel::partition_table, std::to_string(this->get_id()), this->context.get(),
			this->right_column_indices, std::move(right_batch), std::ref(right_sequence), this->normalize_right, this->join_column_common_types,
			this->output_.get_cache("output_b").get(),
			this->query_graph->get_output_message_cache(),
			"output_b",
			std::ref(node_count_right),
			std::ref(messages_to_wait_for_right),
			this->logger.get(),
			RIGHT_TABLE_IDX);

		// create thread with ExternalBatchColumnDataSequence for the right table being distriubted
		// BlazingThread right_consumer([cloned_context, this](){
		// 	ExternalBatchColumnDataSequence<ColumnDataPartitionMessage> external_input_right(cloned_context, this->get_message_id(), this);
		// 	std::unique_ptr<ral::frame::BlazingHostTable> host_table;

		// 	while (host_table = external_input_right.next()) {
		// 		this->add_to_output_cache(std::move(host_table), "output_b");
		// 	}
		// });
		distribute_right_thread.join();
		int total_count_right = node_count_right[self_node.id()];
		for (auto message : messages_to_wait_for_right){
			auto meta_message = this->query_graph->get_input_message_cache()->pullCacheData(message);
			total_count_right += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
		}
		this->output_.get_cache("output_b")->wait_for_count(total_count_right);
	}

	void small_table_scatter_distribution(std::unique_ptr<ral::frame::BlazingTable> small_table_batch,
		std::unique_ptr<ral::frame::BlazingTable> big_table_batch,
		BatchSequence small_table_sequence,
		BatchSequenceBypass big_table_sequence,
		const std::pair<bool, bool> & scatter_left_right){
		using ColumnDataMessage = ral::communication::messages::ColumnDataMessage;

		this->context->incrementQuerySubstep();

		// In this function we are assuming that one and only one of the two bools in scatter_left_right is true
		assert((scatter_left_right.first || scatter_left_right.second) && not (scatter_left_right.first && scatter_left_right.second));

		std::string small_output_cache_name = scatter_left_right.first ? "output_a" : "output_b";
		std::string big_output_cache_name = scatter_left_right.first ? "output_b" : "output_a";
		std::map<std::string, int> node_count;
		std::vector<std::string> messages_to_wait_for;

		BlazingThread distribute_small_table_thread([this, &small_table_batch, &small_table_sequence, small_output_cache_name, &node_count, &messages_to_wait_for](){
			bool done = false;
			int batch_count = 0;
			auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
			while (!done) {
				try {
					if(small_table_batch != nullptr ) {
						// ral::distribution::scatterData(this->context.get(), small_table_batch->toBlazingTableView());

						int self_node_idx = context->getNodeIndex(self_node);
						auto nodes_to_send = context->getAllOtherNodes(self_node_idx);
						std::string worker_ids_metadata;
						for (auto i = 0; i < nodes_to_send.size(); i++)	{
							if(nodes_to_send[i].id() != self_node.id()){
								worker_ids_metadata += nodes_to_send[i].id();
								if (i < nodes_to_send.size() - 1) {
									worker_ids_metadata += ",";
								}
								node_count[nodes_to_send[i].id()]++;
							}

						}
						node_count[self_node.id()]++;

						ral::cache::MetadataDictionary metadata;
						metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
						metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
						metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "true");
						metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, small_output_cache_name);
						metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
						metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, worker_ids_metadata);
						ral::cache::CacheMachine* output_cache = this->query_graph->get_output_message_cache();
						output_cache->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(small_table_batch->toBlazingTableView().clone(), metadata),"",true);
						this->output_.get_cache(small_output_cache_name).get()->addToCache(std::move(small_table_batch),"",true);
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
			// ral::distribution::notifyLastTablePartitions(this->context.get(), ColumnDataMessage::MessageID());

			auto nodes = context->getAllNodes();
			for(std::size_t i = 0; i < nodes.size(); ++i) {
				if(!(nodes[i] == self_node)) {
					ral::cache::MetadataDictionary metadata;
					metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
					metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
					metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
					metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
					metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
					metadata.add_value(ral::cache::MESSAGE_ID,"part_count_" +
																										metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																										metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																										metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
					metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
					metadata.add_value(ral::cache::PARTITION_COUNT, std::to_string(node_count[nodes[i].id()]));
					messages_to_wait_for.push_back("part_count_" +
																				metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																				metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																				metadata.get_values()[ral::cache::WORKER_IDS_METADATA_LABEL]);

					this->query_graph->get_output_message_cache()->addCacheData(
							std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata),"",true);
				}
			}
		});

		// BlazingThread collect_small_table_thread([this, small_output_cache_name](){
		// 	ExternalBatchColumnDataSequence<ColumnDataMessage> external_input_left(this->context, this->get_message_id(), this);

		// 	while (external_input_left.wait_for_next()) {
		// 		std::unique_ptr<ral::frame::BlazingHostTable> host_table = external_input_left.next();
		// 		this->add_to_output_cache(std::move(host_table), small_output_cache_name);
		// 	}
		// });
		distribute_small_table_thread.join();
		auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
 		int total_count = node_count[self_node.id()];
		for (auto message : messages_to_wait_for){
			auto meta_message = this->query_graph->get_input_message_cache()->pullCacheData(message);
			total_count += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
		}
		this->output_cache(small_output_cache_name)->wait_for_count(total_count);

		this->add_to_output_cache(std::move(big_table_batch), big_output_cache_name);
		BlazingThread big_table_passthrough_thread([this, &big_table_sequence, big_output_cache_name](){
			while (big_table_sequence.wait_for_next()) {
				auto batch = big_table_sequence.next();
				this->add_to_output_cache(std::move(batch), big_output_cache_name);
			}
		});


		// collect_small_table_thread.join();
		big_table_passthrough_thread.join();
	}

	virtual kstatus run() {
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

	std::string get_join_type() {
		return join_type;
	}

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
