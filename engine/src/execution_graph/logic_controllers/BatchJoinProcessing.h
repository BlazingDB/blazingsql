#pragma once

#include "BatchProcessing.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "parser/expression_utils.hpp"
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "distribution/primitives.h"
#include "Utils.cuh"
#include "blazingdb/concurrency/BlazingThread.h"

#include <cudf/stream_compaction.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/join.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using ColumnDataMessage = ral::communication::messages::experimental::ColumnDataMessage;

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

struct TableSchema {
	std::vector<cudf::data_type> column_types;
	std::vector<std::string> column_names;
	TableSchema(std::vector<cudf::data_type> column_types, std::vector<std::string> column_names) 
		: column_types{column_types}, column_names{column_names}
	{}
};

class PartwiseJoin :public kernel {
public:
	PartwiseJoin(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context}, left_sequence{nullptr, this}, right_sequence{nullptr, this} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");

		SET_SIZE_THRESHOLD = 300000000;
		this->max_left_ind = -1;
		this->max_right_ind = -1;

        this->left_sequence.set_source(this->input_.get_cache("input_a"));
        this->right_sequence.set_source(this->input_.get_cache("input_b"));

        this->leftArrayCache = 	ral::cache::create_cache_machine(ral::cache::cache_settings{.type = ral::cache::CacheType::SIMPLE});
        this->rightArrayCache = ral::cache::create_cache_machine(ral::cache::cache_settings{.type = ral::cache::CacheType::SIMPLE});
	}

	std::unique_ptr<TableSchema> left_schema{nullptr};
 	std::unique_ptr<TableSchema> right_schema{nullptr};
	
	std::unique_ptr<ral::frame::BlazingTable> load_set(BatchSequence & input, bool load_all){
		std::size_t bytes_loaded = 0;
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_loaded;
		if (input.wait_for_next()){
			tables_loaded.emplace_back(input.next());
			bytes_loaded += tables_loaded.back()->sizeInBytes(); 
		} else {
			return nullptr;
		}

		while ((input.has_next_now() && bytes_loaded < SET_SIZE_THRESHOLD) ||
					(load_all && input.wait_for_next())) {
			tables_loaded.emplace_back(input.next());
			bytes_loaded += tables_loaded.back()->sizeInBytes(); 
		}
		if (tables_loaded.size() == 0){
			return nullptr;
		} else if (tables_loaded.size() == 1){
			return std::move(tables_loaded[0]);
		} else {
			std::vector<ral::frame::BlazingTableView> tables_to_concat(tables_loaded.size());
			for (std::size_t i = 0; i < tables_loaded.size(); i++){
				tables_to_concat[i] = tables_loaded[i]->toBlazingTableView();
			}
			return ral::utilities::experimental::concatTables(tables_to_concat);			
		}
	}


    std::unique_ptr<ral::frame::BlazingTable> load_left_set(){
		
		this->max_left_ind++;
		// need to load all the left side, only if doing a FULL OUTER JOIN
		bool load_all = this->join_type == OUTER_JOIN ?  true : false;
		auto table = load_set(this->left_sequence, load_all);
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
		// need to load all the right side, if doing any type of outer join
		bool load_all = this->join_type != INNER_JOIN ?  true : false;
		auto table = load_set(this->right_sequence, load_all);
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
	void normalize(std::unique_ptr<ral::frame::BlazingTable> & left, std::unique_ptr<ral::frame::BlazingTable> & right){
		std::vector<std::unique_ptr<ral::frame::BlazingColumn>> left_columns = left->releaseBlazingColumns();
		std::vector<std::unique_ptr<ral::frame::BlazingColumn>> right_columns = right->releaseBlazingColumns();
		for (size_t i = 0; i < this->left_column_indices.size(); i++){
			if (left_columns[this->left_column_indices[i]]->view().type().id() != right_columns[this->right_column_indices[i]]->view().type().id()){
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns_to_normalize;
			columns_to_normalize.emplace_back(std::move(left_columns[this->left_column_indices[i]]));
			columns_to_normalize.emplace_back(std::move(right_columns[this->right_column_indices[i]]));
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> normalized_columns = ral::utilities::experimental::normalizeColumnTypes(std::move(columns_to_normalize));
			left_columns[this->left_column_indices[i]] = std::move(normalized_columns[0]);
			right_columns[this->right_column_indices[i]] = std::move(normalized_columns[1]);
			}
		}
		left = std::make_unique<ral::frame::BlazingTable>(std::move(left_columns), left->names());
		right = std::make_unique<ral::frame::BlazingTable>(std::move(right_columns), right->names());
	}


    std::unique_ptr<ral::frame::BlazingTable> join_set(const ral::frame::BlazingTableView & table_left, const ral::frame::BlazingTableView & table_right){
		std::unique_ptr<CudfTable> result_table;
		std::vector<std::pair<cudf::size_type, cudf::size_type>> columns_in_common;
		if(this->join_type == INNER_JOIN) {
			//Removing nulls on key columns before joining
			std::unique_ptr<CudfTable> table_left_dropna;
			std::unique_ptr<CudfTable> table_right_dropna;
			bool has_nulls_left = ral::processor::check_if_has_nulls(table_left.view(), left_column_indices);
			bool has_nulls_right = ral::processor::check_if_has_nulls(table_right.view(), right_column_indices);
			if(has_nulls_left){
				table_left_dropna = cudf::experimental::drop_nulls(table_left.view(), left_column_indices);
			}
			if(has_nulls_right){
				table_right_dropna = cudf::experimental::drop_nulls(table_right.view(), right_column_indices);
			}

			result_table = cudf::experimental::inner_join(
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
				table_right_dropna = cudf::experimental::drop_nulls(table_right.view(), right_column_indices);
			}
			
			result_table = cudf::experimental::left_join(
				table_left.view(),
				has_nulls_right ? table_right_dropna->view() : table_right.view(),
				this->left_column_indices,
				this->right_column_indices,
				columns_in_common);
		} else if(this->join_type == OUTER_JOIN) {
			result_table = cudf::experimental::full_join(
				table_left.view(),
				table_right.view(),
				this->left_column_indices,
				this->right_column_indices,
				columns_in_common);
		} else {
			RAL_FAIL("Unsupported join operator");
		}
		return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), this->result_names);
	}

    virtual kstatus run() {

		this->left_sequence = BatchSequence(this->input_.get_cache("input_a"), this);
		this->right_sequence = BatchSequence(this->input_.get_cache("input_b"), this);

		// lets parse part of the expression here, because we need the joinType before we load
		std::string new_join_statement, filter_statement;
		StringUtil::findAndReplaceAll(this->expression, "IS NOT DISTINCT FROM", "=");
		split_inequality_join_into_join_and_filter(this->expression, new_join_statement, filter_statement);
		std::string condition = get_named_expression(new_join_statement, "condition");
		this->join_type = get_named_expression(new_join_statement, "joinType");

		std::unique_ptr<ral::frame::BlazingTable> left_batch = nullptr;
		std::unique_ptr<ral::frame::BlazingTable> right_batch = nullptr;
		bool done = false;
		bool produced_output = false;
		int left_ind = 0;
		int right_ind = 0;
		
		while (!done) {
			try {

				if (left_batch == nullptr && right_batch == nullptr){ // first load
					left_batch = load_left_set();
					right_batch = load_right_set();
					this->max_left_ind = 0; // we have loaded just once. This is the highest index for now
					this->max_right_ind = 0; // we have loaded just once. This is the highest index for now

					{ // parsing more of the expression here because we need to have the number of columns of the tables
						std::vector<int> column_indices;
						ral::processor::parseJoinConditionToColumnIndices(condition, column_indices);
						for(int i = 0; i < column_indices.size();i++){
							if(column_indices[i] >= left_batch->num_columns()){
								this->right_column_indices.push_back(column_indices[i] - left_batch->num_columns());
							}else{
								this->left_column_indices.push_back(column_indices[i]);
							}
						}
						this->result_names = left_batch->names();
						std::vector<std::string> right_names = right_batch->names();
						this->result_names.insert(this->result_names.end(), right_names.begin(), right_names.end());
					}

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
					normalize(left_batch, right_batch);
					std::unique_ptr<ral::frame::BlazingTable> joined = join_set(left_batch->toBlazingTableView(), right_batch->toBlazingTableView());
					
					produced_output = true;
					if (filter_statement != "") {
						auto filter_table = ral::processor::process_filter(joined->toBlazingTableView(), filter_statement, this->context.get());					
						this->add_to_output_cache(std::move(filter_table));
					} else{
						// printf("joined table\n");
						// ral::utilities::print_blazing_table_view(joined->toBlazingTableView());
						this->add_to_output_cache(std::move(joined));
					}

					mark_set_completed(left_ind, right_ind);
				}				
				
			} catch(const std::exception& e) {
				// TODO add retry here
				std::string err = "ERROR: in PartwiseJoin left_ind " + std::to_string(left_ind) + " right_ind " + std::to_string(right_ind) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
				throw e;
			}
		}
		
		if (!produced_output){
			std::cout<<"WARNING: Join kernel did not produce an output"<<std::endl;
			// WSM TODO put an empty output into output cache
		}
		
		return kstatus::proceed;
	}

private:
    BatchSequence left_sequence, right_sequence;

    int max_left_ind;
    int max_right_ind;
	std::shared_ptr<Context> context;
	std::string expression;
	std::vector<std::vector<bool>> completion_matrix;
	std::shared_ptr<ral::cache::CacheMachine> leftArrayCache;
    std::shared_ptr<ral::cache::CacheMachine> rightArrayCache;
	std::size_t SET_SIZE_THRESHOLD;

	// parsed expression related parameters
	std::string join_type;
	std::vector<cudf::size_type> left_column_indices, right_column_indices;
  	std::vector<std::string> result_names;
};


class JoinPartitionKernel :public kernel {
public:
	JoinPartitionKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
		this->output_.add_port("output_a", "output_b");
	}


    static void partition_table(std::shared_ptr<Context> local_context, 
				std::vector<cudf::size_type> column_indices, 
				std::unique_ptr<ral::frame::BlazingTable> batch, 
				BatchSequence & sequence, 
				std::shared_ptr<ral::cache::CacheMachine> & output){

        bool done = false;
		// num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future. 
		// If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
		int num_partitions = local_context->getTotalNodes(); 
		std::unique_ptr<CudfTable> hashed_data;
		std::vector<cudf::size_type> hased_data_offsets;
		int batch_count = 0;
        while (!done) {		
            try {            
				auto batch_view = batch->view();
				std::vector<CudfTableView> partitioned;
				if (batch->num_rows() > 0) {
					std::tie(hashed_data, hased_data_offsets) = cudf::experimental::hash_partition(batch_view, column_indices, num_partitions);

					assert(hased_data_offsets.begin() != hased_data_offsets.end());
					// the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
					std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
					partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);
					
				} else {
					for(int nodeIndex = 0; nodeIndex < local_context->getTotalNodes(); nodeIndex++ ){
						partitioned.push_back(batch_view);
					}
				}
				std::vector<ral::distribution::experimental::NodeColumnView > partitions_to_send;
				for(int nodeIndex = 0; nodeIndex < local_context->getTotalNodes(); nodeIndex++ ){
					ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
					if (local_context->getNode(nodeIndex) == ral::communication::experimental::CommunicationData::getInstance().getSelfNode()){
						// hash_partition followed by split does not create a partition that we can own, so we need to clone it.
						// if we dont clone it, hashed_data will go out of scope before we get to use the partition
						// also we need a BlazingTable to put into the cache, we cant cache views.
						std::unique_ptr<ral::frame::BlazingTable> partition_table_clone = partition_table_view.clone();

						// TODO: create message id and send to add add_to_output_cache
						output->addToCache(std::move(partition_table_clone));
					} else {
						partitions_to_send.emplace_back(
							std::make_pair(local_context->getNode(nodeIndex), partition_table_view));
					}
				}
				ral::distribution::experimental::distributeTablePartitions(local_context.get(), partitions_to_send);

				if (sequence.wait_for_next()){
					batch = sequence.next();
					batch_count++;
				} else {
					done = true;
				}
			} catch(const std::exception& e) {
				// TODO add retry here
				std::string err = "ERROR: in JoinPartitionKernel batch_count " + std::to_string(batch_count) + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
        }
		//printf("... notifyLastTablePartitions\n");
		ral::distribution::experimental::notifyLastTablePartitions(local_context.get(), ColumnDataPartitionMessage::MessageID());
    }

	std::pair<bool, bool> determine_if_we_are_scattering_a_small_table(const ral::frame::BlazingTableView & left_batch_view, 
		const ral::frame::BlazingTableView & right_batch_view ){

		std::pair<bool, uint64_t> left_num_rows_estimate = this->query_graph->get_estimated_input_rows_to_cache(this->kernel_id, "input_a");	
		std::pair<bool, uint64_t> right_num_rows_estimate = this->query_graph->get_estimated_input_rows_to_cache(this->kernel_id, "input_b");	
		double left_batch_rows = (double)left_batch_view.num_rows();
		double left_batch_bytes = (double)ral::utilities::experimental::get_table_size_bytes(left_batch_view);
		double right_batch_rows = (double)right_batch_view.num_rows();
		double right_batch_bytes = (double)ral::utilities::experimental::get_table_size_bytes(right_batch_view);
		int64_t left_bytes_estimate;
		if (!left_num_rows_estimate.first || left_batch_bytes == 0){
			// if we cant get a good estimate of current bytes, then we will set to -1 to signify that
			left_bytes_estimate = -1;
		} else {
			left_bytes_estimate = (int64_t)(left_batch_bytes*(((double)left_num_rows_estimate.second)/left_batch_rows));
		}
		int64_t right_bytes_estimate;
		if (!right_num_rows_estimate.first || right_batch_bytes == 0){
			// if we cant get a good estimate of current bytes, then we will set to -1 to signify that
			right_bytes_estimate = -1;
		} else {
			right_bytes_estimate = (int64_t)(right_batch_bytes*(((double)right_num_rows_estimate.second)/right_batch_rows));
		}

		int self_node_idx = context->getNodeIndex(ral::communication::experimental::CommunicationData::getInstance().getSelfNode());

		context->incrementQuerySubstep();
		ral::distribution::experimental::distributeLeftRightTableSizeBytes(context.get(), left_bytes_estimate, right_bytes_estimate);
		std::vector<int64_t> nodes_num_bytes_left;
		std::vector<int64_t> nodes_num_bytes_right;
		ral::distribution::experimental::collectLeftRightTableSizeBytes(context.get(), nodes_num_bytes_left, nodes_num_bytes_right);
		nodes_num_bytes_left[self_node_idx] = left_bytes_estimate;
		nodes_num_bytes_right[self_node_idx] = right_bytes_estimate;

		bool any_unknowns_left = std::any_of(nodes_num_bytes_left.begin(), nodes_num_bytes_left.end(), [](int64_t bytes){return bytes == -1;});
		bool any_unknowns_right = std::any_of(nodes_num_bytes_right.begin(), nodes_num_bytes_right.end(), [](int64_t bytes){return bytes == -1;});

		if (any_unknowns_left || any_unknowns_right){
			return std::make_pair(false, false); // we wont do any small table scatter if we have unknowns
		}

		int64_t total_bytes_left = std::accumulate(nodes_num_bytes_left.begin(), nodes_num_bytes_left.end(), int64_t(0));
		int64_t total_bytes_right = std::accumulate(nodes_num_bytes_right.begin(), nodes_num_bytes_right.end(), int64_t(0));

		int num_nodes = context->getTotalNodes();

		bool scatter_left = false;
		bool scatter_right = false;
		int64_t estimate_regular_distribution = (total_bytes_left + total_bytes_right) * (num_nodes - 1) / num_nodes;
		int64_t estimate_scatter_left = (total_bytes_left) * (num_nodes - 1);
		int64_t estimate_scatter_right = (total_bytes_right) * (num_nodes - 1);
		int64_t MAX_SCATTER_MEM_OVERHEAD = 500000000;  // 500Mb  how much extra memory consumption per node are we ok with
													// WSM TODO get this value from config

		if(estimate_scatter_left < estimate_regular_distribution ||
			estimate_scatter_right < estimate_regular_distribution) {
			if(estimate_scatter_left < estimate_scatter_right &&
				total_bytes_left < MAX_SCATTER_MEM_OVERHEAD) {
				scatter_left = true;
			} else if(estimate_scatter_right < estimate_scatter_left &&
					total_bytes_right < MAX_SCATTER_MEM_OVERHEAD) {
				scatter_right = true;
			}
		}
		return std::make_pair(scatter_left, scatter_right);
	}

	void perform_standard_hash_partitioning(const std::string & condition, 
		std::unique_ptr<ral::frame::BlazingTable> left_batch,
		std::unique_ptr<ral::frame::BlazingTable> right_batch,
		BatchSequence left_sequence,
		BatchSequence right_sequence){
		
		this->context->incrementQuerySubstep();
				
		{ // parsing more of the expression here because we need to have the number of columns of the tables
			std::vector<int> column_indices;
			ral::processor::parseJoinConditionToColumnIndices(condition, column_indices);
			for(int i = 0; i < column_indices.size();i++){
				if(column_indices[i] >= left_batch->num_columns()){
					this->right_column_indices.push_back(column_indices[i] - left_batch->num_columns());
				}else{
					this->left_column_indices.push_back(column_indices[i]);
				}
			}
		}

		BlazingMutableThread distribute_left_thread(&JoinPartitionKernel::partition_table, this->context, 
			this->left_column_indices, std::move(left_batch), std::ref(left_sequence), 
			std::ref(this->output_.get_cache("output_a")));

		BlazingThread left_consumer([context = this->context, this](){
			auto  message_token = ColumnDataPartitionMessage::MessageID() + "_" + this->context->getContextCommunicationToken();
			ExternalBatchColumnDataSequence external_input_left(this->context, message_token);			
			std::unique_ptr<ral::frame::BlazingHostTable> host_table;

			while (host_table = external_input_left.next()) {	
				this->add_to_output_cache(std::move(host_table), "output_a");
			}
		});
		
		// clone context, increment step counter to make it so that the next partition_table will have different message id
		auto cloned_context = context->clone();
		cloned_context->incrementQuerySubstep();

		BlazingMutableThread distribute_right_thread(&JoinPartitionKernel::partition_table, cloned_context, 
			this->right_column_indices, std::move(right_batch), std::ref(right_sequence), 
			std::ref(this->output_.get_cache("output_b")));

		// create thread with ExternalBatchColumnDataSequence for the right table being distriubted
		BlazingThread right_consumer([cloned_context, this](){
			auto message_token = ColumnDataPartitionMessage::MessageID() + "_" + cloned_context->getContextCommunicationToken();
			ExternalBatchColumnDataSequence external_input_right(cloned_context, message_token);
			std::unique_ptr<ral::frame::BlazingHostTable> host_table;
			
			while (host_table = external_input_right.next()) {
				this->add_to_output_cache(std::move(host_table), "output_b");
			}
		});
	
		distribute_left_thread.join();
		left_consumer.join();
		distribute_right_thread.join();
		right_consumer.join();
	}

	void small_table_scatter_distribution(std::unique_ptr<ral::frame::BlazingTable> small_table_batch,
		std::unique_ptr<ral::frame::BlazingTable> big_table_batch,
		BatchSequence small_table_sequence,
		BatchSequenceBypass big_table_sequence, 
		const std::pair<bool, bool> & scatter_left_right){

		this->context->incrementQuerySubstep();

		// In this function we are assuming that one and only one of the two bools in scatter_left_right is true
		assert((scatter_left_right.first || scatter_left_right.second) && not (scatter_left_right.first && scatter_left_right.second)); 
		
		std::string small_output_cache_name = scatter_left_right.first ? "output_a" : "output_b";
		std::string big_output_cache_name = scatter_left_right.first ? "output_b" : "output_a";

		BlazingThread distribute_small_table_thread([this, &small_table_batch, &small_table_sequence, small_output_cache_name](){
			bool done = false;
			int batch_count = 0;
			while (!done) {		
				try {  
					if(small_table_batch->num_rows() > 0) {
						ral::distribution::experimental::scatterData(this->context.get(), small_table_batch->toBlazingTableView());
					}
					this->add_to_output_cache(std::move(small_table_batch), small_output_cache_name);
					if (small_table_sequence.wait_for_next()){
						small_table_batch = small_table_sequence.next();
						batch_count++;
					} else {
						done = true;
					}
				} catch(const std::exception& e) {
					// TODO add retry here
					std::string err = "ERROR: in JoinPartitionKernel scatter_small_table batch_count " + std::to_string(batch_count) + " Error message: " + std::string(e.what());
					std::cout<<err<<std::endl;
				}
			}
			ral::distribution::experimental::notifyLastTablePartitions(this->context.get(), ColumnDataMessage::MessageID());
		});
		
		BlazingThread collect_small_table_thread([this, small_output_cache_name](){
			auto  message_token = ColumnDataMessage::MessageID() + "_" + this->context->getContextCommunicationToken();
			ExternalBatchColumnDataSequence external_input_left(this->context, message_token);
			
			while (external_input_left.wait_for_next()) {	
				std::unique_ptr<ral::frame::BlazingHostTable> host_table = external_input_left.next();
				this->add_to_output_cache(std::move(host_table), small_output_cache_name);
			}
		});

		this->add_to_output_cache(std::move(big_table_batch), big_output_cache_name);

		BlazingThread big_table_passthrough_thread([this, &big_table_sequence, big_output_cache_name](){
			while (big_table_sequence.wait_for_next()) {	
				auto batch = big_table_sequence.next();
				this->add_to_output_cache(std::move(batch), big_output_cache_name);
			}
		});
		
		distribute_small_table_thread.join();
		collect_small_table_thread.join();
		big_table_passthrough_thread.join();		
	}
	
	virtual kstatus run() {
		
		BatchSequence left_sequence(this->input_.get_cache("input_a"), this);
		BatchSequence right_sequence(this->input_.get_cache("input_b"), this);

		// lets parse part of the expression here, because we need the joinType before we load
		std::string new_join_statement, filter_statement;
		StringUtil::findAndReplaceAll(this->expression, "IS NOT DISTINCT FROM", "=");
		split_inequality_join_into_join_and_filter(this->expression, new_join_statement, filter_statement);
		std::string condition = get_named_expression(new_join_statement, "condition");
		this->join_type = get_named_expression(new_join_statement, "joinType");

		std::unique_ptr<ral::frame::BlazingTable> left_batch = left_sequence.next();
		std::unique_ptr<ral::frame::BlazingTable> right_batch = right_sequence.next();

		if (left_batch == nullptr || left_batch->num_columns() == 0){
			std::cout<<"ERROR JoinPartitionKernel has empty left side and cannot determine join column indices"<<std::endl;
		}

		std::pair<bool, bool> scatter_left_right;
		if (this->join_type == OUTER_JOIN){ // cant scatter a full outer join
			scatter_left_right = std::make_pair(false, false);
		} else {
			scatter_left_right = determine_if_we_are_scattering_a_small_table(left_batch->toBlazingTableView(), 
																				right_batch->toBlazingTableView());
			if (scatter_left_right.first && this->join_type == LEFT_JOIN){
				scatter_left_right.first = false; // cant scatter the left side for a left outer join
			}
		}
		// scatter_left_right = std::make_pair(false, false); // Do this for debugging if you want to disable small table join optmization
		if (scatter_left_right.first){
			BatchSequenceBypass big_table_sequence(this->input_.get_cache("input_b"));
			small_table_scatter_distribution( std::move(left_batch), std::move(right_batch),
						std::move(left_sequence), std::move(big_table_sequence), scatter_left_right);
		} else if (scatter_left_right.second) {
			BatchSequenceBypass big_table_sequence(this->input_.get_cache("input_a"));
			small_table_scatter_distribution( std::move(right_batch), std::move(left_batch),
						std::move(right_sequence), std::move(big_table_sequence), scatter_left_right);
		} else {
			perform_standard_hash_partitioning(condition, std::move(left_batch), std::move(right_batch),
				std::move(left_sequence), std::move(right_sequence));
		}		
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
	
	// parsed expression related parameters
	std::string join_type;
	std::vector<cudf::size_type> left_column_indices, right_column_indices;
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
