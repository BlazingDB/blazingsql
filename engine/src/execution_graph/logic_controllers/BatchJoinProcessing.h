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
#include "CodeTimer.h"
#include <cudf/stream_compaction.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/join.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

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
	PartwiseJoin(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}, left_sequence{nullptr, this}, right_sequence{nullptr, this} {
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
		if (not left_schema) {
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
		if (not right_schema) {
			right_schema = std::make_unique<TableSchema>(table->get_schema(),  table->names());
		}
		if (table == nullptr) {
			return ral::frame::createEmptyBlazingTable(right_schema->column_types, right_schema->column_names);
		}
		return std::move(table);
	}

    void mark_set_completed(int left_ind, int right_ind){
		if (completion_matrix.size() <= left_ind){
			completion_matrix.resize(left_ind + 1);
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
		CodeTimer timer;

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
				logger->error("In PartwiseJoin kernel left_idx[{}] right_ind[{}] for {}. What: {}", left_ind, right_ind, expression, e.what());
				throw e;
			}
		}
		
		if (!produced_output){
			logger->warn("PartwiseJoin kernel did not produce an output");
			// WSM TODO put an empty output into output cache
		}

		logger->debug("PartwiseJoin Kernel [{}] Completed in [{}] ms", this->get_id(), timer.elapsed_time());
		
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
	JoinPartitionKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {

		this->input_.add_port("input_a", "input_b");
		this->output_.add_port("output_a", "output_b");
	}


    static void partition_table(std::shared_ptr<Context> context, 
				std::vector<cudf::size_type> column_indices, 
				std::unique_ptr<ral::frame::BlazingTable> batch, 
				BatchSequence & sequence, 
				std::shared_ptr<ral::cache::CacheMachine> & output,
				const std::string & message_id){

        bool done = false;
		// num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future. 
		// If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
		int num_partitions = context->getTotalNodes(); 
		std::unique_ptr<CudfTable> hashed_data;
		std::vector<cudf::size_type> hased_data_offsets;
		int batch_count = 0;
        while (!done) {		
            try {            
				//std::cout << "\tbatch.sz: " << batch->num_rows() << std::endl;
				auto batch_view = batch->view();
				std::vector<CudfTableView> partitioned;
				if (batch->num_rows() > 0) {
					std::tie(hashed_data, hased_data_offsets) = cudf::experimental::hash_partition(batch_view, column_indices, num_partitions);

					assert(hased_data_offsets.begin() != hased_data_offsets.end());
					// the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
					std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
					partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);
					
				} else {
					for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
						partitioned.push_back(batch_view);
					}
				}
				std::vector<ral::distribution::experimental::NodeColumnView > partitions_to_send;
				for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
					ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
					if (context->getNode(nodeIndex) == ral::communication::experimental::CommunicationData::getInstance().getSelfNode()){
						// hash_partition followed by split does not create a partition that we can own, so we need to clone it.
						// if we dont clone it, hashed_data will go out of scope before we get to use the partition
						// also we need a BlazingTable to put into the cache, we cant cache views.
						std::unique_ptr<ral::frame::BlazingTable> partition_table_clone = partition_table_view.clone();

						// TODO: create message id and send to add add_to_output_cache
						output->addToCache(std::move(partition_table_clone), message_id);
					} else {
						partitions_to_send.emplace_back(
							std::make_pair(context->getNode(nodeIndex), partition_table_view));
					}
				}
				ral::distribution::experimental::distributeTablePartitions(context.get(), partitions_to_send);

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
		ral::distribution::experimental::notifyLastTablePartitions(context.get());
    }
	
	virtual kstatus run() {
		CodeTimer timer;

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
			logger->error("JoinPartitionKernel has empty left side and cannot determine join column indices");
		}
		
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

		BlazingMutableThread distribute_left_thread(&JoinPartitionKernel::partition_table, context, 
			this->left_column_indices, std::move(left_batch), std::ref(left_sequence), 
			std::ref(this->output_.get_cache("output_a")), "output_a_" + this->get_message_id());

		BlazingThread left_consumer([context = this->context, this](){
			ExternalBatchColumnDataSequence external_input_left(this->context, this->get_message_id());
			std::unique_ptr<ral::frame::BlazingHostTable> host_table;
			//std::cout << "... consumming left => "<< this->get_message_id()<<std::endl;
			while (host_table = external_input_left.next()) {	
				this->add_to_output_cache(std::move(host_table), "output_a");
			}
		});
		
		distribute_left_thread.join();
		left_consumer.join();

		// clone context, increment step counter to make it so that the next partition_table will have different message id
		auto cloned_context = context->clone();
		cloned_context->incrementQuerySubstep();

		BlazingMutableThread distribute_right_thread(&JoinPartitionKernel::partition_table, cloned_context, 
			this->right_column_indices, std::move(right_batch), std::ref(right_sequence), 
			std::ref(this->output_.get_cache("output_b")), "output_b_" + this->get_message_id());

		// create thread with ExternalBatchColumnDataSequence for the right table being distriubted
		BlazingThread right_consumer([cloned_context, this](){
			ExternalBatchColumnDataSequence external_input_right(cloned_context, this->get_message_id());
			std::unique_ptr<ral::frame::BlazingHostTable> host_table;

			//std::cout << "... consumming right => "<< this->get_message_id()<<std::endl;

			while (host_table = external_input_right.next()) {	
				this->add_to_output_cache(std::move(host_table), "output_b");
			}
		});
	
		distribute_right_thread.join();
		right_consumer.join();
		
		logger->debug("JoinPartition Kernel [{}] Completed in [{}] ms", this->get_id(), timer.elapsed_time());

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
