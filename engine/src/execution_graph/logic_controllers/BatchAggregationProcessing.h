#pragma once

#include "BatchProcessing.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "operators/GroupBy.h"
#include "distribution/primitives.h"
#include "utilities/DebuggingUtils.h"

#include <cudf/partitioning.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;



class ComputeAggregateKernel :public kernel {
public:
	ComputeAggregateKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
        this->query_graph = query_graph;
	}

	virtual kstatus run() {

        std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases;
        std::tie(this->group_column_indices, aggregation_input_expressions, this->aggregation_types, 
            aggregation_column_assigned_aliases) = ral::operators::experimental::parseGroupByExpression(this->expression);

		BatchSequence input(this->input_cache(), this);
        int batch_count = 0;
        while (input.wait_for_next()) {
			auto batch = input.next();

            try {
                std::unique_ptr<ral::frame::BlazingTable> output;
                if(this->aggregation_types.size() == 0) {
                    output = ral::operators::experimental::compute_groupby_without_aggregations(
                            batch->toBlazingTableView(), this->group_column_indices);
                } else if (this->group_column_indices.size() == 0) {
                    output = ral::operators::experimental::compute_aggregations_without_groupby(
                            batch->toBlazingTableView(), aggregation_input_expressions, this->aggregation_types, aggregation_column_assigned_aliases);                
                } else {
                    output = ral::operators::experimental::compute_aggregations_with_groupby(
                        batch->toBlazingTableView(), aggregation_input_expressions, this->aggregation_types, aggregation_column_assigned_aliases, group_column_indices);
                }
                
                this->add_to_output_cache(std::move(output));
                batch_count++;
            } catch(const std::exception& e) {
                // TODO add retry here
    			std::string err = "ERROR: in ComputeAggregateKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
                std::cout<<err<<std::endl;
            }
		}
        // std::cout<<"ComputeAggregateKernel end "<<std::endl;
		return kstatus::proceed;
	}

    std::pair<bool, uint64_t> get_estimated_output_num_rows(){
        if(this->aggregation_types.size() > 0 && this->group_column_indices.size() == 0) { // aggregation without groupby
            return std::make_pair(true, 1);
        } else {
            std::pair<bool, uint64_t> total_in = this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
            if (total_in.first){
                double out_so_far = (double)this->output_.total_rows_added();
                double in_so_far = (double)this->input_.total_rows_added();
                if (in_so_far == 0){
                    return std::make_pair(false, 0);    
                } else {
                    return std::make_pair(true, (uint64_t)( ((double)total_in.second) *out_so_far/in_so_far) );
                }
            } else {
                return std::make_pair(false, 0);
            }
        }
    }

private:
	std::shared_ptr<Context> context;
	std::string expression;
    std::vector<AggregateKind> aggregation_types;
    std::vector<int> group_column_indices;
};



class DistributeAggregateKernel :public kernel {
public:
	DistributeAggregateKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
        this->query_graph = query_graph;
	}

	virtual kstatus run() {
		
        std::vector<int> group_column_indices;
        std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases; // not used in this kernel
        std::vector<AggregateKind> aggregation_types; // not used in this kernel
        std::tie(group_column_indices, aggregation_input_expressions, aggregation_types, 
            aggregation_column_assigned_aliases) = ral::operators::experimental::parseGroupByExpression(this->expression);

        std::vector<cudf::size_type> columns_to_hash;
        std::transform(group_column_indices.begin(), group_column_indices.end(), std::back_inserter(columns_to_hash), [](int index) { return (cudf::size_type)index; });
        

        BlazingThread producer_thread([this, group_column_indices, columns_to_hash](){
            // num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future. 
            // If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
            int num_partitions = this->context->getTotalNodes(); 
            bool set_empty_part_for_non_master_node = false; // this is only for aggregation without group by

            BatchSequence input(this->input_cache(), this);
            int batch_count = 0;
            while (input.wait_for_next()) {
                auto batch = input.next();

                try {
                    //std::cout<<"DistributeAggregateKernel batch "<<batch_count<<std::endl;

                    // If its an aggregation without group by we want to send all the results to the master node
                    if (group_column_indices.size() == 0) {
                        if(this->context->isMasterNode(ral::communication::experimental::CommunicationData::getInstance().getSelfNode())) {
                            this->add_to_output_cache(std::move(batch));
                        } else {  
                            if (!set_empty_part_for_non_master_node){ // we want to keep in the non-master nodes something, so that the cache is not empty
                                std::unique_ptr<ral::frame::BlazingTable> empty = 
                                    ral::utilities::experimental::create_empty_table(batch->toBlazingTableView());
                                this->add_to_output_cache(std::move(empty));
                                set_empty_part_for_non_master_node = true;
                            }
                            std::vector<ral::distribution::experimental::NodeColumnView> selfPartition;
                            selfPartition.emplace_back(this->context->getMasterNode(), batch->toBlazingTableView());
                            ral::distribution::experimental::distributeTablePartitions(this->context.get(), selfPartition);
                        }
                    } else {
                        CudfTableView batch_view = batch->view();
                        std::vector<CudfTableView> partitioned;
                        std::unique_ptr<CudfTable> hashed_data; // Keep table alive in this scope
                        if (batch_view.num_rows() > 0) {
                            std::vector<cudf::size_type> hased_data_offsets;
                            std::tie(hashed_data, hased_data_offsets) = cudf::experimental::hash_partition(batch->view(), columns_to_hash, num_partitions);
                            // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
                            std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
                            partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);
                        } else {
                            //  copy empty view
                            for (auto i = 0; i < num_partitions; i++) {
                                partitioned.push_back(batch_view);
                            }
                        }

                        std::vector<ral::distribution::experimental::NodeColumnView > partitions_to_send;
                        for(int nodeIndex = 0; nodeIndex < this->context->getTotalNodes(); nodeIndex++ ){
                            ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
                            if (this->context->getNode(nodeIndex) == ral::communication::experimental::CommunicationData::getInstance().getSelfNode()){
                                // hash_partition followed by split does not create a partition that we can own, so we need to clone it.
                                // if we dont clone it, hashed_data will go out of scope before we get to use the partition
                                // also we need a BlazingTable to put into the cache, we cant cache views.
                                std::unique_ptr<ral::frame::BlazingTable> partition_table_clone = partition_table_view.clone();
                                this->add_to_output_cache(std::move(partition_table_clone));
                            } else {
                                partitions_to_send.emplace_back(
                                    std::make_pair(this->context->getNode(nodeIndex), partition_table_view));
                            }
                        }
                        ral::distribution::experimental::distributeTablePartitions(this->context.get(), partitions_to_send);			
                    }
                    batch_count++;
                } catch(const std::exception& e) {
                    // TODO add retry here
                    std::string err = "ERROR: in DistributeAggregateKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
                    std::cout<<err<<std::endl;
                }
            }

            if (!(group_column_indices.size() == 0
                && this->context->isMasterNode(ral::communication::experimental::CommunicationData::getInstance().getSelfNode()))) {
                // Aggregations without groupby does not send distributeTablePartitions
                ral::distribution::experimental::notifyLastTablePartitions(this->context.get(), ColumnDataPartitionMessage::MessageID());
            }
        });
        
        BlazingThread consumer_thread([this, group_column_indices](){
            // Lets put the server listener to feed the output, but not if its aggregations without group by and its not the master
            if(group_column_indices.size() > 0 || 
                        this->context->isMasterNode(ral::communication::experimental::CommunicationData::getInstance().getSelfNode())) {
                ExternalBatchColumnDataSequence external_input(context);
                std::unique_ptr<ral::frame::BlazingHostTable> host_table;
                while (host_table = external_input.next()) {
                    this->add_to_output_cache(std::move(host_table));
                }
            }
        });
        producer_thread.join();
        consumer_thread.join();
        // std::cout<<"DistributeAggregateKernel end "<<std::endl;
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};


class MergeAggregateKernel :public kernel {
public:
	MergeAggregateKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
        this->query_graph = query_graph;
	}

	virtual kstatus run() {

        std::vector<std::unique_ptr<ral::frame::BlazingTable>> tablesToConcat;
		std::vector<ral::frame::BlazingTableView> tableViewsToConcat;

        if (ready_to_execute()){
            BatchSequence input(this->input_cache(), this);
            int batch_count=0;
            try {
                while (input.wait_for_next()) {
                    auto batch = input.next();
                    // std::cout<<"MergeAggregateKernel batch "<<batch_count<<std::endl;
                    // ral::utilities::print_blazing_table_view_schema(batch->toBlazingTableView(), "MergeAggregateKernel_batch" + std::to_string(batch_count));
                    batch_count++;
                    tableViewsToConcat.emplace_back(batch->toBlazingTableView());
                    tablesToConcat.emplace_back(std::move(batch));
                }
                auto concatenated = ral::utilities::experimental::concatTables(tableViewsToConcat);
                        
                std::vector<int> group_column_indices;
                std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases;
                std::vector<AggregateKind> aggregation_types;
                std::tie(group_column_indices, aggregation_input_expressions, aggregation_types, 
                    aggregation_column_assigned_aliases) = ral::operators::experimental::parseGroupByExpression(this->expression);

                std::vector<int> mod_group_column_indices;
                std::vector<std::string> mod_aggregation_input_expressions, mod_aggregation_column_assigned_aliases, merging_column_names;
                std::vector<AggregateKind> mod_aggregation_types;
                std::tie(mod_group_column_indices, mod_aggregation_input_expressions, mod_aggregation_types, 
                    mod_aggregation_column_assigned_aliases) = ral::operators::experimental::modGroupByParametersForMerge(
                    group_column_indices, aggregation_types, concatenated->names());

                std::unique_ptr<ral::frame::BlazingTable> output;
                if(aggregation_types.size() == 0) {
                    output = ral::operators::experimental::compute_groupby_without_aggregations(
                            concatenated->toBlazingTableView(), mod_group_column_indices);
                } else if (group_column_indices.size() == 0) {
                    // aggregations without groupby are only merged on the master node
                    if(context->isMasterNode(ral::communication::experimental::CommunicationData::getInstance().getSelfNode())) {
                        output = ral::operators::experimental::compute_aggregations_without_groupby(
                                concatenated->toBlazingTableView(), mod_aggregation_input_expressions, mod_aggregation_types, 
                                mod_aggregation_column_assigned_aliases);
                    } else {
                        // with aggregations without groupby the distribution phase should deposit an empty dataframe with the right schema into the cache, which is then output here
                        output = std::move(concatenated);
                    }
                } else {
                    output = ral::operators::experimental::compute_aggregations_with_groupby(
                            concatenated->toBlazingTableView(), mod_aggregation_input_expressions, mod_aggregation_types,
                            mod_aggregation_column_assigned_aliases, mod_group_column_indices);
                }
                // ral::utilities::print_blazing_table_view_schema(output->toBlazingTableView(), "MergeAggregateKernel_output");
                this->add_to_output_cache(std::move(output));
             } catch(const std::exception& e) {
                // TODO add retry here
    			std::string err = "ERROR: in MergeAggregateKernel for " + expression + " Error message: " + std::string(e.what());
                std::cout<<err<<std::endl;
            }
        }
		// std::cout<<"MergeAggregateKernel end "<<std::endl;
		return kstatus::proceed;
	}

    bool ready_to_execute() {
        // WSM TODO: in this function we want to wait until all batch inputs are available
        return true;
    }

private:
	std::shared_ptr<Context> context;
	std::string expression;
};





} // namespace batch
} // namespace ral



/*
- single node
    ComputeAggregateKernel
    MergeAggregateKernel
- multi node
    ComputeAggregateKernel
    DistributeAggregateKernel
    MergeAggregateKernel
*/
