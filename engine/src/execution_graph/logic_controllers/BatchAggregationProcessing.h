#pragma once

#include "BatchProcessing.h"
#include "TaskFlowProcessor.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "operators/GroupBy.h"
#include "distribution/primitives.h"

#include <cudf/hashing.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;



class ComputeAggregationKernel : public PhysicalPlan {
public:
	ComputeAggregationKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
	}

	virtual kstatus run() {

        std::vector<int> group_column_indices;
        std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases;
        std::vector<AggregateKind> aggregation_types;
        std::tie(group_column_indices, aggregation_input_expressions, aggregation_types, 
            aggregation_column_assigned_aliases) = ral::operators::experimental::parseGroupByExpression(this->expression);

		BatchSequence input(this->input_cache());
		while (input.has_next()) {
			auto batch = input.next();

            std::unique_ptr<ral::frame::BlazingTable> output;
            if(aggregation_types.size() == 0) {
                output = ral::operators::experimental::compute_groupby_without_aggregations(
                        batch->toBlazingTableView(), group_column_indices);
            } else if (group_column_indices.size() == 0) {
                output = ral::operators::experimental::compute_aggregations_without_groupby(
                        batch->toBlazingTableView(), aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases);                
            } else {
                output = ral::operators::experimental::compute_aggregations_with_groupby(
                    batch->toBlazingTableView(), aggregation_input_expressions, aggregation_types, aggregation_column_assigned_aliases, group_column_indices);
            }
            this->output_cache()->addToCache(std::move(output));
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};



class HashPartitionKernel : public PhysicalPlan {
public:
	HashPartitionKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
	}

	virtual kstatus run() {
		
		BatchSequence input(this->input_cache());
		while (input.has_next()) {
			auto batch = input.next();
			
			// WSM TODO from queryString determine columns_to_hash
            std::vector<cudf::size_type> columns_to_hash;
            
            // WSM num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future. 
            // If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
            int num_partitions = context->getTotalNodes(); 
            std::unique_ptr<CudfTable> hashed_data;
            std::vector<cudf::size_type> hased_data_offsets;
            std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch->view(), columns_to_hash, num_partitions);

            // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
            std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
            std::vector<CudfTableView> partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);

            std::vector<std::unique_ptr<ral::frame::BlazingTable>> self_partitions;
            std::vector<ral::distribution::experimental::NodeColumnView > partitions_to_send;
            for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
                ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
                if (context->getNode(nodeIndex) == ral::communication::experimental::CommunicationData::getInstance().getSelfNode()){
                    // hash_partition followed by split does not create a partition that we can own, so we need to clone it.
                    // if we dont clone it, hashed_data will go out of scope before we get to use the partition
                    /* WSM TODO. Lets make it so that inserting a table into a cache will do the following:
                    - if its going to move it off gpu, then continue as usual
                    - if its going to keep it in GPU, then see if it owns the data, if it does not, it should make a clone.
                    when we implement this, we can remove this clone here
                    */
                    std::unique_ptr<ral::frame::BlazingTable> partition_table_clone = partition_table_view.clone();
                    self_partitions.emplace_back(std::move(partition_table_clone));
                } else {
                    partitions_to_send.emplace_back(
                        std::make_pair(context->getNode(nodeIndex), partition_table_view));
                }
            }
			
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};


class AggregationMergeKernel : public PhysicalPlan {
public:
	AggregationMergeKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
	}

	virtual kstatus run() {

        std::vector<std::unique_ptr<ral::frame::BlazingTable>> tablesToConcat;
		std::vector<ral::frame::BlazingTableView> tableViewsToConcat;

        if (ready_to_execute()){
            BatchSequence input(this->input_cache());
            while (input.has_next()) {
                auto batch = input.next();
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
            this->output_cache()->addToCache(std::move(output));
        }
		
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
aggwithgroupby becomes:
- single node
    ComputeAggregationKernel
    AggregationMergeKernel
- multi node
    ComputeAggregationKernel
    HashPartitionKernel
    AggregationMergeKernel

groupbywoagg becomes:
- single node
    ComputeAggregationKernel
    AggregationMergeKernel
- multi node
    ComputeAggregationKernel
    HashPartitionKernel
    AggregationMergeKernel

aggwogroupby becomes:
- single node
    ComputeAggregationKernel
    MergeAggWithoutGroupByKernel
- multi node
    ComputeAggregationKernel
    SendToMasterKernel
    AggregationMergeKernel








*/