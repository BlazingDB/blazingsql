#pragma once

#include "BatchProcessing.h"
#include "TaskFlowProcessor.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "operators/GroupBy.h"

#include <cudf/hashing.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;



class ComputeGroupByKernel : public PhysicalPlan {
public:
	ComputeGroupByKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		// this->input_.add_port("input_a"); // WSM only need one port here, do I need to do this?
	}

	virtual kstatus run() {

        std::vector<int> group_column_indices;
        std::vector<std::string> aggregation_input_expressions;
        std::vector<AggregateKind> aggregation_types;
        std::vector<std::string> aggregation_column_assigned_aliases;
        std::tie(group_column_indices, aggregation_input_expressions, aggregation_types, 
            aggregation_column_assigned_aliases) = ral::operators::experimental::parseGroupByExpression(this->expression);

		BatchSequence input(this->input_.get_cache("input_a"));
		while (input.has_next()) {
			auto batch = input.next();

            std::unique_ptr<ral::frame::BlazingTable> output;
            if (aggregation_types.size() > 0){			
                std::unique_ptr<ral::frame::BlazingTable> output = ral::operators::experimental::compute_aggregations_with_groupby(
                        batch->toBlazingTableView(), aggregation_types, aggregation_input_expressions, aggregation_column_assigned_aliases, group_column_indices);
            } else {
                std::unique_ptr<ral::frame::BlazingTable> output = ral::operators::experimental::compute_groupby_without_aggregations(
                        batch->toBlazingTableView(), group_column_indices);
            }
            this->output_.get_cache()->addToCache(std::move(output));
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
		// this->input_.add_port("input_a"); // WSM only need one port here, do I need to do this?
	}

	virtual kstatus run() {
		
		BatchSequence input(this->input_.get_cache("input_a"));
		while (input.has_next()) {
			auto batch = input.next();
			
			// WSM TODO from queryString determine columns_to_hash
            std::vector<cudf::size_type> columns_to_hash;
            
            std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions;
			
            std::unique_ptr<CudfTable> hashed_data;
            std::vector<cudf::size_type> hased_data_offsets;
            int num_partitions = context->getTotalNodes(); // WSM this will do for now, but may want a function to determine this in the future
            std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch->view(), columns_to_hash, num_partitions);

            // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
            std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
            std::vector<CudfTableView> partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);

            //   WSM TODO ADD SHUFFLE HERE AND ADD TO CACHE
			
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};


class GroupByWithAggMergeKernel : public PhysicalPlan {
public:
	GroupByWithAggMergeKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		// this->input_.add_port("input_a"); // WSM only need one port here, do I need to do this?
	}

	virtual kstatus run() {

        std::vector<std::unique_ptr<ral::frame::BlazingTable>> tablesToConcat;
		std::vector<ral::frame::BlazingTableView> tableViewsToConcat;

        if (ready_to_execute()){
            BatchSequence input(this->input_.get_cache("input_a"));
            while (input.has_next()) {
                auto batch = input.next();
                tableViewsToConcat.emplace_back(batch->toBlazingTableView());
                tablesToConcat.emplace_back(std::move(batch));
            }
            auto concatenated = ral::utilities::experimental::concatTables(tableViewsToConcat);
                    
            std::vector<int> group_column_indices;
            std::vector<std::string> aggregation_input_expressions;
            std::vector<AggregateKind> aggregation_types;
            std::vector<std::string> aggregation_column_assigned_aliases;
            std::tie(group_column_indices, aggregation_input_expressions, aggregation_types, 
                aggregation_column_assigned_aliases) = ral::operators::experimental::parseGroupByExpression(this->expression);

            std::vector<int> mod_group_column_indices;
            std::vector<std::string> mod_aggregation_input_expressions;
            std::vector<AggregateKind> mod_aggregation_types;
            std::vector<std::string> mod_aggregation_column_assigned_aliases;
            std::vector<std::string> merging_column_names;
            std::tie(mod_group_column_indices, mod_aggregation_input_expressions, mod_aggregation_types, 
                mod_aggregation_column_assigned_aliases) = ral::operators::experimental::modGroupByParametersForMerge(
                group_column_indices, aggregation_types, concatenated->names());

            std::unique_ptr<ral::frame::BlazingTable> output;
            if (mod_aggregation_types.size() > 0){			
                output = ral::operators::experimental::compute_aggregations_with_groupby(
                        concatenated->toBlazingTableView(), mod_aggregation_types, mod_aggregation_input_expressions, 
                        mod_aggregation_column_assigned_aliases, mod_group_column_indices);
            } else {
                output = ral::operators::experimental::compute_groupby_without_aggregations(
                        concatenated->toBlazingTableView(), mod_group_column_indices);
            }
            this->output_.get_cache()->addToCache(std::move(output));
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
    ComputeGroupByKernel
    GroupByWithAggMergeKernel
- multi node
    ComputeGroupByKernel
    HashPartitionKernel
    GroupByWithAggMergeKernel

groupbywoagg becomes:
- single node
    ComputeGroupByKernel
    GroupByWithAggMergeKernel
- multi node
    ComputeGroupByKernel
    HashPartitionKernel
    GroupByWithAggMergeKernel

aggwogroupby becomes:
- single node
    ComputeAggWithoutGroupByKernel
    MergeAggWithoutGroupByKernel
- multi node
    ComputeAggWithoutGroupByKernel
    SendToMasterKernel
    MergeAggWithoutGroupByKernel








*/