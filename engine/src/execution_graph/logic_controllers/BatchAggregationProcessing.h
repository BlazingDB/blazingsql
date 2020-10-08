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
#include <cudf/partitioning.hpp>
#include "CodeTimer.h"

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;
using namespace fmt::literals;

class ComputeAggregateKernel : public kernel {
public:
	ComputeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::ComputeAggregateKernel} {
        this->query_graph = query_graph;
	}

    bool can_you_throttle_my_input() {
		return true;
	}

	virtual kstatus run() {
		CodeTimer timer;
        CodeTimer eventTimer(false);

        std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases;
        std::tie(this->group_column_indices, aggregation_input_expressions, this->aggregation_types,
            aggregation_column_assigned_aliases) = ral::operators::parseGroupByExpression(this->expression);

        bool ordered = false; // If we start using sort based aggregations this may need to change
        BatchSequence input(this->input_cache(), this, ordered);
        int batch_count = 0;
        while (input.wait_for_next()) {

            this->output_cache()->wait_if_cache_is_saturated();
            auto batch = input.next();

            eventTimer.start();

            auto log_input_num_rows = batch ? batch->num_rows() : 0;
            auto log_input_num_bytes = batch ? batch->sizeInBytes() : 0;

            try {
                std::unique_ptr<ral::frame::BlazingTable> output;
                if(this->aggregation_types.size() == 0) {
                    output = ral::operators::compute_groupby_without_aggregations(
                            batch->toBlazingTableView(), this->group_column_indices);
                } else if (this->group_column_indices.size() == 0) {
                    output = ral::operators::compute_aggregations_without_groupby(
                            batch->toBlazingTableView(), aggregation_input_expressions, this->aggregation_types, aggregation_column_assigned_aliases);
                } else {
                    output = ral::operators::compute_aggregations_with_groupby(
                        batch->toBlazingTableView(), aggregation_input_expressions, this->aggregation_types, aggregation_column_assigned_aliases, group_column_indices);
                }

                eventTimer.stop();

                if(output){
                    auto log_output_num_rows = output->num_rows();
                    auto log_output_num_bytes = output->sizeInBytes();

                    events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                                    "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                                    "query_id"_a=context->getContextToken(),
                                    "kernel_id"_a=this->get_id(),
                                    "input_num_rows"_a=log_input_num_rows,
                                    "input_num_bytes"_a=log_input_num_bytes,
                                    "output_num_rows"_a=log_output_num_rows,
                                    "output_num_bytes"_a=log_output_num_bytes,
                                    "event_type"_a="ComputeAggregateKernel",
                                    "timestamp_begin"_a=eventTimer.start_time(),
                                    "timestamp_end"_a=eventTimer.end_time());
                }

                this->add_to_output_cache(std::move(output));
                batch_count++;
            } catch(const std::exception& e) {
                // TODO add retry here
                logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                            "query_id"_a=context->getContextToken(),
                            "step"_a=context->getQueryStep(),
                            "substep"_a=context->getQuerySubstep(),
                            "info"_a="In ComputeAggregate kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                            "duration"_a="");
                throw;
            }
        }

        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="ComputeAggregate Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());

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
    std::vector<AggregateKind> aggregation_types;
    std::vector<int> group_column_indices;
};

class DistributeAggregateKernel : public kernel {
public:
	DistributeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::DistributeAggregateKernel} {
        this->query_graph = query_graph;
	}

    bool can_you_throttle_my_input() {
		return true;
	}

	virtual kstatus run() {
        using ColumnDataPartitionMessage = ral::communication::messages::ColumnDataPartitionMessage;

        CodeTimer timer;

        std::vector<int> group_column_indices;
        std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases; // not used in this kernel
        std::vector<AggregateKind> aggregation_types; // not used in this kernel
        std::tie(group_column_indices, aggregation_input_expressions, aggregation_types,
            aggregation_column_assigned_aliases) = ral::operators::parseGroupByExpression(this->expression);

        std::vector<cudf::size_type> columns_to_hash;
        std::transform(group_column_indices.begin(), group_column_indices.end(), std::back_inserter(columns_to_hash), [](int index) { return (cudf::size_type)index; });


        BlazingThread producer_thread([this, group_column_indices, columns_to_hash](){
            // num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future.
            // If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
            int num_partitions = this->context->getTotalNodes();
            bool set_empty_part_for_non_master_node = false; // this is only for aggregation without group by

            bool ordered = false; // If we start using sort based aggregations this may need to change
            BatchSequence input(this->input_cache(), this, ordered);
            int batch_count = 0;
            while (input.wait_for_next()) {
                auto batch = input.next();

                try {
                    //std::cout<<"DistributeAggregateKernel batch "<<batch_count<<std::endl;

                    // If its an aggregation without group by we want to send all the results to the master node
                    if (group_column_indices.size() == 0) {
                        if(this->context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode())) {
                            this->add_to_output_cache(std::move(batch));
                        } else {
                            if (!set_empty_part_for_non_master_node){ // we want to keep in the non-master nodes something, so that the cache is not empty
                                std::unique_ptr<ral::frame::BlazingTable> empty =
                                    ral::utilities::create_empty_table(batch->toBlazingTableView());
                                this->add_to_output_cache(std::move(empty));
                                set_empty_part_for_non_master_node = true;
                            }
                            std::vector<ral::distribution::NodeColumnView> selfPartition;
                            selfPartition.emplace_back(this->context->getMasterNode(), batch->toBlazingTableView());
                            ral::distribution::distributeTablePartitions(this->context.get(), selfPartition);
                        }
                    } else {
                        CudfTableView batch_view = batch->view();
                        std::vector<CudfTableView> partitioned;
                        std::unique_ptr<CudfTable> hashed_data; // Keep table alive in this scope
                        if (batch_view.num_rows() > 0) {
                            std::vector<cudf::size_type> hased_data_offsets;
                            std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch->view(), columns_to_hash, num_partitions);
                            // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
                            std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
                            partitioned = cudf::split(hashed_data->view(), split_indexes);
                        } else {
                            //  copy empty view
                            for (auto i = 0; i < num_partitions; i++) {
                                partitioned.push_back(batch_view);
                            }
                        }

                        std::vector<ral::distribution::NodeColumnView > partitions_to_send;
                        for(int nodeIndex = 0; nodeIndex < this->context->getTotalNodes(); nodeIndex++ ){
                            ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
                            if (this->context->getNode(nodeIndex) == ral::communication::CommunicationData::getInstance().getSelfNode()){
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
                        ral::distribution::distributeTablePartitions(this->context.get(), partitions_to_send);
                    }
                    batch_count++;
                } catch(const std::exception& e) {
                    // TODO add retry here
                    this->logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                                        "query_id"_a=context->getContextToken(),
                                        "step"_a=context->getQueryStep(),
                                        "substep"_a=context->getQuerySubstep(),
                                        "info"_a="In DistributeAggregate kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
                                        "duration"_a="");
                    throw;
                }
            }

            if (!(group_column_indices.size() == 0
                && this->context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode()))) {
                // Aggregations without groupby does not send distributeTablePartitions
                ral::distribution::notifyLastTablePartitions(this->context.get(), ColumnDataPartitionMessage::MessageID());
            }
        });

        BlazingThread consumer_thread([this, group_column_indices](){
            // Lets put the server listener to feed the output, but not if its aggregations without group by and its not the master
            if(group_column_indices.size() > 0 ||
                        this->context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode())) {
                ExternalBatchColumnDataSequence<ColumnDataPartitionMessage> external_input(context, this->get_message_id(), this);
                std::unique_ptr<ral::frame::BlazingHostTable> host_table;
                while (host_table = external_input.next()) {
                    this->add_to_output_cache(std::move(host_table));
                }
            }
        });
        producer_thread.join();
        consumer_thread.join();

        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="DistributeAggregate Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};


class MergeAggregateKernel : public kernel {
public:
	MergeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::MergeAggregateKernel} {
        this->query_graph = query_graph;
	}

    bool can_you_throttle_my_input() {
		return false;
	}

	virtual kstatus run() {
        CodeTimer timer;
        CodeTimer eventTimer(false);

        std::vector<std::unique_ptr<ral::frame::BlazingTable>> tablesToConcat;
		std::vector<ral::frame::BlazingTableView> tableViewsToConcat;

        // This Kernel needs all of the input before it can do any output. So lets wait until all the input is available
        this->input_cache()->wait_until_finished();

        bool ordered = false; // If we start using sort based aggregations this may need to change
        BatchSequence input(this->input_cache(), this, ordered);
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
            eventTimer.start();

			if( ral::utilities::checkIfConcatenatingStringsWillOverflow(tableViewsToConcat)) {
				logger->warn("{query_id}|{step}|{substep}|{info}",
								"query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
								"step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
								"substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
								"info"_a="In MergeAggregateKernel::run Concatenating Strings will overflow strings length");
			}
            auto concatenated = ral::utilities::concatTables(tableViewsToConcat);

            auto log_input_num_rows = concatenated ? concatenated->num_rows() : 0;
            auto log_input_num_bytes = concatenated ? concatenated->sizeInBytes() : 0;

            std::vector<int> group_column_indices;
            std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases;
            std::vector<AggregateKind> aggregation_types;
            std::tie(group_column_indices, aggregation_input_expressions, aggregation_types,
                aggregation_column_assigned_aliases) = ral::operators::parseGroupByExpression(this->expression);

            std::vector<int> mod_group_column_indices;
            std::vector<std::string> mod_aggregation_input_expressions, mod_aggregation_column_assigned_aliases, merging_column_names;
            std::vector<AggregateKind> mod_aggregation_types;
            std::tie(mod_group_column_indices, mod_aggregation_input_expressions, mod_aggregation_types,
                mod_aggregation_column_assigned_aliases) = ral::operators::modGroupByParametersForMerge(
                group_column_indices, aggregation_types, concatenated->names());

            std::unique_ptr<ral::frame::BlazingTable> output;
            if(aggregation_types.size() == 0) {
                output = ral::operators::compute_groupby_without_aggregations(
                        concatenated->toBlazingTableView(), mod_group_column_indices);
            } else if (group_column_indices.size() == 0) {
                // aggregations without groupby are only merged on the master node
                if(context->isMasterNode(ral::communication::CommunicationData::getInstance().getSelfNode())) {
                    output = ral::operators::compute_aggregations_without_groupby(
                            concatenated->toBlazingTableView(), mod_aggregation_input_expressions, mod_aggregation_types,
                            mod_aggregation_column_assigned_aliases);
                } else {
                    // with aggregations without groupby the distribution phase should deposit an empty dataframe with the right schema into the cache, which is then output here
                    output = std::move(concatenated);
                }
            } else {
                output = ral::operators::compute_aggregations_with_groupby(
                        concatenated->toBlazingTableView(), mod_aggregation_input_expressions, mod_aggregation_types,
                        mod_aggregation_column_assigned_aliases, mod_group_column_indices);
            }
            // ral::utilities::print_blazing_table_view_schema(output->toBlazingTableView(), "MergeAggregateKernel_output");
            eventTimer.stop();

            auto log_output_num_rows = output->num_rows();
            auto log_output_num_bytes = output->sizeInBytes();

            events_logger->info("{ral_id}|{query_id}|{kernel_id}|{input_num_rows}|{input_num_bytes}|{output_num_rows}|{output_num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
                            "ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
                            "query_id"_a=context->getContextToken(),
                            "kernel_id"_a=this->get_id(),
                            "input_num_rows"_a=log_input_num_rows,
                            "input_num_bytes"_a=log_input_num_bytes,
                            "output_num_rows"_a=log_output_num_rows,
                            "output_num_bytes"_a=log_output_num_bytes,
                            "event_type"_a="MergeAggregateKernel",
                            "timestamp_begin"_a=eventTimer.start_time(),
                            "timestamp_end"_a=eventTimer.end_time());

            this->add_to_output_cache(std::move(output));
        } catch(const std::exception& e) {
            // TODO add retry here
            logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
                        "query_id"_a=context->getContextToken(),
                        "step"_a=context->getQueryStep(),
                        "substep"_a=context->getQuerySubstep(),
                        "info"_a="In MergeAggregate kernel for {}. What: {}"_format(expression, e.what()),
                        "duration"_a="");
            throw;
        }


        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="MergeAggregate Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

    bool ready_to_execute() {
        // WSM TODO: in this function we want to wait until all batch inputs are available
        return true;
    }

private:

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
