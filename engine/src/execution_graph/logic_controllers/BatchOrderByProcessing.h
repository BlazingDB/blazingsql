#pragma once

#include "BatchProcessing.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "distribution/primitives.h"
#include "operators/OrderBy.h"
#include "CodeTimer.h"
#include "taskflow/distributing_kernel.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <bmr/BlazingMemoryResource.h>

namespace ral {
namespace batch {
using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using namespace fmt::literals;


class PartitionSingleNodeKernel : public kernel {
public:
	PartitionSingleNodeKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::PartitionSingleNodeKernel} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
		CodeTimer timer;

		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"), this);
		auto partitionPlan = std::move(input_partitionPlan.next());

		bool ordered = false;
		BatchSequence input(this->input_.get_cache("input_a"), this, ordered);
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				auto batch = input.next();
				auto partitions = ral::operators::partition_table(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression);

				for (auto i = 0; i < partitions.size(); i++) {
					std::string cache_id = "output_" + std::to_string(i);
					this->add_to_output_cache(
						std::make_unique<ral::frame::BlazingTable>(std::make_unique<cudf::table>(partitions[i]), batch->names()),
						cache_id
						);
				}
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In PartitionSingleNode kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
				throw;
			}
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="PartitionSingleNode Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};

class SortAndSampleKernel : public distributing_kernel {

std::size_t SAMPLES_MESSAGE_TRACKER_IDX = 0;
std::size_t PARTITION_PLAN_MESSAGE_TRACKER_IDX = 1;

public:
	SortAndSampleKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: distributing_kernel{kernel_id,queryString, context, kernel_type::SortAndSampleKernel}
	{
		this->query_graph = query_graph;
		set_number_of_message_trackers(2); //default
		this->output_.add_port("output_a", "output_b");
	}

	void compute_partition_plan(std::vector<ral::frame::BlazingTableView> sampledTableViews, std::size_t avg_bytes_per_row, std::size_t local_total_num_rows) {

		std::cout<<"compute_partition_plan start samples"<<std::endl;
		for (auto samples : sampledTableViews){
			std::cout<<"sample num rows: "<<samples.num_rows()<<std::endl;
		}

		if (this->context->getAllNodes().size() == 1){ // single node mode
			auto partitionPlan = ral::operators::generate_partition_plan(sampledTableViews,
				local_total_num_rows, avg_bytes_per_row, this->expression, this->context.get());
			this->add_to_output_cache(std::move(partitionPlan), "output_b");
		} else { // distributed mode
			if( ral::utilities::checkIfConcatenatingStringsWillOverflow(sampledTableViews)) {
				logger->warn("{query_id}|{step}|{substep}|{info}",
								"query_id"_a=(context ? std::to_string(context->getContextToken()) : ""),
								"step"_a=(context ? std::to_string(context->getQueryStep()) : ""),
								"substep"_a=(context ? std::to_string(context->getQuerySubstep()) : ""),
								"info"_a="In SortAndSampleKernel::compute_partition_plan Concatenating Strings will overflow strings length");
			}
			
			
			auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
			if(context->isMasterNode(self_node)) {
				context->incrementQuerySubstep();
				auto nodes = context->getAllNodes();

				int samples_to_collect = this->context->getAllNodes().size();
				std::vector<std::unique_ptr<ral::cache::CacheData> >table_scope_holder;
				std::vector<size_t> total_table_rows;
				std::vector<ral::frame::BlazingTableView> samples;

				for(std::size_t i = 0; i < nodes.size(); ++i) {
					if(!(nodes[i] == self_node)) {
						std::string message_id = std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + nodes[i].id();

						table_scope_holder.push_back(this->query_graph->get_input_message_cache()->pullCacheData(message_id));
						ral::cache::GPUCacheDataMetaData * cache_ptr = static_cast<ral::cache::GPUCacheDataMetaData *> (table_scope_holder[table_scope_holder.size() - 1].get());

						total_table_rows.push_back(std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]));
						samples.push_back(cache_ptr->getTableView());
					}
				}

				for (std::size_t i = 0; i < sampledTableViews.size(); i++){
					samples.push_back(sampledTableViews[i]);
				}
				
				total_table_rows.push_back(local_total_num_rows);

				std::size_t totalNumRows = std::accumulate(total_table_rows.begin(), total_table_rows.end(), std::size_t(0));
				std::unique_ptr<ral::frame::BlazingTable> partitionPlan = ral::operators::generate_partition_plan(samples, totalNumRows, avg_bytes_per_row, this->expression, this->context.get());

				broadcast(std::move(partitionPlan),
					this->output_.get_cache("output_b").get(),
					"", // message_id_prefix
					"output_b", // cache_id
					PARTITION_PLAN_MESSAGE_TRACKER_IDX); //message_tracker_idx (message tracker for the partitionPlan)

			} else {
				context->incrementQuerySubstep();

				auto concatSamples = ral::utilities::concatTables(sampledTableViews);
				concatSamples->ensureOwnership();

				ral::cache::MetadataDictionary extra_metadata;
				extra_metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, local_total_num_rows);
				send_message(std::move(concatSamples),
					"false", //specific_cache
					"", //cache_id
					this->context->getMasterNode().id(), //target_id
					"", //message_id_prefix
					true, //always_add
					false, //wait_for
					SAMPLES_MESSAGE_TRACKER_IDX, //message_tracker_idx (message tracker for samples)
					extra_metadata);

				context->incrementQuerySubstep();
			}

			this->output_cache("output_b")->wait_for_count(1); // waiting for the partition_plan to arrive before continuing
		}
	}

	virtual kstatus run() {
		CodeTimer timer;
		CodeTimer eventTimer(false);

		bool try_num_rows_estimation = true;
		bool get_samples = true;
		bool estimate_samples = false;
		uint64_t num_rows_estimate = 0;
		uint64_t population_to_sample = 0;
		uint64_t population_sampled = 0;
		BlazingThread partition_plan_thread;

		float order_by_samples_ratio = 0.1;
		std::map<std::string, std::string> config_options = context->getConfigOptions();
		auto it = config_options.find("ORDER_BY_SAMPLES_RATIO");
		if (it != config_options.end()){
			order_by_samples_ratio = std::stof(config_options["ORDER_BY_SAMPLES_RATIO"]);
		}
		int max_order_by_samples = 10000;
		it = config_options.find("MAX_ORDER_BY_SAMPLES_PER_NODE");
		if (it != config_options.end()){
			max_order_by_samples = std::stoi(config_options["MAX_ORDER_BY_SAMPLES_PER_NODE"]);
		}

		bool ordered = false;
		BatchSequence input(this->input_cache(), this, ordered);
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::size_t localTotalNumRows = 0;
		std::size_t localTotalBytes = 0;
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				auto batch = input.next();

				eventTimer.start();
				auto log_input_num_rows = batch ? batch->num_rows() : 0;
				auto log_input_num_bytes = batch ? batch->sizeInBytes() : 0;

				auto sortedTable = ral::operators::sort(batch->toBlazingTableView(), this->expression);
				if (get_samples) {
					auto sampledTable = ral::operators::sample(batch->toBlazingTableView(), this->expression, order_by_samples_ratio);
					sampledTableViews.push_back(sampledTable->toBlazingTableView());
					sampledTables.push_back(std::move(sampledTable));
				}
				localTotalNumRows += batch->view().num_rows();
				localTotalBytes += batch->sizeInBytes();

				// Try samples estimation
				if(try_num_rows_estimation) {
					std::tie(estimate_samples, num_rows_estimate) = this->query_graph->get_estimated_input_rows_to_cache(this->get_id(), std::to_string(this->get_id()));
					population_to_sample = static_cast<uint64_t>(num_rows_estimate * order_by_samples_ratio);
					population_to_sample = (population_to_sample > max_order_by_samples) ? (max_order_by_samples) : population_to_sample;
					try_num_rows_estimation = false;
				}
				population_sampled += batch->num_rows();
				if (estimate_samples && population_to_sample > 0 && population_sampled > population_to_sample)	{

					size_t avg_bytes_per_row = localTotalNumRows == 0 ? 1 : localTotalBytes/localTotalNumRows;
					partition_plan_thread = BlazingThread(&SortAndSampleKernel::compute_partition_plan, this, sampledTableViews, avg_bytes_per_row, num_rows_estimate);
					estimate_samples = false;
					get_samples = false;
				}
				// End estimation

				eventTimer.stop();

				if(sortedTable){
					auto log_output_num_rows = sortedTable->num_rows();
					auto log_output_num_bytes = sortedTable->sizeInBytes();

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
				}

				if(this->add_to_output_cache(std::move(sortedTable), "output_a")){
					batch_count++;
				}

			} catch(const std::exception& e) {
				// TODO add retry here
				// Note that we have to handle the collected samples in a special way. We need to compare to the current batch_count and perhaps evict one set of samples
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In SortAndSample kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
				throw;
			}
		}

		if (partition_plan_thread.joinable()){
			partition_plan_thread.join();
		} else {
			size_t avg_bytes_per_row = localTotalNumRows == 0 ? 1 : localTotalBytes/localTotalNumRows;
			compute_partition_plan(sampledTableViews, avg_bytes_per_row, localTotalNumRows);
		}


		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="SortAndSample Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};

class PartitionKernel : public distributing_kernel {
public:
	PartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: distributing_kernel{kernel_id, queryString, context, kernel_type::PartitionKernel} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");

		std::map<std::string, std::string> config_options = context->getConfigOptions();
		int max_num_order_by_partitions_per_node = 8;
		auto it = config_options.find("MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE");
		if (it != config_options.end()){
			max_num_order_by_partitions_per_node = std::stoi(config_options["MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE"]);
		}
		set_number_of_message_trackers(max_num_order_by_partitions_per_node);
	}

	virtual kstatus run() {

		CodeTimer timer;

		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"), this);
		auto partitionPlan = input_partitionPlan.next();

		if (partitionPlan == nullptr){
			std::cout<<"ERROR: partitionPlan is nullptr"<<std::endl;
		} else {
			std::cout<<"OK: partitionPlan is not nullptr. num rows: "<<partitionPlan->num_rows()<<std::endl;
		}

		context->incrementQuerySubstep();

		std::map<std::string, std::map<int32_t, int> > node_count;

		bool ordered = false;
		BatchSequence input(this->input_.get_cache("input_a"), this, ordered);
		int batch_count = 0;
		std::vector<cudf::order> sortOrderTypes;
		std::vector<int> sortColIndices;
		std::tie(sortColIndices, sortOrderTypes, std::ignore) =	ral::operators::get_sort_vars(this->expression);
		auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
		auto nodes = context->getAllNodes();

		// If we have no partitionPlan, its because we have no data, therefore its one partition per node
		int num_partitions_per_node = partitionPlan->num_rows() == 0 ? 1 : (partitionPlan->num_rows() + 1) / this->context->getTotalNodes();

		std::map<int32_t, int> temp_partitions_map;
		for (size_t i = 0; i < num_partitions_per_node; i++) {
			temp_partitions_map[i] = 0;
		}
		for (auto &&node : nodes) {
			node_count.emplace(node.id(), temp_partitions_map);
		}

		while (input.wait_for_next()) {
			try {
				auto batch = input.next();

				std::vector<ral::distribution::NodeColumnView> partitions = ral::distribution::partitionData(this->context.get(), batch->toBlazingTableView(), partitionPlan->toBlazingTableView(), sortColIndices, sortOrderTypes);
				std::vector<int32_t> part_ids(partitions.size());
				std::generate(part_ids.begin(), part_ids.end(), [count=0, num_partitions_per_node] () mutable { return (count++) % (num_partitions_per_node); });

				scatterParts(partitions,
					this->output_.get_cache().get(),
					"", //message_id_prefix
					part_ids
				);

				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In Partition kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
				throw;
			}
		}

		for (auto i = 0; i < num_partitions_per_node; i++) {
			std::string cache_id = "output_" + std::to_string(i);
			send_total_partition_counts(
				"", //message_prefix
				cache_id, //cache_id
				i //message_tracker_idx
			);
		}

		for (auto i = 0; i < num_partitions_per_node; i++) {
			int total_count = get_total_partition_counts(i);
			std::string cache_id = "output_" + std::to_string(i);
			this->output_cache(cache_id)->wait_for_count(total_count);
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="Partition Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());



		return kstatus::proceed;
	}

private:

};

/**
 * This kernel has a loop over all its different input caches.
 * It then pulls all the inputs from one cache and merges them.
 */
class MergeStreamKernel : public kernel {
public:
	MergeStreamKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::MergeStreamKernel}  {
		this->query_graph = query_graph;
	}

	virtual kstatus run() {
		CodeTimer timer;

		int batch_count = 0;
		for (auto idx = 0; idx < this->input_.count(); idx++)
		{
			try {
				std::vector<ral::frame::BlazingTableView> tableViews;
				std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables;
				auto cache_id = "input_" + std::to_string(idx);

				// This Kernel needs all of the input before it can do any output. So lets wait until all the input is available
				this->input_.get_cache(cache_id)->wait_until_finished();

				while (this->input_.get_cache(cache_id)->wait_for_next()) {
					CodeTimer cacheEventTimer(false);

					cacheEventTimer.start();
					auto table = this->input_.get_cache(cache_id)->pullFromCache();
					cacheEventTimer.stop();

					if (table) {
						auto num_rows = table->num_rows();
						auto num_bytes = table->sizeInBytes();

						cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
										"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
										"query_id"_a=context->getContextToken(),
										"source"_a=this->input_.get_cache(cache_id)->get_id(),
										"sink"_a=this->get_id(),
										"num_rows"_a=num_rows,
										"num_bytes"_a=num_bytes,
										"event_type"_a="removeCache",
										"timestamp_begin"_a=cacheEventTimer.start_time(),
										"timestamp_end"_a=cacheEventTimer.end_time());

						tableViews.emplace_back(table->toBlazingTableView());
						tables.emplace_back(std::move(table));
					}
				}

				if (tableViews.empty()) {
					// noop
				} else if(tableViews.size() == 1) {
					this->add_to_output_cache(std::move(tables.front()));
				} else {
					auto output = ral::operators::merge(tableViews, this->expression);
					this->add_to_output_cache(std::move(output));
				}
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
												"query_id"_a=context->getContextToken(),
												"step"_a=context->getQueryStep(),
												"substep"_a=context->getQuerySubstep(),
												"info"_a="In MergeStream kernel batch {} for {}. What: {} . max_memory_used: {}"_format(batch_count, expression, e.what(), blazing_device_memory_resource::getInstance().get_full_memory_summary()),
												"duration"_a="");
				throw;
			}
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="MergeStream Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};


/**
 * @brief This kernel only returns a specified number of rows given by their corresponding logical limit expression.
 */

class LimitKernel : public distributing_kernel {

public:
	LimitKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: distributing_kernel{kernel_id,queryString, context, kernel_type::LimitKernel}  {
		this->query_graph = query_graph;
		set_number_of_message_trackers(1); //default
	}


	virtual kstatus run() {
		CodeTimer timer;
		CodeTimer eventTimer(false);

		int64_t total_batch_rows = 0;
		std::vector<std::unique_ptr<ral::cache::CacheData>> cache_vector;
		BatchSequenceBypass input_seq(this->input_cache(), this);
		while (input_seq.wait_for_next()) {
			auto batch = input_seq.next();
			total_batch_rows += batch->num_rows();
			cache_vector.push_back(std::move(batch));
		}

		cudf::size_type limitRows;
		std::tie(std::ignore, std::ignore, limitRows) = ral::operators::get_sort_vars(this->expression);
		int64_t rows_limit = limitRows;

		if(this->context->getTotalNodes() > 1 && rows_limit >= 0) {
			this->context->incrementQuerySubstep();

			// ral::distribution::distributeNumRows(context, total_batch_rows);
			auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
			int self_node_idx = context->getNodeIndex(self_node);
			auto nodes_to_send = context->getAllOtherNodes(self_node_idx);
			std::string worker_ids_metadata;
			std::vector<std::string> limit_messages_to_wait_for;
			for (auto i = 0; i < nodes_to_send.size(); i++)	{
				worker_ids_metadata += nodes_to_send[i].id();
				if (i < nodes_to_send.size() - 1) {
					worker_ids_metadata += ",";
				}
				limit_messages_to_wait_for.push_back(
					std::to_string(this->context->getContextToken()) + "_" +	std::to_string(this->get_id()) +	"_" +	nodes_to_send[i].id());
			}
			ral::cache::MetadataDictionary extra_metadata;
			extra_metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, total_batch_rows);
			send_message(nullptr, //empty table
				"false", //specific_cache
				"", //cache_id
				worker_ids_metadata, //target_id
				"", //message_id_prefix
				true, //always_add
				false, //wait_for
				0, //message_tracker_idx
				extra_metadata);
	
			int64_t prev_total_rows = 0;
			for (auto i = 0; i < limit_messages_to_wait_for.size(); i++)	{
				auto meta_message = this->query_graph->get_input_message_cache()->pullCacheData(limit_messages_to_wait_for[i]);
				if(i < context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode())){
					prev_total_rows += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData*>(meta_message.get())->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]);
				}
			}
			rows_limit = std::min(std::max(rows_limit - prev_total_rows, int64_t{0}), total_batch_rows);
		}

		if (rows_limit < 0) {
			for (auto &&cache_data : cache_vector) {
				this->add_to_output_cache(std::move(cache_data));
			}
		} else {
			int batch_count = 0;
			for (auto &&cache_data : cache_vector)
			{
				try {
					auto batch = cache_data->decache();

					auto log_input_num_rows = batch->num_rows();
					auto log_input_num_bytes = batch->sizeInBytes();

					eventTimer.start();
					std::tie(batch, rows_limit) = ral::operators::limit_table(std::move(batch), rows_limit);
					eventTimer.stop();

					auto log_output_num_rows = batch->num_rows();
					auto log_output_num_bytes = batch->sizeInBytes();

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

					this->add_to_output_cache(std::move(batch));

					if (rows_limit == 0){
						break;
					}
					batch_count++;
				} catch(const std::exception& e) {
					// TODO add retry here
					logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
												"query_id"_a=context->getContextToken(),
												"step"_a=context->getQueryStep(),
												"substep"_a=context->getQuerySubstep(),
												"info"_a="In Limit kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
												"duration"_a="");
					throw;
				}
			}
		}

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="Limit Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};

} // namespace batch
} // namespace ral
