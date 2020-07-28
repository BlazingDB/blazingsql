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

namespace ral {
namespace batch {
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

	bool can_you_throttle_my_input() {
		return true;
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

				// std::cout<<">>>>>>>>>>>>>>> PARTITIONS START"<< std::endl;
				// for(auto& partition : partitions)
				// 	ral::utilities::print_blazing_table_view(ral::frame::BlazingTableView(partition, batch->names()));
				// std::cout<<">>>>>>>>>>>>>>> PARTITIONS START"<< std::endl;

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

class SortAndSampleKernel : public kernel {
public:
	SortAndSampleKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id,queryString, context, kernel_type::SortAndSampleKernel}
	{
		this->query_graph = query_graph;
		this->output_.add_port("output_a", "output_b");
	}

	void compute_partition_plan(std::vector<ral::frame::BlazingTableView> sampledTableViews, std::size_t avg_bytes_per_row, std::size_t local_total_num_rows) {
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
			auto concatSamples = ral::utilities::concatTables(sampledTableViews);
			std::vector<ral::frame::BlazingTableView> samples;
			auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
			std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
			if(context->isMasterNode(self_node)) {
				context->incrementQuerySubstep();
				auto nodes = context->getAllNodes();

				int samples_to_collect = this->context->getAllNodes().size();
				std::vector<std::unique_ptr<ral::cache::CacheData> >table_scope_holder;
				std::vector<size_t> total_table_rows;

				for(std::size_t i = 0; i < nodes.size(); ++i) {
					if(!(nodes[i] == self_node)) {
						std::string message_id = std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + nodes[i].id();


						table_scope_holder.push_back(this->query_graph->get_input_cache()->pullCacheData(message_id));
						ral::cache::GPUCacheDataMetaData * cache_ptr = static_cast<ral::cache::GPUCacheDataMetaData *> (table_scope_holder[table_scope_holder.size() - 1].get());

						total_table_rows.push_back(std::stoll(cache_ptr->getMetadata().get_values()[ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL]));
						samples.push_back(cache_ptr->getTableView());
					}
				}

				samples.push_back(concatSamples->toBlazingTableView());
				total_table_rows.push_back(local_total_num_rows);


				std::size_t totalNumRows = std::accumulate(total_table_rows.begin(), total_table_rows.end(), std::size_t(0));
				partitionPlan = ral::operators::generate_partition_plan(samples, totalNumRows, avg_bytes_per_row, this->expression, this->context.get());

				int self_node_idx = context->getNodeIndex(self_node);
				auto nodes_to_send = context->getAllOtherNodes(self_node_idx);
				std::string worker_ids_metadata;
				for (auto i = 0; i < nodes_to_send.size(); i++)	{
					if(nodes_to_send[i].id() != self_node.id()){
						worker_ids_metadata += nodes_to_send[i].id();
						if (i < nodes_to_send.size() - 1) {
							worker_ids_metadata += ",";
						}
					}

				}
				ral::cache::MetadataDictionary metadata;
				metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
				metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
				metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "true");
				metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "output_b");
				metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
				metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, worker_ids_metadata);
				metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, std::to_string(local_total_num_rows));


				ral::cache::CacheMachine* output_cache = this->query_graph->get_output_cache();
				output_cache->addCacheData(std::unique_ptr<ral::cache::GPUCacheDataMetaData>(new ral::cache::GPUCacheDataMetaData(std::move(partitionPlan->toBlazingTableView().clone()), metadata)),"",true);
						
				this->add_to_output_cache(std::move(partitionPlan), "output_b");
			} else {
				context->incrementQuerySubstep();

				// ral::distribution::sendSamplesToMaster(context, selfSamples, local_total_num_rows);

				ral::cache::MetadataDictionary metadata;
				metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
				metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
				metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
				metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
				metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
				metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, this->context->getMasterNode().id());
				metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, std::to_string(local_total_num_rows));
				std::string message_id = std::to_string(this->context->getContextToken()) + "_" + std::to_string(this->get_id()) + "_" + self_node.id();
				metadata.add_value(ral::cache::MESSAGE_ID, message_id);

				ral::cache::CacheMachine* output_cache = this->query_graph->get_output_cache();
				concatSamples->ensureOwnership();
				output_cache->addCacheData(std::unique_ptr<ral::cache::GPUCacheData>(new ral::cache::GPUCacheDataMetaData(std::move(concatSamples), metadata)),"",true);

				context->incrementQuerySubstep();

			}

			this->output_cache("output_b")->wait_for_count(1);
		}
	}

	bool can_you_throttle_my_input() {
		return true;
	}

	virtual kstatus run() {
		CodeTimer timer;
		CodeTimer eventTimer(false);

		bool try_num_rows_estimation = true;
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

		bool ordered = false;
		BatchSequence input(this->input_cache(), this, ordered);
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::size_t localTotalNumRows = 0;
		std::size_t localTotalBytes = 0;
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				this->output_cache("output_a")->wait_if_cache_is_saturated();
				auto batch = input.next();

				eventTimer.start();
				auto log_input_num_rows = batch ? batch->num_rows() : 0;
				auto log_input_num_bytes = batch ? batch->sizeInBytes() : 0;

				auto sortedTable = ral::operators::sort(batch->toBlazingTableView(), this->expression);
				auto sampledTable = ral::operators::sample(batch->toBlazingTableView(), this->expression);
		
				sampledTableViews.push_back(sampledTable->toBlazingTableView());
				sampledTables.push_back(std::move(sampledTable));
				localTotalNumRows += batch->view().num_rows();
				localTotalBytes += batch->sizeInBytes();

				// Try samples estimation
				if(try_num_rows_estimation) {
					std::tie(estimate_samples, num_rows_estimate) = this->query_graph->get_estimated_input_rows_to_cache(this->get_id(), std::to_string(this->get_id()));
					population_to_sample = static_cast<uint64_t>(std::ceil(num_rows_estimate * order_by_samples_ratio));
					try_num_rows_estimation = false;
				}
				population_sampled += batch->num_rows();
				if (estimate_samples && population_to_sample > 0 && population_sampled > population_to_sample)	{
					
					size_t avg_bytes_per_row = localTotalNumRows == 0 ? 1 : localTotalBytes/localTotalNumRows;
					partition_plan_thread = BlazingThread(&SortAndSampleKernel::compute_partition_plan, this, sampledTableViews, avg_bytes_per_row, num_rows_estimate);
					estimate_samples = false;
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

				this->add_to_output_cache(std::move(sortedTable), "output_a");
				batch_count++;
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

class PartitionKernel : public kernel {
public:
	PartitionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::PartitionKernel} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
	}

	bool can_you_throttle_my_input() {
		return true;
	}

	virtual kstatus run() {
		using ColumnDataPartitionMessage = ral::communication::messages::ColumnDataPartitionMessage;

		CodeTimer timer;

		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"), this);
		auto partitionPlan = std::move(input_partitionPlan.next());

		context->incrementQuerySubstep();

		std::vector<std::string> messages_to_wait_for;
		std::map<std::string, int> node_count;
		BlazingThread generator([input_cache = this->input_.get_cache("input_a"), &partitionPlan, &node_count, &messages_to_wait_for,this](){
			bool ordered = false;
			BatchSequence input(input_cache, this, ordered);
			int batch_count = 0;
			std::vector<cudf::order> sortOrderTypes;
			std::vector<int> sortColIndices;
			std::tie(sortColIndices, sortOrderTypes, std::ignore) =	ral::operators::get_sort_vars(this->expression);
			auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
			while (input.wait_for_next()) {
				try {
					auto batch = input.next();

					std::vector<ral::distribution::NodeColumnView> partitions = ral::distribution::partitionData(this->context.get(), batch->toBlazingTableView(), partitionPlan->toBlazingTableView(), sortColIndices, sortOrderTypes);

					std::vector<int32_t> part_ids(partitions.size());
					int num_partitions_per_node = partitions.size() / this->context->getTotalNodes();
					std::generate(part_ids.begin(), part_ids.end(), [count=0, num_partitions_per_node] () mutable { return (count++) % (num_partitions_per_node); });

					// ral::distribution::distributeTablePartitions(context, partitions, part_ids);

					ral::cache::MetadataDictionary metadata;
					metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
					metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
					metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "true");
					metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
					ral::cache::CacheMachine* output_cache = this->query_graph->get_output_cache();
					for (auto i = 0; i < partitions.size(); i++) {
						blazingdb::transport::Node dest_node;
						ral::frame::BlazingTableView table_view;
						std::tie(dest_node, table_view) = partitions[i];
						if(dest_node == self_node || table_view.num_rows() == 0) {
							continue;
						}

						metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, dest_node.id());
						metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "output_" + std::to_string(part_ids[i]) );
						
						node_count[dest_node.id()]++;
						output_cache->addCacheData(std::unique_ptr<ral::cache::GPUCacheData>(new ral::cache::GPUCacheDataMetaData(table_view.clone(), metadata)),"",true);
					}

					for (auto i = 0; i < partitions.size(); i++) {
						auto & partition = partitions[i];
						if(partition.first == self_node) {
							std::string cache_id = "output_" + std::to_string(part_ids[i]);
							this->add_to_output_cache(partition.second.clone(), cache_id);
							node_count[self_node.id()]++;
						}
					}

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


			auto nodes = context->getAllNodes();
			for(std::size_t i = 0; i < nodes.size(); ++i) {
				if(!(nodes[i] == self_node)) {
					ral::cache::MetadataDictionary metadata;
					metadata.add_value(ral::cache::KERNEL_ID_METADATA_LABEL, std::to_string(this->get_id()));
					metadata.add_value(ral::cache::QUERY_ID_METADATA_LABEL, std::to_string(this->context->getContextToken()));
					metadata.add_value(ral::cache::ADD_TO_SPECIFIC_CACHE_METADATA_LABEL, "false");
					metadata.add_value(ral::cache::CACHE_ID_METADATA_LABEL, "");
					metadata.add_value(ral::cache::SENDER_WORKER_ID_METADATA_LABEL, self_node.id());
					metadata.add_value(ral::cache::MESSAGE_ID, metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																										metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																										metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
					metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, nodes[i].id());
					metadata.add_value(ral::cache::PARTITION_COUNT, std::to_string(node_count[nodes[i].id()]));
					messages_to_wait_for.push_back(metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																				metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																				metadata.get_values()[ral::cache::WORKER_IDS_METADATA_LABEL]);

					this->query_graph->get_output_cache()->addCacheData(
							std::unique_ptr<ral::cache::GPUCacheData>(new ral::cache::GPUCacheDataMetaData(ral::utilities::create_empty_table({}, {}), metadata)),"",true);
				}
			}
		});
		generator.join();

		auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
 		int total_count = node_count[self_node.id()];
		for (auto message : messages_to_wait_for){
			auto meta_message = this->query_graph->get_input_cache()->pullCacheData(message);
			total_count += std::stoi(static_cast<ral::cache::GPUCacheDataMetaData *>(meta_message.get())->getMetadata().get_values()[ral::cache::PARTITION_COUNT]);
		}

		//for (auto i = 0; i < partitions.size(); i++) {
		//		std::string cache_id = "output_" + std::to_string(part_ids[i]);
		//		this->output_cache(cache_id)->wait_for_count(total_count);
		//}




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

class MergeStreamKernel : public kernel {
public:
	MergeStreamKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::MergeStreamKernel}  {
		this->query_graph = query_graph;
	}

	bool can_you_throttle_my_input() {
		return false;
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
					// std::cout<<">>>>>>>>>>>>>>> MERGE PARTITIONS START"<< std::endl;
					// for (auto view : tableViews)
					// 	ral::utilities::print_blazing_table_view(view);

					// std::cout<<">>>>>>>>>>>>>>> MERGE PARTITIONS END"<< std::endl;

					auto output = ral::operators::merge(tableViews, this->expression);

	//					ral::utilities::print_blazing_table_view(output->toBlazingTableView());

					this->add_to_output_cache(std::move(output));
				}
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
												"query_id"_a=context->getContextToken(),
												"step"_a=context->getQueryStep(),
												"substep"_a=context->getQuerySubstep(),
												"info"_a="In MergeStream kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
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


class LimitKernel : public kernel {
public:
	LimitKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id,queryString, context, kernel_type::LimitKernel}  {
		this->query_graph = query_graph;
	}

	bool can_you_throttle_my_input() {
		return false;
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
			std::vector<std::string> messages_to_wait_for;
			for (auto i = 0; i < nodes_to_send.size(); i++)	{
				worker_ids_metadata += nodes_to_send[i].id();
				messages_to_wait_for.push_back(
					std::to_string(this->context->getContextToken()) + "_" +	std::to_string(this->get_id()) +	"_" +	nodes_to_send[i].id());

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
			metadata.add_value(ral::cache::MESSAGE_ID, metadata.get_values()[ral::cache::QUERY_ID_METADATA_LABEL] + "_" +
																								metadata.get_values()[ral::cache::KERNEL_ID_METADATA_LABEL] +	"_" +
																								metadata.get_values()[ral::cache::SENDER_WORKER_ID_METADATA_LABEL]);
			metadata.add_value(ral::cache::WORKER_IDS_METADATA_LABEL, worker_ids_metadata);
			metadata.add_value(ral::cache::TOTAL_TABLE_ROWS_METADATA_LABEL, std::to_string(total_batch_rows));
			ral::cache::CacheMachine* output_cache = this->query_graph->get_output_cache();
			output_cache->addCacheData(std::make_unique<ral::cache::GPUCacheDataMetaData>(ral::utilities::create_empty_table({}, {}), metadata),"",true);

			// std::vector<int64_t> nodesRowSize = ral::distribution::collectNumRows(context.get());

			int64_t prev_total_rows = 0;
			for (auto i = 0; i < messages_to_wait_for.size(); i++)	{
				auto meta_message = this->query_graph->get_input_cache()->pullCacheData(messages_to_wait_for[i]);
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
