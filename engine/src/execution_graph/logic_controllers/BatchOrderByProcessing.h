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
	PartitionSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context, kernel_type::PartitionSingleNodeKernel} {
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

		BatchSequence input(this->input_.get_cache("input_a"), this);
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
	SortAndSampleKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context, kernel_type::SortAndSampleKernel}
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
			auto concatSamples = ral::utilities::concatTables(sampledTableViews);
			auto partitionPlan = ral::operators::generate_distributed_partition_plan(concatSamples->toBlazingTableView(), 
				local_total_num_rows, avg_bytes_per_row, this->expression, this->context.get());
			this->add_to_output_cache(std::move(partitionPlan), "output_b");
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

		BatchSequence input(this->input_cache(), this);
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
					population_to_sample = static_cast<uint64_t>(num_rows_estimate * order_by_samples_ratio);
					try_num_rows_estimation = false;
				}
				population_sampled += batch->num_rows();
				if (estimate_samples && population_sampled > population_to_sample)	{
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
	PartitionKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context, kernel_type::PartitionKernel} {
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

		BlazingThread generator([input_cache = this->input_.get_cache("input_a"), &partitionPlan, this](){
			BatchSequence input(input_cache, this);
			int batch_count = 0;
			while (input.wait_for_next()) {
				try {
					auto batch = input.next();
					auto self_partitions = ral::operators::distribute_table_partitions(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression, this->context.get());

					for (auto && self_part : self_partitions) {
						std::string cache_id = "output_" + std::to_string(self_part.first);
						this->add_to_output_cache(std::move(self_part.second), cache_id);
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
			ral::distribution::notifyLastTablePartitions(this->context.get(), ColumnDataPartitionMessage::MessageID());
		});

		BlazingThread consumer([this](){
			ExternalBatchColumnDataSequence<ColumnDataPartitionMessage> external_input(context, this->get_message_id(), this);
			std::unique_ptr<ral::frame::BlazingHostTable> host_table;
			while (host_table = external_input.next()) {
				std::string cache_id = "output_" + std::to_string(host_table->get_part_id());
				this->add_to_output_cache(std::move(host_table), cache_id);
			}
		});
		generator.join();
		consumer.join();

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
	MergeStreamKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context, kernel_type::MergeStreamKernel}  {
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
	LimitKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context, kernel_type::LimitKernel}  {
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

		int64_t rows_limit = ral::operators::get_local_limit(total_batch_rows, this->expression, this->context.get());

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
