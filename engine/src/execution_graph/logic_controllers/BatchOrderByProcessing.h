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

class SortAndSampleSingleNodeKernel : public kernel {
public:
	SortAndSampleSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context}
	{
		this->query_graph = query_graph;
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
		CodeTimer timer;

		BatchSequence input(this->input_cache(), this);
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::vector<size_t> tableTotalRows;
		size_t total_num_rows = 0;
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				auto batch = input.next();
				auto sortedTable = ral::operators::experimental::sort(batch->toBlazingTableView(), this->expression);
				auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
				sampledTableViews.push_back(sampledTable->toBlazingTableView());
				sampledTables.push_back(std::move(sampledTable));
				tableTotalRows.push_back(batch->view().num_rows());
				this->add_to_output_cache(std::move(sortedTable), "output_a");
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				// Note that we have to handle the collected samples in a special way. We need to compare to the current batch_count and perhaps evict one set of samples 
				logger->error("{query_id}|{step}|{substep}|{info}|{duration}||||",
											"query_id"_a=context->getContextToken(),
											"step"_a=context->getQueryStep(),
											"substep"_a=context->getQuerySubstep(),
											"info"_a="In SortAndSampleSingleNode kernel batch {} for {}. What: {}"_format(batch_count, expression, e.what()),
											"duration"_a="");
			}
		}
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		auto partitionPlan = ral::operators::experimental::generate_partition_plan(32, sampledTableViews, tableTotalRows, this->expression);
		this->add_to_output_cache(std::move(partitionPlan), "output_b");
		
		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
									"query_id"_a=context->getContextToken(),
									"step"_a=context->getQueryStep(),
									"substep"_a=context->getQuerySubstep(),
									"info"_a="SortAndSampleSingleNode Kernel Completed",
									"duration"_a=timer.elapsed_time(),
									"kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};

class PartitionSingleNodeKernel : public kernel {
public:
	PartitionSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
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
				auto partitions = ral::operators::experimental::partition_table(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression);

				// std::cout<<">>>>>>>>>>>>>>> PARTITIONS START"<< std::endl;
				// for(auto& partition : partitions)
				// 	ral::utilities::print_blazing_table_view(ral::frame::BlazingTableView(partition, batch->names()));
				// std::cout<<">>>>>>>>>>>>>>> PARTITIONS START"<< std::endl;

				for (auto i = 0; i < partitions.size(); i++) {
					std::string cache_id = "output_" + std::to_string(i);
					this->add_to_output_cache(
						std::make_unique<ral::frame::BlazingTable>(std::make_unique<cudf::experimental::table>(partitions[i]), batch->names()),
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
		: kernel{queryString, context}
	{
		this->query_graph = query_graph;
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
		CodeTimer timer;

		BatchSequence input(this->input_cache(), this);
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::vector<size_t> tableTotalRows;
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				auto batch = input.next();
				auto sortedTable = ral::operators::experimental::sort(batch->toBlazingTableView(), this->expression);
				auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
				sampledTableViews.push_back(sampledTable->toBlazingTableView());
				sampledTables.push_back(std::move(sampledTable));
				tableTotalRows.push_back(batch->view().num_rows());
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
			}
		}
		size_t totalNumRows = std::accumulate(tableTotalRows.begin(), tableTotalRows.end(), 0);
		auto concatSamples = ral::utilities::experimental::concatTables(sampledTableViews);
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		auto partitionPlan = ral::operators::experimental::generate_distributed_partition_plan(32, concatSamples->toBlazingTableView(), totalNumRows, this->expression, this->context.get());
		this->add_to_output_cache(std::move(partitionPlan), "output_b");
		
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
		: kernel{queryString, context} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
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
					auto self_partitions = ral::operators::experimental::distribute_table_partitions(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression, this->context.get());

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
				}	
			}
			ral::distribution::experimental::notifyLastTablePartitions(this->context.get(), ColumnDataPartitionMessage::MessageID());
		});
		
		BlazingThread consumer([this](){
			ExternalBatchColumnDataSequence external_input(context, this->get_message_id());
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
		: kernel{queryString, context}  {
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
				while (this->input_.get_cache(cache_id)->wait_for_next()) {
					auto table = this->input_.get_cache(cache_id)->pullFromCache(context.get());
					if (table) {
						tableViews.emplace_back(table->toBlazingTableView());
						tables.emplace_back(std::move(table));
					}
				}

				if (tableViews.empty()) {
					// noop
				} else if(tableViews.size() == 1) {
					this->add_to_output_cache(std::move(tables.front()));
				}	else {
					// std::cout<<">>>>>>>>>>>>>>> MERGE PARTITIONS START"<< std::endl;
					// for (auto view : tableViews)
					// 	ral::utilities::print_blazing_table_view(view);

					// std::cout<<">>>>>>>>>>>>>>> MERGE PARTITIONS END"<< std::endl;

					auto output = ral::operators::experimental::merge(tableViews, this->expression);

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
		: kernel{queryString, context}  {
		this->query_graph = query_graph;
	}
	
	virtual kstatus run() {
		CodeTimer timer;

		int64_t total_batch_rows = 0;
		std::vector<std::unique_ptr<ral::cache::CacheData>> cache_vector;
		BatchSequenceBypass input_seq(this->input_cache());
		while (input_seq.wait_for_next()) {
			auto batch = input_seq.next();
			total_batch_rows += batch->num_rows();
			cache_vector.push_back(std::move(batch));
		}

		int64_t rows_limit = ral::operators::experimental::get_local_limit(total_batch_rows, this->expression, this->context.get());

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
					std::tie(batch, rows_limit) = ral::operators::experimental::limit_table(std::move(batch), rows_limit);
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
