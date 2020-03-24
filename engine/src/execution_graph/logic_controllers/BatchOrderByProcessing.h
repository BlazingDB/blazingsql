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
#include "operators/OrderBy.h"

#include <cudf/hashing.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

class SortAndSampleSingleNodeKernel : public PhysicalPlan {
public:
	SortAndSampleSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}
	{
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::vector<size_t> tableTotalRows;
		size_t total_num_rows = 0;
		while (input.has_next()) {
			auto batch = input.next();
			auto sortedTable = ral::operators::experimental::sort(batch->toBlazingTableView(), this->expression);
// std::cout<<">>>>>>>>>>>>>>> sortedTable START"<< std::endl;
// ral::utilities::print_blazing_table_view(sortedTable->toBlazingTableView());
// std::cout<<">>>>>>>>>>>>>>> sortedTable END"<< std::endl;
			auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
			sampledTableViews.push_back(sampledTable->toBlazingTableView());
			sampledTables.push_back(std::move(sampledTable));
			tableTotalRows.push_back(batch->view().num_rows());
			this->output_.get_cache("output_a")->addToCache(std::move(sortedTable));
		}
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		auto partitionPlan = ral::operators::experimental::generate_partition_plan(9 /*how many?*/, sampledTableViews, tableTotalRows, this->expression);
std::cout<<">>>>>>>>>>>>>>> PARTITION PLAN START"<< std::endl;
ral::utilities::print_blazing_table_view(partitionPlan->toBlazingTableView());
std::cout<<">>>>>>>>>>>>>>> PARTITION PLAN END"<< std::endl;
		this->output_.get_cache("output_b")->addToCache(std::move(partitionPlan));
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionSingleNodeKernel : public PhysicalPlan {
public:
	PartitionSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"));
		auto partitionPlan = std::move(input_partitionPlan.next());
		
		BatchSequence input(this->input_.get_cache("input_a"));
		while (input.has_next()) {
			auto batch = input.next();
			
			auto partitions = ral::operators::experimental::partition_table(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression);

			// std::cout<<">>>>>>>>>>>>>>> PARTITIONS START"<< std::endl;
			// for(auto& partition : partitions)
			// 	ral::utilities::print_blazing_table_view(ral::frame::BlazingTableView(partition, batch->names()));
			// std::cout<<">>>>>>>>>>>>>>> PARTITIONS START"<< std::endl;

			for (auto i = 0; i < partitions.size(); i++) {
				std::string cache_id = "output_" + std::to_string(i);
				this->output_[cache_id]->addToCache(
					std::make_unique<ral::frame::BlazingTable>(
						std::make_unique<cudf::experimental::table>(partitions[i]),
						batch->names())
					);
			}
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class SortAndSampleKernel : public PhysicalPlan {
public:
	SortAndSampleKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}
	{
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> sampledTables;
		std::vector<ral::frame::BlazingTableView> sampledTableViews;
		std::vector<size_t> tableTotalRows;
		while (input.has_next()) {
			auto batch = input.next();
			auto sortedTable = ral::operators::experimental::sort(batch->toBlazingTableView(), this->expression);
			auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
			sampledTableViews.push_back(sampledTable->toBlazingTableView());
			sampledTables.push_back(std::move(sampledTable));
			tableTotalRows.push_back(batch->view().num_rows());
			this->output_.get_cache("output_a")->addToCache(std::move(sortedTable));
		}
		size_t totalNumRows = std::accumulate(tableTotalRows.begin(), tableTotalRows.end(), 0);
		auto concatSamples = ral::utilities::experimental::concatTables(sampledTableViews);
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		auto partitionPlan = ral::operators::experimental::generate_distributed_partition_plan(9, concatSamples->toBlazingTableView(), totalNumRows, this->expression, this->context.get());
		this->output_.get_cache("output_b")->addToCache(std::move(partitionPlan));
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionKernel : public PhysicalPlan {
public:
	PartitionKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
		BatchSequence input_partitionPlan(this->input_.get_cache("input_b"));
		auto partitionPlan = std::move(input_partitionPlan.next());
		
		BatchSequence input(this->input_.get_cache("input_a"));
		while (input.has_next()) {
			auto batch = input.next();
			
			auto self_partitions = ral::operators::experimental::distribute_table_partitions(partitionPlan->toBlazingTableView(), batch->toBlazingTableView(), this->expression, this->context.get());

			for (auto && self_part : self_partitions) {
				std::string cache_id = "output_" + std::to_string(self_part.first);
				this->output_[cache_id]->addToCache(std::move(self_part.second));
			}
		}

context->incrementQuerySubstep();

		// ExternalBatchSequence input();
		// while (input.has_next()) {
		// 	auto message = input.next();

				// std::string cache_id = "output_" + std::to_string(message.get_part_id());
				// this->output_[cache_id]->addToCache(std::move(message.table()));
		// }

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class MergeStreamKernel : public PhysicalPlan {
public:
	MergeStreamKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context}  {
	}
	
	virtual kstatus run() {
		for (auto idx = 0; idx < this->input_.count(); idx++)
		{
			std::vector<ral::frame::BlazingTableView> tableViews;
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables;
			auto cache_id = "input_" + std::to_string(idx);
			while (!this->input_.get_cache(cache_id)->is_finished()) {
				auto table = this->input_.get_cache(cache_id)->pullFromCache();
				if (table) {
					tableViews.emplace_back(table->toBlazingTableView());
					tables.emplace_back(std::move(table));
				}
			}

			if (tableViews.empty()) {
				// noop
			} else if(tableViews.size() == 1) {
				this->output_.get_cache()->addToCache(std::move(tables.front()));
			}	else {
				// std::cout<<">>>>>>>>>>>>>>> MERGE PARTITIONS START"<< std::endl;
				// for (auto view : tableViews)
				// 	ral::utilities::print_blazing_table_view(view);

				// std::cout<<">>>>>>>>>>>>>>> MERGE PARTITIONS END"<< std::endl;

				auto output = ral::operators::experimental::merge(tableViews, this->expression);

//					ral::utilities::print_blazing_table_view(output->toBlazingTableView());

				this->output_.get_cache()->addToCache(std::move(output));
			}
		}
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

} // namespace batch
} // namespace ral
