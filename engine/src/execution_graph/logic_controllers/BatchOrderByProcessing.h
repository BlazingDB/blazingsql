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


namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

class SortAndSampleSingleNodeKernel :public kernel {
public:
	SortAndSampleSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context}
	{
		this->query_graph = query_graph;
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
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
	// std::cout<<">>>>>>>>>>>>>>> sortedTable START"<< std::endl;
	// ral::utilities::print_blazing_table_view(sortedTable->toBlazingTableView());
	// std::cout<<">>>>>>>>>>>>>>> sortedTable END"<< std::endl;
				auto sampledTable = ral::operators::experimental::sample(batch->toBlazingTableView(), this->expression);
				sampledTableViews.push_back(sampledTable->toBlazingTableView());
				sampledTables.push_back(std::move(sampledTable));
				tableTotalRows.push_back(batch->view().num_rows());
				this->add_to_output_cache(std::move(sortedTable), "output_a");
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				// Note that we have to handle the collected samples in a special way. We need to compare to the current batch_count and perhaps evict one set of samples 
				std::string err = "ERROR: in SortAndSampleSingleNodeKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		cudf::size_type num_partitions = context->getTotalNodes() * 4; // WSM TODO this is a hardcoded number for now. THis needs to change in the near future
		auto partitionPlan = ral::operators::experimental::generate_partition_plan(num_partitions, sampledTableViews, tableTotalRows, this->expression);
// std::cout<<">>>>>>>>>>>>>>> PARTITION PLAN START"<< std::endl;
// ral::utilities::print_blazing_table_view(partitionPlan->toBlazingTableView());
// std::cout<<">>>>>>>>>>>>>>> PARTITION PLAN END"<< std::endl;
		this->add_to_output_cache(std::move(partitionPlan), "output_b");
			
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionSingleNodeKernel :public kernel {
public:
	PartitionSingleNodeKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
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
				std::string err = "ERROR: in PartitionSingleNodeKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class SortAndSampleKernel :public kernel {
public:
	SortAndSampleKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context}
	{
		this->query_graph = query_graph;
		this->output_.add_port("output_a", "output_b");
	}
	
	virtual kstatus run() {
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
				std::string err = "ERROR: in SortAndSampleKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}
		size_t totalNumRows = std::accumulate(tableTotalRows.begin(), tableTotalRows.end(), 0);
		auto concatSamples = ral::utilities::experimental::concatTables(sampledTableViews);
		// call total_num_partitions = partition_function(size_of_all_data, number_of_nodes, avaiable_memory, ....)
		cudf::size_type num_partitions = context->getTotalNodes() * 4; // WSM TODO this is a hardcoded number for now. THis needs to change in the near future
		auto partitionPlan = ral::operators::experimental::generate_distributed_partition_plan(num_partitions, concatSamples->toBlazingTableView(), totalNumRows, this->expression, this->context.get());
		this->add_to_output_cache(std::move(partitionPlan), "output_b");
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionKernel :public kernel {
public:
	PartitionKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
		this->query_graph = query_graph;
		this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
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
					std::string err = "ERROR: in PartitionKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
					std::cout<<err<<std::endl;
				}	
			}
			ral::distribution::experimental::notifyLastTablePartitions(this->context.get(), ColumnDataPartitionMessage::MessageID());
		});
		
		BlazingThread consumer([this](){
			ExternalBatchColumnDataSequence external_input(context);
			std::unique_ptr<ral::frame::BlazingHostTable> host_table;
			while (host_table = external_input.next()) {
				std::string cache_id = "output_" + std::to_string(host_table->get_part_id());
				this->add_to_output_cache(std::move(host_table), cache_id);
			}
		});
		generator.join();
		consumer.join();

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class MergeStreamKernel :public kernel {
public:
	MergeStreamKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context}  {
		this->query_graph = query_graph;
	}
	
	virtual kstatus run() {
		int cache_count = 0;
		for (auto idx = 0; idx < this->input_.count(); idx++)
		{	
			try {
				std::vector<ral::frame::BlazingTableView> tableViews;
				std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables;
				auto cache_id = "input_" + std::to_string(idx);
				while (this->input_.get_cache(cache_id)->wait_for_next()) {
					auto table = this->input_.get_cache(cache_id)->pullFromCache();
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
				cache_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				std::string err = "ERROR: in MergeStreamKernel cache " + std::to_string(cache_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}
		
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};


class LimitKernel :public kernel {
public:
	LimitKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context}  {
		this->query_graph = query_graph;
	}
	
	virtual kstatus run() {
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
				this->output_cache()->addCacheData(std::move(cache_data));
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
					std::string err = "ERROR: in LimitKernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
					std::cout<<err<<std::endl;
				}
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
