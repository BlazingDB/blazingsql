#pragma once

#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/network/Server.h"
#include <src/communication/network/Client.h>
#include "parser/expression_utils.hpp"


#include "utilities/random_generator.cuh"
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include "execution_graph/logic_controllers/TableScan.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>

#include <src/operators/OrderBy.h>
#include <src/operators/GroupBy.h>
#include <src/utilities/DebuggingUtils.h>
#include <stack>
#include "io/DataLoader.h"
#include "io/Schema.h"
#include <Util/StringUtil.h>
#include "CodeTimer.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include "parser/expression_utils.hpp"

#include <cudf/copying.hpp>
#include <cudf/merge.hpp>
#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include <src/CalciteInterpreter.h>
#include <src/utilities/CommonOperations.h>

#include "distribution/primitives.h"
#include "config/GPUManager.cuh"
#include "CacheMachine.h"
#include "blazingdb/concurrency/BlazingThread.h"

#include "taskflow/graph.h"
namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;
using frame_type = std::unique_ptr<ral::frame::BlazingTable>;
using Context = blazingdb::manager::experimental::Context;
class BatchSequence {
public:
	BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr, const ral::cache::kernel * kernel = nullptr)
	: cache{cache}, kernel{kernel}
	{}
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
		this->cache = cache;
	}
	RecordBatch next() {
		return cache->pullFromCache();
	}
	bool wait_for_next() {
		if (kernel) {
			std::string message_id = std::to_string((int)kernel->get_type_id()) + "_" + std::to_string(kernel->get_id()); 
			// std::cout<<">>>>> WAIT_FOR_NEXT id : " <<  message_id <<std::endl;
		}
		
		return cache->wait_for_next();
	}

	bool has_next_now() {
		return cache->has_next_now();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache;
	const ral::cache::kernel * kernel;
};

class BatchSequenceBypass {
public:
	BatchSequenceBypass(std::shared_ptr<ral::cache::CacheMachine> cache = nullptr)
	: cache{cache}
	{}
	void set_source(std::shared_ptr<ral::cache::CacheMachine> cache) {
		this->cache = cache;
	}
	std::unique_ptr<ral::cache::CacheData> next() {
		return cache->pullCacheData();
	}
	// cache->addToRawCache(cache->pullFromRawCache())
	bool wait_for_next() {
		return cache->wait_for_next();
	}

	bool has_next_now() {
		return cache->has_next_now();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache;
};


using ColumnDataPartitionMessage = ral::communication::messages::experimental::ColumnDataPartitionMessage;
typedef ral::communication::network::experimental::Server Server;
typedef ral::communication::network::experimental::Client Client;
using ral::communication::messages::experimental::ReceivedHostMessage;


class ExternalBatchColumnDataSequence {
public:
	ExternalBatchColumnDataSequence(std::shared_ptr<Context> context, std::string message_token = "")
		: context{context}, last_message_counter{context->getTotalNodes() - 1}
	{
		host_cache = std::make_shared<ral::cache::HostCacheMachine>();
		std::string context_comm_token = context->getContextCommunicationToken();
		const uint32_t context_token = context->getContextToken();
		if (message_token.length() == 0) {
			message_token = ColumnDataPartitionMessage::MessageID() + "_" + context_comm_token;
		}
		BlazingMutableThread t([this, message_token, context_token](){
			while(true){
					auto message = Server::getInstance().getHostMessage(context_token, message_token);
					if(!message) {
						--last_message_counter;
						if (last_message_counter == 0 ){
							this->host_cache->finish();
							break;
						}
					}	else{
						auto concreteMessage = std::static_pointer_cast<ReceivedHostMessage>(message);
						assert(concreteMessage != nullptr);
						auto host_table = concreteMessage->releaseBlazingHostTable();
						host_table->setPartitionId(concreteMessage->getPartitionId());
						this->host_cache->addToCache(std::move(host_table));			
					}
			}
		});
		t.detach();
	} 

	bool wait_for_next() {
		return host_cache->wait_for_next();
	}

	std::unique_ptr<ral::frame::BlazingHostTable> next() {
		return host_cache->pullFromCache();
	}
private:
	std::shared_ptr<Context> context;
	std::shared_ptr<ral::cache::HostCacheMachine> host_cache;
	int last_message_counter;
};


class DataSourceSequence {
public:
	DataSourceSequence(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
		: context(context), loader(loader), schema(schema), batch_index{0}, cur_file_index{0}, cur_row_group_index{0}, n_batches{0}
	{
		// n_partitions{n_partitions}: TODO Update n_batches using data_loader
		this->provider = loader.get_provider();
		this->parser = loader.get_parser();

		if(this->provider->has_next()) {
			// a file handle that we can use in case errors occur to tell the user which file had parsing issues
			this->cur_data_handle = this->provider->get_next();
		}

		n_files = schema.get_files().size();
		for (size_t index = 0; index < n_files; index++) {
			std::vector<cudf::size_type> row_groups = schema.get_rowgroup_ids(index);
			n_batches += row_groups.size();
			all_row_groups.push_back(row_groups);
		}

		is_empty_data_source = (n_files == 0);

		if(is_empty_data_source){
			n_batches = parser->get_num_partitions();
		}
	}

	RecordBatch next() {
		if (is_empty_data_source) {
			if(n_batches==0){
				is_empty_data_source = false;
				return schema.makeEmptyBlazingTable(projections);
			}

			auto ret = loader.load_batch(context.get(), projections, schema, ral::io::data_handle(), cur_file_index, cur_row_group_index);
			batch_index++;
			cur_row_group_index++;

			if(batch_index == n_batches){
				is_empty_data_source = false;
			}

			return std::move(ret);
		}
		
		cudf::size_type row_group_id = all_row_groups[cur_file_index][cur_row_group_index];
		auto ret = loader.load_batch(context.get(), projections, schema, this->cur_data_handle, cur_file_index, row_group_id);
		batch_index++;
		cur_row_group_index++;
		if (cur_row_group_index == all_row_groups[cur_file_index].size()) {
			cur_file_index++;
			cur_row_group_index = 0;
			if(this->provider->has_next()) {
				// a file handle that we can use in case errors occur to tell the user which file had parsing issues
				this->cur_data_handle = this->provider->get_next();
			}
		}

		return std::move(ret);
	}

	bool wait_for_next() {
		return is_empty_data_source || (cur_file_index < n_files and batch_index.load() < n_batches);
	}

	void set_projections(std::vector<size_t> projections) {
		this->projections = projections;
	}

	// this function can be called from a parallel thread, so we want it to be thread safe
	size_t get_batch_index() {
		return batch_index.load();
	}

	size_t get_num_batches() {
		return n_batches;
	}

private:
	std::shared_ptr<ral::io::data_provider> provider;
	std::shared_ptr<ral::io::data_parser> parser;
	
	std::shared_ptr<Context> context;
	std::vector<size_t> projections;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	size_t cur_file_index;
	size_t cur_row_group_index;
	ral::io::data_handle cur_data_handle;
	std::atomic<size_t> batch_index;
	size_t n_batches;
	size_t n_files;
	std::vector<std::vector<int>> all_row_groups; 
	bool is_empty_data_source;
};

class TableScan : public kernel {
public:
	TableScan(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(), input(loader, schema, context)
	{
		this->query_graph = query_graph;
	}
	virtual kstatus run() {
		while( input.wait_for_next() ) {
			auto batch = input.next();
			this->add_to_output_cache(std::move(batch));
		}
		return kstatus::proceed;
	}

	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		double rows_so_far = (double)this->output_.total_rows_added();
		double num_batches = (double)this->input.get_num_batches();
		double current_batch = (double)this->input.get_batch_index();
		if (current_batch == 0 || num_batches == 0){
			return std::make_pair(false,0);
		} else {
			return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
		}
	}

private:
	DataSourceSequence input;
};

class BindableTableScan : public kernel {
public:
	BindableTableScan(std::string & expression, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context, 
		std::shared_ptr<ral::cache::graph> query_graph)
	: kernel(), input(loader, schema, context), expression(expression), context(context)
	{
		this->query_graph = query_graph;
	}
	virtual kstatus run() {
		input.set_projections(get_projections(expression));
		int batch_count = 0;
		while (input.wait_for_next() ) {
			try {
				auto batch = input.next();

				if(is_filtered_bindable_scan(expression)) {
					auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
					columns->setNames(fix_column_aliases(columns->names(), expression));
					this->add_to_output_cache(std::move(columns));
				}
				else{
					batch->setNames(fix_column_aliases(batch->names(), expression));
					this->add_to_output_cache(std::move(batch));
				}
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				std::string err = "ERROR: in BindableTableScan kernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}
		return kstatus::proceed;
	}
	
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows(){
		double rows_so_far = (double)this->output_.total_rows_added();
		double num_batches = (double)this->input.get_num_batches();
		double current_batch = (double)this->input.get_batch_index();
		if (current_batch == 0 || num_batches == 0){
			return std::make_pair(false,0);
		} else {
			return std::make_pair(true, (uint64_t)(rows_so_far/(current_batch/num_batches)));
		}
	}

private:
	DataSourceSequence input;
	std::shared_ptr<Context> context;
	std::string expression;
};

class Projection : public kernel {
public:
	Projection(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	{
		this->context = context;
		this->expression = queryString;
		this->query_graph = query_graph;
	}
	virtual kstatus run() {
		BatchSequence input(this->input_cache(), this);
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				auto batch = input.next();
				auto columns = ral::processor::process_project(std::move(batch), expression, context.get());
				this->add_to_output_cache(std::move(columns));
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				std::string err = "ERROR: in Projection kernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class Filter : public kernel {
public:
	Filter(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
	{
		this->context = context;
		this->expression = queryString;
		this->query_graph = query_graph;
	}
	virtual kstatus run() {
		BatchSequence input(this->input_cache(), this);
		int batch_count = 0;
		while (input.wait_for_next()) {
			try {
				auto batch = input.next();
				auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
				this->add_to_output_cache(std::move(columns));
				batch_count++;
			} catch(const std::exception& e) {
				// TODO add retry here
				std::string err = "ERROR: in Filter kernel batch " + std::to_string(batch_count) + " for " + expression + " Error message: " + std::string(e.what());
				std::cout<<err<<std::endl;
			}
		}
		return kstatus::proceed;
	}
	
	std::pair<bool, uint64_t> get_estimated_output_num_rows(){
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
private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class Print : public kernel {
public:
	Print() : kernel() { ofs = &(std::cout); }
	Print(std::ostream & stream) : kernel() { ofs = &stream; }
	virtual kstatus run() {
		std::lock_guard<std::mutex> lg(print_lock);
		BatchSequence input(this->input_cache(), this);
		while (input.wait_for_next() ) {
			auto batch = input.next();
			ral::utilities::print_blazing_table_view(batch->toBlazingTableView());
		}
		return kstatus::stop;
	}

protected:
	std::ostream * ofs = nullptr;
	std::mutex print_lock;
};
 

class OutputKernel : public kernel {
public:
	OutputKernel() : kernel("OutputKernel") {  }
	virtual kstatus run() {
		output = std::move(this->input_.get_cache()->pullFromCache());
		return kstatus::stop;
	}
	
	frame_type	release() {
		return std::move(output);
	}

protected:
	frame_type output;
};


namespace test {
class generate : public kernel {
public:
	generate(std::int64_t count = 1000) : kernel(), count(count) {}
	virtual kstatus run() {

		cudf::test::fixed_width_column_wrapper<int32_t> column1{{0, 1, 2, 3, 4, 5}, {1, 1, 1, 1, 1, 1}};

		CudfTableView cudfTableView{{column1} };

		const std::vector<std::string> columnNames{"column1"};
		ral::frame::BlazingTableView blazingTableView{cudfTableView, columnNames};

		std::unique_ptr<ral::frame::BlazingTable> table = ral::generator::generate_sample(blazingTableView, 4);

		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	std::int64_t count;
};
}
using GeneratorKernel = test::generate;


} // namespace batch
} // namespace ral
