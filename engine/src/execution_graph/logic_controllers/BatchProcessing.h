#pragma once

#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/messages/ComponentMessages.h"
#include "communication/network/Server.h"
#include <src/communication/network/Client.h>
#include "parser/expression_utils.hpp"

namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;

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
		std::thread t([this, message_token, context_token](){
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
		: context(context), loader(loader), schema(schema), batch_index{0}, file_index{0}, batch_id{0}, n_batches{0}
	{
		// n_partitions{n_partitions}: TODO Update n_batches using data_loader
		this->provider = loader.get_provider();
		this->parser = loader.get_parser();

		// iterates through files and parses them into columns
		while(this->provider->has_next()) {
			// a file handle that we can use in case errors occur to tell the user which file had parsing issues
			user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
			files.push_back(this->provider->get_next());
		}
		n_files = files.size();
		for (size_t index = 0; index < n_files; index++) {
			ral::io::Schema fileSchema = schema.fileSchema(file_index);
			std::vector<int> row_groups = fileSchema.get_rowgroup_ids(0);
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
			//is_empty_data_source = false;
			//return schema.makeEmptyBlazingTable(projections);

			auto ret = loader.load_batch(context.get(), projections, schema, "", ral::io::data_handle(), file_index, batch_id);
			batch_index++;
			batch_id++;

			if(batch_index == n_batches){
				is_empty_data_source = false;
			}

			return std::move(ret);
		}
		
		//This is just a workaround, mainly for ORC files
		if(all_row_groups[file_index].size()==1 && all_row_groups[file_index][0]==-1){
			batch_id = -1; //load all the stripes when can't get the rowgroups size
		}

		auto ret = loader.load_batch(context.get(), projections, schema, user_readable_file_handles[file_index], files[file_index], file_index, batch_id);
		batch_index++;
		
		batch_id++;
		if (batch_id == all_row_groups[file_index].size()) {
			file_index++;
			batch_id = 0;
		}
		return std::move(ret);
	}
	bool wait_for_next() {
		return is_empty_data_source || (file_index < n_files and batch_index < n_batches);
	}

	void set_projections(std::vector<size_t> projections) {
		this->projections = projections;
	}

private:
	std::shared_ptr<ral::io::data_provider> provider;
	std::shared_ptr<ral::io::data_parser> parser;
	std::vector<std::string> user_readable_file_handles;
	std::vector<ral::io::data_handle> files;

	std::shared_ptr<Context> context;
	std::vector<size_t> projections;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	size_t file_index;
	size_t batch_index;
	size_t batch_id;
	size_t n_batches;
	size_t n_files;
	std::vector<std::vector<int>> all_row_groups; 
	bool is_empty_data_source;
};

struct PhysicalPlan : kernel {
	virtual kstatus run() = 0;

	std::shared_ptr<ral::cache::CacheMachine>  input_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->input_.get_cache(kernel_id);
	}
	std::shared_ptr<ral::cache::CacheMachine>  output_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->output_.get_cache(kernel_id);
	}

	void add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string kernel_id = "") {
		if (kernel_id.empty()) {
			kernel_id = std::to_string(this->get_id());
		}
		
		std::string message_id = std::to_string((int)this->get_type_id()) + "_" + kernel_id;
		this->output_.get_cache(kernel_id)->addToCache(std::move(table), message_id);
	}

	void add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string kernel_id = "") {
		if (kernel_id.empty()) {
			kernel_id = std::to_string(this->get_id());
		}

		std::string message_id = std::to_string((int)this->get_type_id()) + "_" + kernel_id;
		this->output_.get_cache(kernel_id)->addCacheData(std::move(cache_data), message_id);
	}
	
	void add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string kernel_id = "") {
		if (kernel_id.empty()) {
			kernel_id = std::to_string(this->get_id());
		}

		std::string message_id = std::to_string((int)this->get_type_id()) + "_" + kernel_id;
		this->output_.get_cache(kernel_id)->addHostFrameToCache(std::move(host_table), message_id);
	}

	std::string get_message_id(){
		return std::to_string((int)this->get_type_id()) + "_" + std::to_string(this->get_id());
	}

	size_t n_batches() {
		return n_batches_; // TODO: use set_n_batches(n_batches) in make_kernel
	}
private:
	size_t n_batches_;
};

class TableScan : public PhysicalPlan {
public:
	TableScan(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
	: PhysicalPlan(), input(loader, schema, context)
	{}
	virtual kstatus run() {
		while( input.wait_for_next() ) {
			auto batch = input.next();
			this->add_to_output_cache(std::move(batch));
		}
		return kstatus::proceed;
	}
private:
	DataSourceSequence input;
};

class BindableTableScan : public PhysicalPlan {
public:
	BindableTableScan(std::string & expression, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
	: PhysicalPlan(), input(loader, schema, context), expression(expression), context(context)
	{}
	virtual kstatus run() {
		input.set_projections(get_projections(expression));
		int batch_count = 0;
		while (input.wait_for_next() ) {
			try {
				auto batch = input.next();

				if(is_filtered_bindable_scan(expression)) {
					auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
					this->add_to_output_cache(std::move(columns));
				}
				else{
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
private:
	DataSourceSequence input;
	std::shared_ptr<Context> context;
	std::string expression;
};

class Projection : public PhysicalPlan {
public:
	Projection(const std::string & queryString, std::shared_ptr<Context> context)
	{
		this->context = context;
		this->expression = queryString;
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

class Filter : public PhysicalPlan {
public:
	Filter(const std::string & queryString, std::shared_ptr<Context> context)
	{
		this->context = context;
		this->expression = queryString;
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
private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class Print : public PhysicalPlan {
public:
	Print() : PhysicalPlan() { ofs = &(std::cout); }
	Print(std::ostream & stream) : PhysicalPlan() { ofs = &stream; }
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

} // namespace batch
} // namespace ral
