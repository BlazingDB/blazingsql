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

namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;

class BatchSequence {
public:
	BatchSequence(std::shared_ptr<ral::cache::CacheMachine> cache)
	: cache{cache}
	{}

	RecordBatch next() {
		return cache->pullFromCache();
	}
	bool has_next() {
		return not cache->is_finished();
	}
private:
	std::shared_ptr<ral::cache::CacheMachine> cache;
};

using ColumnDataPartitionMessage = ral::communication::messages::experimental::ColumnDataPartitionMessage;
typedef ral::communication::network::experimental::Server Server;
typedef ral::communication::network::experimental::Client Client;
using ral::communication::messages::experimental::ReceivedHostMessage;


template<class MessageType>
class ExternalBatchSequence {
public:
	ExternalBatchSequence(std::shared_ptr<Context> context, std::shared_ptr<ral::cache::CacheMachine> cache)
		: context{context}, cache{cache}
	{
		std::string context_comm_token = context->getContextCommunicationToken();
		const uint32_t context_token = context->getContextToken();
		const std::string message_token = MessageType::MessageID() + "_" + context_comm_token;
		Server::getInstance().registerListener(context_token, message_token, 
			[this](uint32_t context_token, std::string message_token) mutable {
				auto message = Server::getInstance().getMessage(context_token, message_token);
				auto concreteMessage = std::static_pointer_cast<ReceivedHostMessage>(message);
				auto host_table = concreteMessage->releaseBlazingHostTable();
				this->cache->addHostFrameToCache(std::move(host_table));
			});
	}

	RecordBatch next() {
		return cache->pullFromCache();
	}
	bool has_next() {
		return not cache->is_finished();
	}
private:
	std::shared_ptr<Context> context;
	std::shared_ptr<ral::cache::CacheMachine> cache;
};

using ExternalBatchColumnDataSequence = ExternalBatchSequence<ColumnDataPartitionMessage>;

class DataSourceSequence {
public:
	DataSourceSequence(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
		: context(context), loader(loader), schema(schema), batch_index{0}, file_index{0}, batch_id{0}
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
		

	}
	RecordBatch next() {
		// std::cout << "Datasource.next: " << file_index << "|" << batch_id << "|" << all_row_groups[file_index].size() << std::endl;
		auto ret = loader.load_batch(context.get(), {}, schema, user_readable_file_handles[file_index], files[file_index], file_index, batch_id);
		batch_index++;
		
		batch_id++;
		if (batch_id == all_row_groups[file_index].size()) {
			file_index++;
			batch_id = 0;
		}
		return std::move(ret);
	}
	bool has_next() {
		return file_index < n_files and batch_index < n_batches;
	}

private:
	std::shared_ptr<ral::io::data_provider> provider;
	std::shared_ptr<ral::io::data_parser> parser;
	std::vector<std::string> user_readable_file_handles;
	std::vector<ral::io::data_handle> files;

	std::shared_ptr<Context> context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	size_t file_index;
	size_t batch_index;
	size_t batch_id;
	size_t n_batches;
	size_t n_files;
	std::vector<std::vector<int>> all_row_groups; 
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
		while( input.has_next() ) {
			auto batch = input.next();
			this->output_cache()->addToCache(std::move(batch));
		}
		return kstatus::proceed;
	}
private:
	DataSourceSequence input;
};

class Projection : public PhysicalPlan {
public:
	Projection(const std::string & queryString, std::shared_ptr<Context> context)
	{
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		BatchSequence input(this->input_cache());
		while (input.has_next() ) {
			auto batch = input.next();
			auto columns = ral::processor::process_project(std::move(batch), expression, context.get());
			this->output_cache()->addToCache(std::move(columns));
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
		BatchSequence input(this->input_cache());
		while (input.has_next() ) {
			auto batch = input.next();
			auto columns = ral::processor::process_filter(batch->toBlazingTableView(), expression, context.get());
			this->output_cache()->addToCache(std::move(columns));
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
		BatchSequence input(this->input_cache());
		while (input.has_next() ) {
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
