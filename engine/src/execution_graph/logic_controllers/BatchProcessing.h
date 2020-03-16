#pragma once
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/DataLoader.h"
#include "io/Schema.h"

namespace ral {
namespace batch {

//
//struct Schema {};
//
//using RecordBatch = std::unique_ptr<BlazingTable>;
//
//struct DataSourceKernel : Kernel {
//	virtual Schema schema() = 0;
//	virtual std::vector<RecordBatch> scan(const std::vector<std::string> & columns) = 0;
//};

class TableScanKernel : public kernel {
public:
	TableScanKernel(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
	: kernel(), context(context), loader(loader), schema(schema)
	{}

	virtual kstatus run() {
		CodeTimer blazing_timer;
		blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
		
		for(auto &&batch : loader.load_data(context.get(), {}, schema) ) {
			context->incrementQueryStep();
			int num_rows = batch->num_rows();
			Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query load_data", "num rows", num_rows));
			blazing_timer.reset();
			this->output_.get_cache()->addToCache(std::move(batch));
		}
		return (kstatus::proceed);
	}

private:
	std::shared_ptr<Context> context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
};

 class ProjectKernel : public kernel {
public:
	 ProjectKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->context = context;
		this->expression = queryString;
	}

	virtual kstatus run() {
		while (frame_type batch = std::move(this->input_.get_cache()->pullFromCache()) ) {
			auto output = ral::processor::process_project(std::move(batch), expression, context.get());
			this->output_.get_cache()->addToCache(std::move(output));
			context->incrementQueryStep();
		}
		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

//
// TableScanKernel scan
// ProjectKernel project
// ra::cache::graph g;
// g += scan >> project;
// g += project >> output;

// Join!  nothing
// Sort !! ~~~~
// 	Groupby !! worng 


// 1. ProjectKernel ok
// 2. FilterKernel ~ same as before
// 3. JoinKernel ~  replicar distribuido
// 4. UnionKernel ~ 
// 	=> batches => simple! ??
// 		==> sort => 
// 	=> batches => particionar => pivots ??

// 5. SortKernel ~ replicar distribuido  
// 6. AggregateKernel ~ replicar distribuido
// 7. TableScanKernel ~ ok
// 8. BindableTableScanKernel ~ ok


class JoinKernel :  public kernel {
public:
	JoinKernel(const std::string & queryString, std::shared_ptr<Context> context) {
			this->input_.add_port("input_a", "input_b");
			this->context = context;
			this->expression = queryString;
		}
		virtual kstatus run() {
			try {
				CodeTimer blazing_timer;
				blazing_timer.reset();   
				frame_type left_frame_result;
				frame_type right_frame_result;
				while (left_frame_result = std::move(this->input_["input_a"]->pullFromCache())  and  
						right_frame_result = std::move(this->input_["input_b"]->pullFromCache())) {
					int numLeft = left_frame_result->num_rows();
					int numRight = right_frame_result->num_rows();

					std::string new_join_statement, filter_statement;
					StringUtil::findAndReplaceAll(expression, "IS NOT DISTINCT FROM", "=");
					split_inequality_join_into_join_and_filter(expression, new_join_statement, filter_statement);

					std::unique_ptr<ral::frame::BlazingTable> result_frame = ral::processor::process_logical_join(context.get(), left_frame_result->toBlazingTableView(), right_frame_result->toBlazingTableView(), new_join_statement);

					std::string extraInfo = "left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
					Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query process_join", "num rows result", result_frame->num_rows(), extraInfo));
					blazing_timer.reset();
					context->incrementQueryStep();
					if (filter_statement != ""){
						result_frame = ral::processor::process_filter(result_frame->toBlazingTableView(), filter_statement, context.get());
						Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query inequality join process_filter", "num rows", result_frame->num_rows()));
						blazing_timer.reset();
						context->incrementQueryStep();
					}
					this->output_.get_cache()->addToCache(std::move(result_frame));
					return kstatus::proceed;
				}
			} catch (std::exception &e) {
				std::cerr << "Exception-JoinKernel: " << e.what() << std::endl;
			}
			return kstatus::stop;
		}

	private:
		std::shared_ptr<Context> context;
		std::string expression;
	};


} // namespace batch
} // namespace ral
