#pragma once
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/DataLoader.h"
#include "io/Schema.h"

namespace ral {
namespace cache {
//
//struct Schema {};
//
//using RecordBatch = std::unique_ptr<BlazingTable>;
//
//struct DataSourceKernel : Kernel {
//	virtual Schema schema() = 0;
//	virtual std::vector<RecordBatch> scan(const std::vector<std::string> & columns) = 0;
//};

class DataSourceKernel : public kernel {
public:
	DataSourceKernel(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
	: kernel(), context(context), loader(loader), schema(schema)
	{}

	virtual kstatus run() {
		CodeTimer blazing_timer;
		blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
		auto table = loader.load_data(context.get(), {}, schema);
		context->incrementQueryStep();
		int num_rows = table->num_rows();
		Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query load_data", "num rows", num_rows));
		blazing_timer.reset();
		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	std::shared_ptr<Context> context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
};

} // namespace cache
} // namespace ral
