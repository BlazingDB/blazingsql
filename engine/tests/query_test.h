#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <GDFColumn.cuh>
#include <GDFCounter.cuh>
#include <gtest/gtest.h>

#include "blazingdb/io/Library/Logging/ServiceLogging.h"
#include <blazingdb/io/Library/Logging/CoutOutput.h>
#include <blazingdb/io/Library/Logging/Logger.h>

#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>

namespace interpreter_wrapper {

gdf_error evaluate_query(std::vector<std::vector<gdf_column_cpp>> input_tables,
	std::vector<std::string> table_names,
	std::vector<std::vector<std::string>> column_names,
	std::string logicalPlan,
	std::vector<gdf_column_cpp> & outputs) {
	std::vector<std::string> splitted = StringUtil::split(logicalPlan, "\n");
	if(splitted[splitted.size() - 1].length() == 0) {
		splitted.erase(splitted.end() - 1);
	}
	uint32_t ctxToken = 0;
	uint32_t masterIndex = 0;
	using blazingdb::manager::experimental::Context;
	using blazingdb::transport::experimental::Node;

	std::vector<std::shared_ptr<Node>> contextNodes;
	auto address = blazingdb::transport::Address::TCP("127.0.0.1", 8001, 1234);
	contextNodes.push_back(Node::Make(address));
	Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], ""};

	blazing_frame output_frame = evaluate_split_query(input_tables, table_names, column_names, splitted, &queryContext);

	for(size_t i = 0; i < output_frame.get_width(); i++) {
		GDFRefCounter::getInstance()->deregister_column(output_frame.get_column(i).get_gdf_column());
		outputs.push_back(output_frame.get_column(i));
	}

	return GDF_SUCCESS;
}
}  // namespace interpreter_wrapper

struct query_test : public ::testing::Test {
	void SetUp() {
		rmmInitialize(nullptr);
		auto output = new Library::Logging::CoutOutput();
		Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
	}

	gdf_error evaluate_query(std::vector<std::vector<gdf_column_cpp>> input_tables,
		std::vector<std::string> table_names,
		std::vector<std::vector<std::string>> column_names,
		std::string logicalPlan,
		std::vector<gdf_column_cpp> & outputs) {
		return interpreter_wrapper::evaluate_query(input_tables, table_names, column_names, logicalPlan, outputs);
	}
};