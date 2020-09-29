#include "engine/engine.h"
#include "../CalciteInterpreter.h"
#include "../io/data_parser/ArgsUtil.h"
#include "../io/data_parser/CSVParser.h"
#include "../io/data_parser/GDFParser.h"
#include "../io/data_parser/JSONParser.h"
#include "../io/data_parser/OrcParser.h"
#include "../io/data_parser/ArrowParser.h"
#include "../io/data_parser/ParquetParser.h"
#include "../io/data_provider/DummyProvider.h"
#include "../io/data_provider/UriDataProvider.h"
#include "../skip_data/SkipDataProcessor.h"
#include "../execution_graph/logic_controllers/LogicalFilter.h"
#include "communication/network/Server.h"
#include <numeric>
#include <map>
#include "communication/CommunicationData.h"
#include <spdlog/spdlog.h>
#include "CodeTimer.h"
#include "communication/CommunicationInterface/protocols.hpp"
#include "error.hpp"

using namespace fmt::literals;

std::pair<std::vector<ral::io::data_loader>, std::vector<ral::io::Schema>> get_loaders_and_schemas(
	const std::vector<TableSchema> & tableSchemas,
	const std::vector<std::vector<std::string>> & tableSchemaCppArgKeys,
	const std::vector<std::vector<std::string>> & tableSchemaCppArgValues,
	const std::vector<std::vector<std::string>> & filesAll,
	const std::vector<int> & fileTypes,
	const std::vector<std::vector<std::map<std::string, std::string>>> & uri_values){

	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;

	for(int i = 0; i < tableSchemas.size(); i++) {
		auto tableSchema = tableSchemas[i];
		auto files = filesAll[i];
		auto fileType = fileTypes[i];
		
		auto args_map = ral::io::to_map(tableSchemaCppArgKeys[i], tableSchemaCppArgValues[i]);
		
		std::vector<cudf::type_id> types;
		for(int col = 0; col < tableSchemas[i].types.size(); col++) {
			types.push_back(tableSchemas[i].types[col]);
		}

		auto schema = ral::io::Schema(tableSchema.names,
			tableSchema.calcite_to_file_indices,
			types,
			tableSchema.in_file,
			tableSchema.row_groups_ids);

		std::shared_ptr<ral::io::data_parser> parser;
		if(fileType == ral::io::DataType::PARQUET) {
			parser = std::make_shared<ral::io::parquet_parser>();
		} else if(fileType == gdfFileType || fileType == daskFileType) {
			parser = std::make_shared<ral::io::gdf_parser>(tableSchema.blazingTableViews);
		} else if(fileType == ral::io::DataType::ORC) {
			parser = std::make_shared<ral::io::orc_parser>(args_map);
		} else if(fileType == ral::io::DataType::JSON) {
			parser = std::make_shared<ral::io::json_parser>(args_map);
		} else if(fileType == ral::io::DataType::CSV) {
			parser = std::make_shared<ral::io::csv_parser>(args_map);
		} else if(fileType == ral::io::DataType::ARROW){
	     	parser = std::make_shared<ral::io::arrow_parser>(tableSchema.arrow_table);
		}

		std::shared_ptr<ral::io::data_provider> provider;
		std::vector<Uri> uris;
		for(int fileIndex = 0; fileIndex < filesAll[i].size(); fileIndex++) {
			uris.push_back(Uri{filesAll[i][fileIndex]});
			schema.add_file(filesAll[i][fileIndex]);
		}

		if(fileType == ral::io::DataType::CUDF || fileType == ral::io::DataType::DASK_CUDF) {
			// is gdf
			provider = std::make_shared<ral::io::dummy_data_provider>();
		} else {
			// is file (this includes the case where fileType is UNDEFINED too)
			provider = std::make_shared<ral::io::uri_data_provider>(uris, uri_values[i]);
		}
		ral::io::data_loader loader(parser, provider);
		input_loaders.push_back(loader);
		schemas.push_back(schema);
	}
	return std::make_pair(std::move(input_loaders), std::move(schemas));
}

// In case there are columns with the same name, we add a numerical suffix, example:
//
// q1: select n1.n_nationkey, n2.n_nationkey
//         from nation n1 inner join nation n2 on n1.n_nationkey = n2.n_nationkey
//
// original column names:
//     [n_nationkey, n_nationkey]
// final column names:
//     [n_nationkey, n_nationkey0]
//
// q2: select n_nationkey as n_nationkey0,
//         n_regionkey as n_nationkey,
//         n_regionkey + n_regionkey as n_nationkey
//         from nation
//
// original column names:
//     [n_nationkey0, n_nationkey, n_nationkey]
// final column names:
//     [n_nationkey0, n_nationkey, n_nationkey1]

void fix_column_names_duplicated(std::vector<std::string> & col_names){
	std::map<std::string,int> unique_names;

	for(auto & col_name : col_names){
		if(unique_names.find(col_name) == unique_names.end()){
			unique_names[col_name]=-1;
		} else {
			col_name = col_name + std::to_string(++unique_names[col_name]);
		}
	}
}

std::shared_ptr<ral::cache::graph> runGenerateGraph(int32_t masterIndex,
	std::vector<NodeMetaDataTCP> tcpMetadata,
	std::vector<std::string> tableNames,
	std::vector<std::string> tableScans,
	std::vector<TableSchema> tableSchemas,
	std::vector<std::vector<std::string>> tableSchemaCppArgKeys,
	std::vector<std::vector<std::string>> tableSchemaCppArgValues,
	std::vector<std::vector<std::string>> filesAll,
	std::vector<int> fileTypes,
	int32_t ctxToken,
	std::string query,
	uint64_t accessToken,
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values,
	std::map<std::string, std::string> config_options ) {

	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::tie(input_loaders, schemas) = get_loaders_and_schemas(tableSchemas, tableSchemaCppArgKeys,
		tableSchemaCppArgValues, filesAll, fileTypes, uri_values);

	auto logger = spdlog::get("queries_logger");

	using blazingdb::manager::Context;
	using blazingdb::transport::Node;

	auto& communicationData = ral::communication::CommunicationData::getInstance();

	std::vector<Node> contextNodes;
	for(auto currentMetadata : tcpMetadata) {
		auto address =
			blazingdb::transport::Address::TCP(currentMetadata.ip, currentMetadata.communication_port, 0);
		contextNodes.push_back(Node(address, currentMetadata.worker_id));
	}

	Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], "", config_options};

	CodeTimer eventTimer(true);
	logger->info("{ral_id}|{query_id}|{start_time}|{plan}",
									"ral_id"_a=queryContext.getNodeIndex(communicationData.getSelfNode()),
									"query_id"_a=queryContext.getContextToken(),
									"start_time"_a=eventTimer.start_time(),
									"plan"_a=query);

	auto graph = generate_graph(input_loaders, schemas, tableNames, tableScans, query, accessToken, queryContext);

	comm::graphs_info::getInstance().register_graph(ctxToken, graph);
	std::cout<<"graph ptr is"<<graph<<"and token is " << ctxToken<<std::endl;
	return graph;
}

std::unique_ptr<PartitionedResultSet> runExecuteGraph(std::shared_ptr<ral::cache::graph> graph) {
	// Execute query
	std::cout<<"executing"<<std::endl;
	std::vector<std::unique_ptr<ral::frame::BlazingTable>> frames;
	frames = execute_graph(graph);
	std::cout<<"graph exceuted"<<std::endl;

	std::unique_ptr<PartitionedResultSet> result = std::make_unique<PartitionedResultSet>();
	std::cout<<"1"<<std::endl;

	assert( frames.size()>0 );
	std::cout<<"2"<<std::endl;

	result->names = frames[0]->names();
	std::cout<<"3"<<std::endl;

	fix_column_names_duplicated(result->names);
std::cout<<"4"<<std::endl;

	for(auto& cudfTable : frames){
		result->cudfTables.emplace_back(std::move(cudfTable->releaseCudfTable()));
		std::cout<<"5"<<std::endl;

	}
	std::cout<<"6"<<std::endl;

	result->skipdata_analysis_fail = false;
	std::cout<<"pretty much done now deregistering graph is"<<graph <<" and instance is "<<&comm::graphs_info::getInstance()<<std::endl;
	auto token = graph->get_context_token();
	std::cout<<"token is"<<token<<std::endl;
	//comm::graphs_info::getInstance().deregister_graph(token);
	std::cout<<"deregistered"<<std::endl;
	return result;
}

std::unique_ptr<ResultSet> performPartition(int32_t masterIndex,
	std::vector<NodeMetaDataTCP> tcpMetadata,
	int32_t ctxToken,
	const ral::frame::BlazingTableView & table,
	std::vector<std::string> column_names) {

	try {
		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();

		std::vector<int> columnIndices;

		using blazingdb::manager::Context;
		using blazingdb::transport::Node;

		std::vector<Node> contextNodes;
		for(auto currentMetadata : tcpMetadata) {
			auto address =
				blazingdb::transport::Address::TCP(currentMetadata.ip, currentMetadata.communication_port, 0);
			contextNodes.push_back(Node(address, currentMetadata.worker_id));
		}

		Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], "", std::map<std::string, std::string>()};
		
		const std::vector<std::string> & table_col_names = table.names();

		for(auto col_name:column_names){
			auto it = std::find(table_col_names.begin(), table_col_names.end(), col_name);
			if(it != table_col_names.end()){
				columnIndices.push_back(std::distance(table_col_names.begin(), it));
			}
		}

		std::unique_ptr<ral::frame::BlazingTable> frame = ral::processor::process_distribution_table(
			table, columnIndices, &queryContext);

		result->names = frame->names();
		result->cudfTable = frame->releaseCudfTable();
		result->skipdata_analysis_fail = false;
		return result;

	} catch(const std::exception & e) {
		std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
		logger->error("|||{info}|||||",
									"info"_a="In performPartition. What: {}"_format(e.what()));
		logger->flush();

		std::cerr << "**[performPartition]** error partitioning table.\n";
		std::cerr << e.what() << std::endl;
		throw;
	}
}



std::unique_ptr<ResultSet> runSkipData(ral::frame::BlazingTableView metadata,
	std::vector<std::string> all_column_names, std::string query) {

	try {

		std::pair<std::unique_ptr<ral::frame::BlazingTable>, bool> result_pair = ral::skip_data::process_skipdata_for_table(
				metadata, all_column_names, query);

		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();
		result->skipdata_analysis_fail = result_pair.second;
		if (!result_pair.second){ // if could process skip-data
			result->names = result_pair.first->names();
			result->cudfTable = result_pair.first->releaseCudfTable();
		}
		return result;

	} catch(const std::exception & e) {
		std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
		logger->error("|||{info}|||||",
									"info"_a="In runSkipData. What: {}"_format(e.what()));
		logger->flush();

		std::cerr << "**[runSkipData]** error parsing metadata.\n";
		std::cerr << e.what() << std::endl;
		throw;
	}
}


TableScanInfo getTableScanInfo(std::string logicalPlan){

	std::vector<std::string> relational_algebra_steps, table_names;
	std::vector<std::vector<int>> table_columns;
	getTableScanInfo(logicalPlan, relational_algebra_steps, table_names, table_columns);
	return TableScanInfo{relational_algebra_steps, table_names, table_columns};
}

std::pair<TableScanInfo, error_code_t> getTableScanInfo_C(std::string logicalPlan) {

	TableScanInfo result;

	try {
		result = getTableScanInfo(logicalPlan);
		return std::make_pair(result, E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(result, E_EXCEPTION);
	}
}

std::pair<std::unique_ptr<ResultSet>, error_code_t> runSkipData_C(
	ral::frame::BlazingTableView metadata,
	std::vector<std::string> all_column_names,
	std::string query) {

	std::unique_ptr<ResultSet> result = nullptr;

	try {
		result = std::move(runSkipData(metadata,
					all_column_names,
					query));
		return std::make_pair(std::move(result), E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(std::move(result), E_EXCEPTION);
	}
}

std::pair<std::unique_ptr<ResultSet>, error_code_t> performPartition_C(
	int32_t masterIndex,
	std::vector<NodeMetaDataTCP> tcpMetadata,
	int32_t ctxToken,
	const ral::frame::BlazingTableView & table,
	std::vector<std::string> column_names) {

	std::unique_ptr<ResultSet> result = nullptr;

	try {
		result = std::move(performPartition(masterIndex,
					tcpMetadata,
					ctxToken,
					table,
					column_names));
		return std::make_pair(std::move(result), E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(std::move(result), E_EXCEPTION);
	}
}
