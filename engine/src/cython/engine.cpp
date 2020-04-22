#include "../../include/engine/engine.h"
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
		
		auto kwargs = ral::io::to_map(tableSchemaCppArgKeys[i], tableSchemaCppArgValues[i]);
		ral::io::ReaderArgs args = ral::io::getReaderArgs((ral::io::DataType) fileType, kwargs);

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
			parser = std::make_shared<ral::io::orc_parser>(args.orcReaderArg);
		} else if(fileType == ral::io::DataType::JSON) {
			parser = std::make_shared<ral::io::json_parser>(args.jsonReaderArg);
		} else if(fileType == ral::io::DataType::CSV) {
			parser = std::make_shared<ral::io::csv_parser>(args.csvReaderArg);
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

std::unique_ptr<ResultSet> runQuery(int32_t masterIndex,
	std::vector<NodeMetaDataTCP> tcpMetadata,
	std::vector<std::string> tableNames,
	std::vector<TableSchema> tableSchemas,
	std::vector<std::vector<std::string>> tableSchemaCppArgKeys,
	std::vector<std::vector<std::string>> tableSchemaCppArgValues,
	std::vector<std::vector<std::string>> filesAll,
	std::vector<int> fileTypes,
	int32_t ctxToken,
	std::string query,
	uint64_t accessToken,
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values,
	bool use_execution_graph) {

	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::tie(input_loaders, schemas) = get_loaders_and_schemas(tableSchemas, tableSchemaCppArgKeys,
		tableSchemaCppArgValues, filesAll, fileTypes, uri_values);

	try {
		using blazingdb::manager::experimental::Context;
		using blazingdb::transport::experimental::Node;

		std::vector<Node> contextNodes;
		for(auto currentMetadata : tcpMetadata) {
			auto address =
				blazingdb::transport::experimental::Address::TCP(currentMetadata.ip, currentMetadata.communication_port, 0);
			contextNodes.push_back(Node(address));
		}

		Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], ""};
		ral::communication::network::experimental::Server::getInstance().registerContext(ctxToken);

		// Execute query
		std::unique_ptr<ral::frame::BlazingTable> frame;
		if (use_execution_graph) {
			frame = execute_plan(input_loaders, schemas, tableNames, query, accessToken, queryContext);

		} else {
			frame = evaluate_query(input_loaders, schemas, tableNames, query, accessToken, queryContext);
		}
		
		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();
		result->names = frame->names();
		result->cudfTable = frame->releaseCudfTable();
		result->skipdata_analysis_fail = false;
		return result;
	} catch(const std::exception & e) {
		std::cerr << e.what() << std::endl;
		throw;
	}
}

std::unique_ptr<ResultSet> performPartition(int32_t masterIndex,
	std::vector<NodeMetaDataTCP> tcpMetadata,
	int32_t ctxToken,
	const ral::frame::BlazingTableView & table,
	std::vector<std::string> column_names) {

	try {
		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();

		std::vector<int> columnIndices;

		using blazingdb::manager::experimental::Context;
		using blazingdb::transport::experimental::Node;

		std::vector<Node> contextNodes;
		for(auto currentMetadata : tcpMetadata) {
			auto address =
				blazingdb::transport::experimental::Address::TCP(currentMetadata.ip, currentMetadata.communication_port, 0);
			contextNodes.push_back(Node(address));
		}

		Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], ""};
		ral::communication::network::experimental::Server::getInstance().registerContext(ctxToken);

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
