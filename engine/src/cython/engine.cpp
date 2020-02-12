#include "../../include/engine/engine.h"
#include "../CalciteInterpreter.h"
#include "../io/DataLoader.h"
#include "../io/Schema.h"
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
#include "communication/network/Server.h"
#include <numeric>

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
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values) {

	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;

	for(int i = 0; i < tableSchemas.size(); i++) {

		auto tableSchema = tableSchemas[i];
		auto files = filesAll[i];
		auto fileType = fileTypes[i];
		std::vector<cudf::type_id> types;

		auto kwargs = ral::io::to_map(tableSchemaCppArgKeys[i], tableSchemaCppArgValues[i]);
		tableSchema.args = ral::io::getReaderArgs((ral::io::DataType) fileType, kwargs);

		for(int col = 0; col < tableSchemas[i].types.size(); col++) {
			types.push_back(tableSchemas[i].types[col]);
		}

		auto schema = ral::io::Schema(tableSchema.names,
			tableSchema.calcite_to_file_indices,
			types,
			tableSchema.num_row_groups,
			tableSchema.in_file,
			tableSchema.row_groups_ids);

		std::shared_ptr<ral::io::data_parser> parser;
		if(fileType == ral::io::DataType::PARQUET) {
			parser = std::make_shared<ral::io::parquet_parser>();
		} else if(fileType == gdfFileType || fileType == daskFileType) {
			parser = std::make_shared<ral::io::gdf_parser>(tableSchema.blazingTableView);
		} else if(fileType == ral::io::DataType::ORC) {
			parser = std::make_shared<ral::io::orc_parser>(tableSchema.args.orcReaderArg);
		} else if(fileType == ral::io::DataType::JSON) {
			parser = std::make_shared<ral::io::json_parser>(tableSchema.args.jsonReaderArg);
		} else if(fileType == ral::io::DataType::CSV) {
			parser = std::make_shared<ral::io::csv_parser>(tableSchema.args.csvReaderArg);
		}else if(fileType == ral::io::DataType::ARROW){
     	parser = std::make_shared<ral::io::arrow_parser>(tableSchema.arrow_table);
		}

		std::shared_ptr<ral::io::data_provider> provider;
		std::vector<Uri> uris;
		for(int fileIndex = 0; fileIndex < filesAll[i].size(); fileIndex++) {
			uris.push_back(Uri{filesAll[i][fileIndex]});
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

		std::unique_ptr<ral::frame::BlazingTable> frame = evaluate_query(input_loaders, schemas, tableNames, query, accessToken, queryContext);

		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();
		result->names = frame->names();
		result->cudfTable = frame->releaseCudfTable();
		return result;
	} catch(const std::exception & e) {
		std::cerr << e.what() << std::endl;
		throw;
	}
}


std::unique_ptr<ResultSet> runSkipData(int32_t masterIndex,
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
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values) {
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;

	std::vector<std::vector<gdf_column*>> minmax_metadata_tables;

	assert(tableSchemas.size() == 1);
	for(int i = 0; i < tableSchemas.size(); i++) {
		auto tableSchema = tableSchemas[i];
		auto files = filesAll[i];
		auto fileType = fileTypes[i];
		std::vector<cudf::type_id> types;

		auto kwargs = ral::io::to_map(tableSchemaCppArgKeys[i], tableSchemaCppArgValues[i]);
		tableSchema.args = ral::io::getReaderArgs((ral::io::DataType) fileType, kwargs);

		//for(int col = 0; col < tableSchemas[i].columns.size(); col++) {
			// TODO percy cudf0.12
			//types.push_back(tableSchemas[i].columns[col]->type().id());
		//}

		std::vector<gdf_column*> minmax_metadata_table;
		for(int col = 0; col < tableSchemas[i].metadata.size(); col++) {
			minmax_metadata_table.push_back(tableSchemas[i].metadata[col]);
		}
		minmax_metadata_tables.push_back(minmax_metadata_table);

		auto schema = ral::io::Schema(tableSchema.names,
			tableSchema.calcite_to_file_indices,
			types,
			tableSchema.num_row_groups,
			tableSchema.in_file);

		std::shared_ptr<ral::io::data_parser> parser;
		if(fileType == ral::io::DataType::PARQUET) {
			parser = std::make_shared<ral::io::parquet_parser>();
		} else if(fileType == gdfFileType || fileType == daskFileType) {
			// TODO percy cudf.102
			//parser = std::make_shared<ral::io::gdf_parser>(tableSchema.columns, tableSchema.names);
		} else if(fileType == ral::io::DataType::ORC) {
			parser = std::make_shared<ral::io::orc_parser>(tableSchema.args.orcReaderArg);
		} else if(fileType == ral::io::DataType::JSON) {
			parser = std::make_shared<ral::io::json_parser>(tableSchema.args.jsonReaderArg);
		} else if(fileType == ral::io::DataType::CSV) {
			parser = std::make_shared<ral::io::csv_parser>(tableSchema.args.csvReaderArg);
		}

		std::shared_ptr<ral::io::data_provider> provider;
		std::vector<Uri> uris;
		for(int fileIndex = 0; fileIndex < filesAll[i].size(); fileIndex++) {
			uris.push_back(Uri{filesAll[i][fileIndex]});
		}

		if(fileType == ral::io::DataType::CUDF || fileType == ral::io::DataType::DASK_CUDF) {
			// is gdf
			provider = std::make_shared<ral::io::dummy_data_provider>();
		} else {
			// is file (this includes the case where fileType is UNDEFINED too)
			// TODO percy cudf0.12 port to cudf::column
			// provider = std::make_shared<ral::io::uri_data_provider>(
			// 	uris, uri_values[i], string_values[i], is_column_string[i]);
		}
		ral::io::data_loader loader(parser, provider);
		input_loaders.push_back(loader);
		schemas.push_back(schema);
	}

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
		// 		skipdata_output_t
		//TODO: fix this @alex, input_loaders[0]
		// auto row_groups_cols = ral::skip_data::process_skipdata_for_table(input_loaders[0], minmax_metadata_tables[0], query, queryContext);

		// std::vector<cudf::column *> columns;
		// std::vector<std::string> names;
		// for(int i = 0; i < row_groups_cols.size(); i++) {
		// 	auto& column = row_groups_cols[i];
		// 	columns.push_back(column.get_gdf_column());
		// 	// TODO percy cudf0.12 port to cudf::column
		// 	// GDFRefCounter::getInstance()->deregister_column(column.get_gdf_column());

		// 	names.push_back(column.name());
		// }

		// TODO percy cudf0.12 port to cudf::column CIO
//		ResultSet result = {columns, names};
		//    std::cout<<"result looks ok"<<std::endl;
		// return result;
		return nullptr;
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
