#include "../../include/engine/engine.h"
#include "../CalciteInterpreter.h"
#include "../Types.h"
#include "../io/DataLoader.h"
#include "../io/Schema.h"
#include "../io/data_parser/ArgsUtil.h"
#include "../io/data_parser/CSVParser.h"
#include "../io/data_parser/GDFParser.h"
#include "../io/data_parser/JSONParser.h"
#include "../io/data_parser/OrcParser.h"
#include "../io/data_parser/ArrowParser.h"
#include "../io/data_parser/ParquetParser.h"
#include "../io/data_parser/ParserUtil.h"
#include "../io/data_provider/DummyProvider.h"
#include "../io/data_provider/UriDataProvider.h"
#include "../skip_data/SkipDataProcessor.h"
#include "communication/network/Server.h"
#include <numeric>

void make_sure_output_is_not_input_gdf(
	blazing_frame & output_frame, std::vector<TableSchema> & tableSchemas, const std::vector<int> & fileTypes) {
	std::vector<void *> input_data_ptrs;
	for(int i = 0; i < tableSchemas.size(); i++) {
		if(fileTypes[i] == gdfFileType || fileTypes[i] == daskFileType) {
			for(int j = 0; j < tableSchemas[i].columns.size(); j++) {
				if(tableSchemas[i].columns[j]->data != nullptr) {
					input_data_ptrs.push_back(tableSchemas[i].columns[j]->data);
				}
			}
		}
	}

	if(input_data_ptrs.size() > 0) {
		for(size_t index = 0; index < output_frame.get_width(); index++) {
			// if we find that in the output the data pointer is the same as an input data pointer, we need to clone it.
			// this can happen when you do something like select * from gdf_table
			// TODO percy cudf0.12 port to cudf::column
//			if(std::find(input_data_ptrs.begin(), input_data_ptrs.end(), output_frame.get_column(index).data()) !=
//				input_data_ptrs.end()) {
//				bool register_column = false;
//				output_frame.set_column(index, output_frame.get_column(index).clone(output_frame.get_column(index).name(), register_column));
//			}
		}
	}
}


ResultSet runQuery(int32_t masterIndex,
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
	std::vector<std::vector<std::map<std::string, gdf_scalar>>> uri_values,
	std::vector<std::vector<std::map<std::string, std::string>>> string_values,
	std::vector<std::vector<std::map<std::string, bool>>> is_column_string) {

	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;

	for(int i = 0; i < tableSchemas.size(); i++) {

		auto tableSchema = tableSchemas[i];
		auto files = filesAll[i];
		auto fileType = fileTypes[i];
		std::vector<cudf::type_id> types;

		auto kwargs = ral::io::to_map(tableSchemaCppArgKeys[i], tableSchemaCppArgValues[i]);
		tableSchema.args = ral::io::getReaderArgs((ral::io::DataType) fileType, kwargs);

		for(int col = 0; col < tableSchemas[i].columns.size(); col++) {
			types.push_back(to_type_id(tableSchemas[i].columns[col]->dtype));
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
				parser = std::make_shared<ral::io::gdf_parser>(tableSchema);
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

			// TODO percy cudf0.12 implement proper scalar support
			provider = std::make_shared<ral::io::uri_data_provider>(
				uris, /* uri_values[i]*/ std::vector<std::map<std::string, cudf::scalar*>>(), string_values[i], is_column_string[i]);
		}
		ral::io::data_loader loader(parser, provider);
		input_loaders.push_back(loader);
		schemas.push_back(schema);
	}

	try {
		using blazingdb::manager::Context;
		using blazingdb::transport::Node;

		std::vector<std::shared_ptr<Node>> contextNodes;
		for(auto currentMetadata : tcpMetadata) {
			auto address =
				blazingdb::transport::Address::TCP(currentMetadata.ip, currentMetadata.communication_port, 0);
			contextNodes.push_back(Node::Make(address));
		}

		Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], ""};
		ral::communication::network::Server::getInstance().registerContext(ctxToken);

		// Execute query

		blazing_frame frame = evaluate_query(input_loaders, schemas, tableNames, query, accessToken, queryContext);
		make_sure_output_is_not_input_gdf(frame, tableSchemas, fileTypes);
		std::vector<cudf::column *> columns;
		std::vector<std::string> names;
		for(int i = 0; i < frame.get_width(); i++) {
			auto& column = frame.get_column(i);
			columns.push_back(column.get_gdf_column());
			names.push_back(column.name());
		}

		std::vector<cudf::column_view> columnViews;
		columnViews.reserve(columns.size());
		//std::transform(
			//columns.cbegin(), columns.cend(), columnViews.begin(), [](cudf::column * column) { return column->view(); });

		// TODO(cristhian): Uncomment this to use a dummy data sending to cython
		//names = {"col001", "col002", "col003"};
		//std::vector<cudf::column> cvss;


		//std::vector<std::int32_t> arr{1, 2, 3, 4, 5, 6, 7, 8, 9};
		//std::vector<std::int32_t> arr2{11, 12, 13, 14, 15, 16, 17, 18, 19};
		//std::vector<std::int32_t> arr3{31, 32, 33, 34, 35, 36, 37, 38, 39};
		//rmm::device_buffer sdata(arr.data(), 9 * sizeof(std::int32_t));
		//rmm::device_buffer sdata2(arr2.data(), 9 * sizeof(std::int32_t));
		//rmm::device_buffer sdata3(arr3.data(), 9 * sizeof(std::int32_t));
		//cvss.emplace_back(cudf::data_type{cudf::type_id::INT32}, 9, sdata);
		//cvss.emplace_back(cudf::data_type{cudf::type_id::INT32}, 9, sdata2);
		//cvss.emplace_back(cudf::data_type{cudf::type_id::INT32}, 9, sdata3);

		//std::transform(
			//cvss.cbegin(), cvss.cend(), std::back_inserter(columnViews), [](const cudf::column & column) { return column.view(); });

		// TODO(gcca): Ask to William about this. Use shared_ptr
		// or implement default constructor to have an empty BlazingTableView
		// beacuse cythons needs initialize a ResultSet by default. After that,
		// remove new statement.
		ResultSet result{{}, {}, new ral::frame::BlazingTableView{CudfTableView{columnViews}, names}};
		return result;
	} catch(const std::exception & e) {
		std::cerr << e.what() << std::endl;
		throw;
	}
}


ResultSet runSkipData(int32_t masterIndex,
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
	std::vector<std::vector<std::map<std::string, gdf_scalar>>> uri_values,
	std::vector<std::vector<std::map<std::string, std::string>>> string_values,
	std::vector<std::vector<std::map<std::string, bool>>> is_column_string) {
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

		for(int col = 0; col < tableSchemas[i].columns.size(); col++) {
			types.push_back(to_type_id(tableSchemas[i].columns[col]->dtype));
		}

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
			parser = std::make_shared<ral::io::gdf_parser>(tableSchema);
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
		using blazingdb::manager::Context;
		using blazingdb::transport::Node;

		std::vector<std::shared_ptr<Node>> contextNodes;
		for(auto currentMetadata : tcpMetadata) {
			auto address =
				blazingdb::transport::Address::TCP(currentMetadata.ip, currentMetadata.communication_port, 0);
			contextNodes.push_back(Node::Make(address));
		}

		Context queryContext{ctxToken, contextNodes, contextNodes[masterIndex], ""};
		ral::communication::network::Server::getInstance().registerContext(ctxToken);

		// Execute query
		// 		skipdata_output_t
		//TODO: fix this @alex, input_loaders[0]
		auto row_groups_cols = ral::skip_data::process_skipdata_for_table(input_loaders[0], minmax_metadata_tables[0], query, queryContext);

		std::vector<cudf::column *> columns;
		std::vector<std::string> names;
		for(int i = 0; i < row_groups_cols.size(); i++) {
			auto& column = row_groups_cols[i];
			columns.push_back(column.get_gdf_column());
			// TODO percy cudf0.12 port to cudf::column
			// GDFRefCounter::getInstance()->deregister_column(column.get_gdf_column());

			names.push_back(column.name());
		}

		// TODO percy cudf0.12 port to cudf::column CIO
//		ResultSet result = {columns, names};
		//    std::cout<<"result looks ok"<<std::endl;
		// return result;
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
