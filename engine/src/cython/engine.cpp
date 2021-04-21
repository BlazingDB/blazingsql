#include "engine/engine.h"
#include "../CalciteInterpreter.h"
#include "../io/data_parser/ArgsUtil.h"
#include "../io/data_parser/CSVParser.h"
#include "../io/data_parser/GDFParser.h"
#include "../io/data_parser/JSONParser.h"
#include "../io/data_parser/OrcParser.h"
#include "../io/data_parser/ArrowParser.h"
#include "../io/data_parser/ParquetParser.h"
#include "../io/data_provider/GDFDataProvider.h"
#include "../io/data_provider/ArrowDataProvider.h"
#include "../io/data_provider/UriDataProvider.h"
#include "../skip_data/SkipDataProcessor.h"
#include "../execution_graph/logic_controllers/LogicalFilter.h"

#include <numeric>
#include <map>
#include "communication/CommunicationData.h"
#include <spdlog/spdlog.h>
#include "CodeTimer.h"
#include "communication/CommunicationInterface/protocols.hpp"
#include "error.hpp"

#ifdef MYSQL_SUPPORT
#include "../io/data_parser/sql/MySQLParser.h"
#include "../io/data_provider/sql/MySQLDataProvider.h"
#endif

#ifdef SQLITE_SUPPORT
#include "../io/data_parser/sql/SQLiteParser.h"
#include "../io/data_provider/sql/SQLiteDataProvider.h"
#endif

using namespace fmt::literals;

std::pair<std::vector<ral::io::data_loader>, std::vector<ral::io::Schema>> get_loaders_and_schemas(
	const std::vector<TableSchema> & tableSchemas,
	const std::vector<std::vector<std::string>> & tableSchemaCppArgKeys,
	const std::vector<std::vector<std::string>> & tableSchemaCppArgValues,
	const std::vector<std::vector<std::string>> & filesAll,
	const std::vector<int> & fileTypes,
	const std::vector<std::vector<std::map<std::string, std::string>>> & uri_values,
  size_t total_number_of_nodes,
  size_t self_node_idx){

	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;

	for(size_t i = 0; i < tableSchemas.size(); i++) {
		auto tableSchema = tableSchemas[i];
		auto files = filesAll[i];
		auto fileType = fileTypes[i];

		auto args_map = ral::io::to_map(tableSchemaCppArgKeys[i], tableSchemaCppArgValues[i]);

		std::vector<cudf::type_id> types;
		for(size_t col = 0; col < tableSchemas[i].types.size(); col++) {
			types.push_back(tableSchemas[i].types[col]);
		}

		auto schema = ral::io::Schema(tableSchema.names,
			tableSchema.calcite_to_file_indices,
			types,
			tableSchema.in_file,
			tableSchema.row_groups_ids);

    bool isSqlProvider = false;
    std::shared_ptr<ral::io::data_provider> provider;

		std::shared_ptr<ral::io::data_parser> parser;
		if(fileType == ral::io::DataType::PARQUET) {
			parser = std::make_shared<ral::io::parquet_parser>();
		} else if(fileType == gdfFileType || fileType == daskFileType) {
			parser = std::make_shared<ral::io::gdf_parser>();
		} else if(fileType == ral::io::DataType::ORC) {
			parser = std::make_shared<ral::io::orc_parser>(args_map);
		} else if(fileType == ral::io::DataType::JSON) {
			parser = std::make_shared<ral::io::json_parser>(args_map);
		} else if(fileType == ral::io::DataType::CSV) {
			parser = std::make_shared<ral::io::csv_parser>(args_map);
		} else if(fileType == ral::io::DataType::ARROW){
			parser = std::make_shared<ral::io::arrow_parser>();
		} else if(fileType == ral::io::DataType::MYSQL) {
#ifdef MYSQL_SUPPORT
      parser = std::make_shared<ral::io::mysql_parser>();
      auto sql = ral::io::getSqlInfo(args_map);
      provider = std::make_shared<ral::io::mysql_data_provider>(sql, total_number_of_nodes, self_node_idx);
      isSqlProvider = true;
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support MySQL integration");
#endif
    } else if(fileType == ral::io::DataType::SQLITE) {
#ifdef SQLITE_SUPPORT
  		parser = std::make_shared<ral::io::sqlite_parser>();
      auto sql = ral::io::getSqlInfo(args_map);
      provider = std::make_shared<ral::io::sqlite_data_provider>(sql, total_number_of_nodes, self_node_idx);
      isSqlProvider = true;
#endif
    }
		std::vector<Uri> uris;
		for(size_t fileIndex = 0; fileIndex < filesAll[i].size(); fileIndex++) {
			uris.push_back(Uri{filesAll[i][fileIndex]});
			schema.add_file(filesAll[i][fileIndex]);
		}

		if (!isSqlProvider) {
			if(fileType == ral::io::DataType::CUDF || fileType == ral::io::DataType::DASK_CUDF) {
				// is gdf
				provider = std::make_shared<ral::io::gdf_data_provider>(tableSchema.blazingTableViews, uri_values[i]);
			} else if (fileType == ral::io::DataType::ARROW) {
				std::vector<std::shared_ptr<arrow::Table>> arrow_tables = {tableSchema.arrow_table};
				provider = std::make_shared<ral::io::arrow_data_provider>(arrow_tables, uri_values[i]);
			} else {
				// is file (this includes the case where fileType is UNDEFINED too)
				provider = std::make_shared<ral::io::uri_data_provider>(uris, uri_values[i]);
			}
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

std::string runGeneratePhysicalGraph(uint32_t masterIndex,
                                     std::vector<std::string> worker_ids,
                                     int32_t ctxToken,
                                     std::string query) {
    using blazingdb::manager::Context;
    using blazingdb::transport::Node;

    std::vector<Node> contextNodes;
    for (const auto &worker_id : worker_ids) {
        contextNodes.emplace_back(worker_id);
    }
    Context queryContext{static_cast<uint32_t>(ctxToken), contextNodes, contextNodes[masterIndex], "", {}};

    return get_physical_plan(query, queryContext);
}

std::shared_ptr<ral::cache::graph> runGenerateGraph(uint32_t masterIndex,
	std::vector<std::string> worker_ids,
	std::vector<std::string> tableNames,
	std::vector<std::string> tableScans,
	std::vector<TableSchema> tableSchemas,
	std::vector<std::vector<std::string>> tableSchemaCppArgKeys,
	std::vector<std::vector<std::string>> tableSchemaCppArgValues,
	std::vector<std::vector<std::string>> filesAll,
	std::vector<int> fileTypes,
	int32_t ctxToken,
	std::string query,
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values,
	std::map<std::string, std::string> config_options,
	std::string sql)
{
  using blazingdb::manager::Context;
  using blazingdb::transport::Node;
  
  auto& communicationData = ral::communication::CommunicationData::getInstance();

  std::vector<Node> contextNodes;
  for (const auto &worker_id : worker_ids) {
    contextNodes.emplace_back(worker_id);
  }
	Context queryContext{static_cast<uint32_t>(ctxToken), contextNodes, contextNodes[masterIndex], "", config_options};
  auto& self_node = ral::communication::CommunicationData::getInstance().getSelfNode();
  int self_node_idx = queryContext.getNodeIndex(self_node);
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::tie(input_loaders, schemas) = get_loaders_and_schemas(tableSchemas, tableSchemaCppArgKeys,
		tableSchemaCppArgValues, filesAll, fileTypes, uri_values, contextNodes.size(), self_node_idx);



  	auto graph = generate_graph(input_loaders, schemas, tableNames, tableScans, query, queryContext, sql);

	comm::graphs_info::getInstance().register_graph(ctxToken, graph);
	return graph;
}

void startExecuteGraph(std::shared_ptr<ral::cache::graph> graph, int32_t ctx_token) {
	start_execute_graph(graph);
}

std::unique_ptr<PartitionedResultSet> getExecuteGraphResult(std::shared_ptr<ral::cache::graph> graph, int32_t ctx_token) {
	// Execute query

	std::vector<std::unique_ptr<ral::frame::BlazingTable>> frames;
	frames = get_execute_graph_results(graph);

	std::unique_ptr<PartitionedResultSet> result = std::make_unique<PartitionedResultSet>();

	assert( frames.size()>0 );

	result->names = frames[0]->names();

	fix_column_names_duplicated(result->names);

	for(auto& cudfTable : frames){
		result->cudfTables.emplace_back(std::move(cudfTable->releaseCudfTable()));
	}

	result->skipdata_analysis_fail = false;

	comm::graphs_info::getInstance().deregister_graph(ctx_token);
	return result;
}
/*
std::unique_ptr<ResultSet> performPartition(int32_t masterIndex,

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
		if(logger){
            logger->error("|||{info}|||||",
                                        "info"_a="In performPartition. What: {}"_format(e.what()));
            logger->flush();
		}

		std::cerr << "**[performPartition]** error partitioning table.\n";
		std::cerr << e.what() << std::endl;
		throw;
	}
}
*/


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
		std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
		if(logger){
            logger->error("|||{info}|||||",
                                        "info"_a="In runSkipData. What: {}"_format(e.what()));
            logger->flush();
		}


		throw;
	}
}


TableScanInfo getTableScanInfo(std::string logicalPlan){

	std::vector<std::string> relational_algebra_steps, table_names;
	std::vector<std::vector<int>> table_columns;
	getTableScanInfo(logicalPlan, relational_algebra_steps, table_names, table_columns);
	return TableScanInfo{relational_algebra_steps, table_names, table_columns};
}

/*
std::pair<std::unique_ptr<PartitionedResultSet>, error_code_t> runQuery_C(int32_t masterIndex,

	std::vector<std::string> tableNames,
	std::vector<std::string> tableScans,
	std::vector<TableSchema> tableSchemas,
	std::vector<std::vector<std::string>> tableSchemaCppArgKeys,
	std::vector<std::vector<std::string>> tableSchemaCppArgValues,
	std::vector<std::vector<std::string>> filesAll,
	std::vector<int> fileTypes,
	int32_t ctxToken,
	std::string query,
	std::vector<std::vector<std::map<std::string, std::string>>> uri_values,
	std::map<std::string, std::string> config_options) {


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

	int32_t ctxToken,
	const ral::frame::BlazingTableView & table,
	std::vector<std::string> column_names) {

	std::unique_ptr<ResultSet> result = nullptr;

	try {
		result = std::move(performPartition(masterIndex,
					ctxToken,
					table,
					column_names));
		return std::make_pair(std::move(result), E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(std::move(result), E_EXCEPTION);
	}
}*/
