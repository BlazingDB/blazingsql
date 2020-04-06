#pragma once

#include "../io/io.h"
#include <string>
#include <vector>

#include <execution_graph/logic_controllers/LogicPrimitives.h>

struct SkipDataResultSet {
	std::vector<int> files;
	std::vector<std::vector<int>> row_groups;
};

struct NodeMetaDataTCP {
	std::string ip;
	std::int32_t communication_port;
};

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
	bool use_execution_graph);


struct TableScanInfo {
	std::vector<std::string> relational_algebra_steps;
	std::vector<std::string> table_names;
	std::vector<std::vector<int>> table_columns;
};

TableScanInfo getTableScanInfo(std::string logicalPlan);

std::unique_ptr<ResultSet> runSkipData(
	ral::frame::BlazingTableView metadata, 
	std::vector<std::string> all_column_names, 
	std::string query);

std::unique_ptr<ResultSet> performPartition(
	int32_t masterIndex,
	std::vector<NodeMetaDataTCP> tcpMetadata,
	int32_t ctxToken,
	const ral::frame::BlazingTableView & table,
	std::vector<std::string> column_names);

