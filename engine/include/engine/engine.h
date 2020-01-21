#include "../io/io.h"
#include "../src/gdf_wrapper/gdf_wrapper.cuh"
#include <string>
#include <vector>

#include <execution_graph/logic_controllers/LogicPrimitives.h>

struct ResultSet {
	ral::frame::BlazingTableView *blazingTableView;
};

struct SkipDataResultSet {
	std::vector<int> files;
	std::vector<std::vector<int> > row_groups;
};

struct NodeMetaDataTCP {
	std::string ip;
	std::int32_t communication_port;
};

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
	std::vector<std::vector<std::map<std::string, bool>>> is_column_string);


struct TableScanInfo {
	std::vector<std::string> relational_algebra_steps;
	std::vector<std::string> table_names;
	std::vector<std::vector<int>> table_columns;
};

TableScanInfo getTableScanInfo(std::string logicalPlan);

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
	std::vector<std::vector<std::map<std::string, bool>>> is_column_string);

