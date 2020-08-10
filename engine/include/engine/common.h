#pragma once

#include <string>
#include <vector>

struct NodeMetaDataTCP {
	std::string worker_id;
	std::string ip;
	std::int32_t communication_port;
};

struct TableScanInfo {
	std::vector<std::string> relational_algebra_steps;
	std::vector<std::string> table_names;
	std::vector<std::vector<int>> table_columns;
};
