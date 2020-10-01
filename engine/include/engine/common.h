#pragma once

#include <string>
#include <vector>
#include <cstdint>



struct NodeMetaDataUCP {
	std::string worker_id;
	std::uintptr_t ep_handle;
	std::uintptr_t worker_handle;
	std::uintptr_t context_handle;
	std::string ip;
	std::int32_t port;
};

struct TableScanInfo {
	std::vector<std::string> relational_algebra_steps;
	std::vector<std::string> table_names;
	std::vector<std::vector<int>> table_columns;
};
