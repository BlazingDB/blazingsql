#pragma once

#include "io/DataLoader.h"
#include <iostream>
#include <string>
#include <vector>
#include <blazingdb/manager/Context.h>
#include "execution_graph/logic_controllers/taskflow/graph.h"

using blazingdb::manager::Context;

std::shared_ptr<ral::cache::graph> generate_graph(std::vector<ral::io::data_loader> input_loaders,
																									std::vector<ral::io::Schema> schemas,
																									std::vector<std::string> table_names,
																									std::vector<std::string> table_scans,
																									std::string logicalPlan,
																									int64_t connection,
																									Context & queryContext);

std::vector<std::unique_ptr<ral::frame::BlazingTable>> execute_graph(std::shared_ptr<ral::cache::graph> graph);

void getTableScanInfo(std::string & logicalPlan_in,
						std::vector<std::string> & relational_algebra_steps_out,
						std::vector<std::string> & table_names_out,
						std::vector<std::vector<int>> & table_columns_out);
