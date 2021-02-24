#pragma once

#include "io/DataLoader.h"
#include <string>
#include <vector>
#include <execution_graph/Context.h>
#include "execution_graph/logic_controllers/taskflow/graph.h"

using blazingdb::manager::Context;

std::shared_ptr<ral::cache::graph> generate_graph(std::vector<ral::io::data_loader> input_loaders,
																									std::vector<ral::io::Schema> schemas,
																									std::vector<std::string> table_names,
																									std::vector<std::string> table_scans,
																									std::string logicalPlan,
																									Context & queryContext,
                                                                                                    const std::string &sql);

void start_execute_graph(std::shared_ptr<ral::cache::graph> graph);
std::vector<std::unique_ptr<ral::frame::BlazingTable>> get_execute_graph_results(std::shared_ptr<ral::cache::graph> graph);

void getTableScanInfo(std::string & logicalPlan_in,
						std::vector<std::string> & relational_algebra_steps_out,
						std::vector<std::string> & table_names_out,
						std::vector<std::vector<int>> & table_columns_out);
