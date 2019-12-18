#include "distribution/NodeSamples.h"

namespace ral {
namespace distribution {

NodeSamples::NodeSamples(
	std::size_t total_row_size, const Node & sender_node, const std::vector<gdf_column_cpp> & columns)
	: total_row_size_{total_row_size}, sender_node_{sender_node}, columns_{columns} {}

const std::size_t NodeSamples::getTotalRowSize() const { return total_row_size_; }

const Node & NodeSamples::getSenderNode() const { return sender_node_; }

std::vector<gdf_column_cpp> NodeSamples::getColumns() { return columns_; }

void NodeSamples::setColumns(const std::vector<gdf_column_cpp> & columns) { columns_ = columns; }

}  // namespace distribution
}  // namespace ral
