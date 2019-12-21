#include "distribution/NodeSamples.h"

namespace ral {
namespace distribution {

NodeSamples::NodeSamples(
	std::size_t total_row_size, const Node & sender_node, const std::vector<std::pair<std::string, cudf::column>> & columns)
	: total_row_size_{total_row_size}, sender_node_{sender_node}, columns_{columns} {}

const std::size_t NodeSamples::getTotalRowSize() const { return total_row_size_; }

const Node & NodeSamples::getSenderNode() const { return sender_node_; }

std::vector<std::pair<std::string, cudf::column>> NodeSamples::getColumns() { return columns_; }

void NodeSamples::setColumns(const std::vector<std::pair<std::string, cudf::column>> & columns) { columns_ = columns; }

}  // namespace distribution
}  // namespace ral
