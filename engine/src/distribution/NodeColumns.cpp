#include "distribution/NodeColumns.h"

namespace ral {
namespace distribution {

NodeColumns::NodeColumns(const Node & node, const std::vector<std::pair<std::string, cudf::column>> & columns)
	: node_{node}, columns_{columns} {}

const Node & NodeColumns::getNode() const { return node_; }

std::vector<std::pair<std::string, cudf::column>> NodeColumns::getColumns() { return columns_; }

}  // namespace distribution
}  // namespace ral
