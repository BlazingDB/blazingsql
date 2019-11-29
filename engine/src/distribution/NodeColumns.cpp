#include "distribution/NodeColumns.h"

namespace ral {
namespace distribution {

NodeColumns::NodeColumns(const Node & node, const std::vector<gdf_column_cpp> & columns)
	: node_{node}, columns_{columns} {}

const Node & NodeColumns::getNode() const { return node_; }

std::vector<gdf_column_cpp> NodeColumns::getColumns() { return columns_; }

}  // namespace distribution
}  // namespace ral
