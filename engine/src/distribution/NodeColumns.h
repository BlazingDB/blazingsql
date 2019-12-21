#ifndef BLAZINGDB_RAL_DISTRIBUTION_NODECOLUMNS_H
#define BLAZINGDB_RAL_DISTRIBUTION_NODECOLUMNS_H

#include "GDFColumn.cuh"
#include <blazingdb/transport/Node.h>
#include <vector>

namespace ral {
namespace distribution {

namespace {
using Node = blazingdb::transport::Node;
}  // namespace

class NodeColumns {
public:
	NodeColumns(const Node & node, const std::vector<std::pair<std::string, cudf::column>> & columns);

public:
	const Node & getNode() const;

	std::vector<std::pair<std::string, cudf::column>> getColumns();

private:
	const Node node_;
	std::vector<std::pair<std::string, cudf::column>> columns_;
};

}  // namespace distribution
}  // namespace ral

#endif  // BLAZINGDB_RAL_DISTRIBUTION_NODECOLUMNS_H
