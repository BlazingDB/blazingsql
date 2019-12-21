#ifndef BLAZINGDB_RAL_DISTRIBUTION_NODESAMPLES_H
#define BLAZINGDB_RAL_DISTRIBUTION_NODESAMPLES_H

#include <GDFColumn.cuh>
#include <blazingdb/transport/Node.h>

namespace ral {
namespace distribution {

namespace {
using Node = blazingdb::transport::Node;
}  // namespace

class NodeSamples {
public:
	NodeSamples(std::size_t total_row_size, const Node & node, const std::vector<std::pair<std::string, cudf::column>> & columns);

public:
	const std::size_t getTotalRowSize() const;

	const Node & getSenderNode() const;

	std::vector<std::pair<std::string, cudf::column>> getColumns();

	void setColumns(const std::vector<std::pair<std::string, cudf::column>> & columns);

private:
	const std::size_t total_row_size_;
	const Node sender_node_;
	std::vector<std::pair<std::string, cudf::column>> columns_;
};

}  // namespace distribution
}  // namespace ral

#endif  // BLAZINGDB_RAL_DISTRIBUTION_NODESAMPLES_H
