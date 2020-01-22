#ifndef BLAZINGDB_RAL_DISTRIBUTION_NODESAMPLES_H
#define BLAZINGDB_RAL_DISTRIBUTION_NODESAMPLES_H

#include <GDFColumn.cuh>
#include <blazingdb/transport/Node.h>

namespace ral {
namespace distribution {

namespace {
using Node = blazingdb::transport::experimental::Node;
}  // namespace

class NodeSamples {
public:
	NodeSamples(std::size_t total_row_size, const Node & node, const std::vector<gdf_column_cpp> & columns);

public:
	const std::size_t getTotalRowSize() const;

	const Node & getSenderNode() const;

	std::vector<gdf_column_cpp> getColumns();

	void setColumns(const std::vector<gdf_column_cpp> & columns);

private:
	const std::size_t total_row_size_;
	const Node sender_node_;
	std::vector<gdf_column_cpp> columns_;
};

}  // namespace distribution
}  // namespace ral

#endif  // BLAZINGDB_RAL_DISTRIBUTION_NODESAMPLES_H
