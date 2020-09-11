#pragma once

#include <string>

namespace ral {
namespace cache {

enum class kernel_type {
	ProjectKernel,
	FilterKernel,
	UnionKernel,
	MergeStreamKernel,
	PartitionKernel,
	SortAndSampleKernel,
	PartitionSingleNodeKernel,
	LimitKernel,
	ComputeAggregateKernel,
	DistributeAggregateKernel,
	MergeAggregateKernel,
	TableScanKernel,
	BindableTableScanKernel,
	PartwiseJoinKernel,
	JoinPartitionKernel,
	SingleTableHashPartitionKernel,
	OutputKernel,
	PrintKernel,
	GenerateKernel,
};

std::string get_kernel_type_name(kernel_type type);
}  // namespace cache
}  // namespace ral