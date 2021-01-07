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
	SortKernel,
	SplitByKeysKernel,
	ConcatPartitionsByKeysKernel,
	ComputeWindowKernel,
	PartitionSingleNodeKernel,
	LimitKernel,
	ComputeAggregateKernel,
	DistributeAggregateKernel,
	MergeAggregateKernel,
	TableScanKernel,
	BindableTableScanKernel,
	PartwiseJoinKernel,
	JoinPartitionKernel,
	OutputKernel,
	PrintKernel,
	GenerateKernel,
};

std::string get_kernel_type_name(kernel_type type);
}  // namespace cache
}  // namespace ral