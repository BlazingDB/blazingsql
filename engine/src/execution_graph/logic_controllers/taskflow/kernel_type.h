#pragma once

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
	SortAndSampleSingleNodeKernel,
	LimitKernel,
	ComputeAggregateKernel,
	DistributeAggregateKernel,
	MergeAggregateKernel,
	TableScanKernel,
	BindableTableScanKernel,
	PartwiseJoinKernel,
	JoinPartitionKernel
};


}  // namespace cache
}  // namespace ral