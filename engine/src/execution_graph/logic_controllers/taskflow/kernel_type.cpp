#include "kernel_type.h"

namespace ral {
namespace cache {

std::string get_kernel_type_name(kernel_type type){
    switch (type){
        case kernel_type::ProjectKernel: return "ProjectKernel";
        case kernel_type::FilterKernel: return "FilterKernel";
        case kernel_type::UnionKernel: return "UnionKernel";
        case kernel_type::MergeStreamKernel: return "MergeStreamKernel";
        case kernel_type::PartitionKernel: return "PartitionKernel";
        case kernel_type::SortAndSampleKernel: return "SortAndSampleKernel";
        case kernel_type::PartitionSingleNodeKernel: return "PartitionSingleNodeKernel";
        case kernel_type::LimitKernel: return "LimitKernel";
        case kernel_type::ComputeAggregateKernel: return "ComputeAggregateKernel";
        case kernel_type::DistributeAggregateKernel: return "DistributeAggregateKernel";
        case kernel_type::MergeAggregateKernel: return "MergeAggregateKernel";
        case kernel_type::TableScanKernel: return "TableScanKernel";
        case kernel_type::BindableTableScanKernel: return "BindableTableScanKernel";
        case kernel_type::PartwiseJoinKernel: return "PartwiseJoinKernel";
        case kernel_type::JoinPartitionKernel: return "JoinPartitionKernel";
        case kernel_type::SingleTableHashPartitionKernel: return "SingleTableHashPartitionKernel";        
        case kernel_type::OutputKernel: return "OutputKernel";
        case kernel_type::PrintKernel: return "PrintKernel";
        case kernel_type::GenerateKernel: return "GenerateKernel";
        default: return "UnknownKernel";
    }
}

}  // namespace cache
}  // namespace ral