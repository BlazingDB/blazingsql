#pragma once

#include "BatchProcessing.h"
#include "LogicPrimitives.h"



namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;
using namespace fmt::literals;

class UnionKernel : public kernel {
public:
    UnionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);

    std::string kernel_name() { return "Union";}

    ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
        std::shared_ptr<ral::cache::CacheMachine> output,
        cudaStream_t stream, std::string kernel_process_name) override;

    virtual kstatus run();

private:
    std::vector<std::string> common_names;
    std::vector<cudf::data_type> common_types;
};

} // namespace batch
} // namespace ral
