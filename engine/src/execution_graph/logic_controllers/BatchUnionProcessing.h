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
    virtual kstatus run();
};

} // namespace batch
} // namespace ral
