#pragma once

#include <cudf/types.hpp>
#include "BatchProcessing.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "distribution/primitives.h"
#include "utilities/DebuggingUtils.h"
#include "error.hpp"
#include "CodeTimer.h"

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;
using namespace fmt::literals;

class UnionKernel : public kernel {
public:
	UnionKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{queryString, context, kernel_type::UnionKernel} {
        this->query_graph = query_graph;
        this->input_.add_port("input_a", "input_b");
	}

    bool can_you_throttle_my_input() {
		return true;
	}

	virtual kstatus run() {
		CodeTimer timer;

        bool isUnionAll = (get_named_expression(this->expression, "all") == "true");
        RAL_EXPECTS(isUnionAll, "In UnionKernel: UNION is not supported, use UNION ALL");

        BatchSequenceBypass input_a(this->input_.get_cache("input_a"), this);
        BatchSequenceBypass input_b(this->input_.get_cache("input_b"), this);
        auto batch_a = input_a.next();
        auto batch_b = input_b.next();

        std::vector<cudf::data_type> data_types_a = batch_a->get_schema();
        std::vector<cudf::data_type> data_types_b = batch_b->get_schema();

        RAL_EXPECTS(std::equal(data_types_a.cbegin(), data_types_a.cend(), data_types_b.cbegin(), data_types_b.cend()), "In UnionKernel: Mismatched column types");

        this->add_to_output_cache(std::move(batch_a));
        this->add_to_output_cache(std::move(batch_b));

        while (input_a.wait_for_next()) {
            auto batch = input_a.next();
            this->add_to_output_cache(std::move(batch));
        }

        while (input_b.wait_for_next()) {
            auto batch = input_b.next();
            this->add_to_output_cache(std::move(batch));
        }

		logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="Union Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());

		return kstatus::proceed;
	}

private:

};

} // namespace batch
} // namespace ral
