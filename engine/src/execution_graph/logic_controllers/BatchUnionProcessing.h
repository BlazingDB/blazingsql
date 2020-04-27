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
#include "Utils.cuh"

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using RecordBatch = std::unique_ptr<ral::frame::BlazingTable>;


class UnionKernel :public kernel {
public:
	UnionKernel(const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: expression{queryString}, context{context} {
        this->query_graph = query_graph;
        this->input_.add_port("input_a", "input_b");
	}

	virtual kstatus run() {
        
        bool isUnionAll = (get_named_expression(this->expression, "all") == "true");
        RAL_EXPECTS(isUnionAll, "In UnionKernel: UNION is not supported, use UNION ALL");

        BatchSequenceBypass input_a(this->input_.get_cache("input_a"));
        BatchSequenceBypass input_b(this->input_.get_cache("input_b"));
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

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

} // namespace batch
} // namespace ral
