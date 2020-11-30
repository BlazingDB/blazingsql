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
	UnionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
		: kernel{kernel_id, queryString, context, kernel_type::UnionKernel} {
        this->query_graph = query_graph;
        this->input_.add_port("input_a", "input_b");
	}

    void do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
        std::shared_ptr<ral::cache::CacheMachine> output,
        cudaStream_t stream, std::string kernel_process_name) override {

        // TODO: bring back the bypass optimization to avoid decaching to GPU memory

        auto & input = inputs[0];
        ral::utilities::normalize_types(input, common_types);
        this->add_to_output_cache(std::move(input));
    }

    virtual kstatus run() {
		CodeTimer timer;

        bool isUnionAll = (get_named_expression(this->expression, "all") == "true");
        RAL_EXPECTS(isUnionAll, "In UnionKernel: UNION is not supported, use UNION ALL");

        auto cache_machine_a = this->input_.get_cache("input_a");
        auto cache_machine_b = this->input_.get_cache("input_b");
        std::unique_ptr<ral::cache::CacheData> cache_data_a = cache_machine_a->pullCacheData();
        std::unique_ptr<ral::cache::CacheData> cache_data_b = cache_machine_b->pullCacheData();

        bool strict = false;
        common_types = ral::utilities::get_common_types(cache_data_a->get_schema(), cache_data_b->get_schema(), strict);

        while(cache_data_a != nullptr ){
            std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
            inputs.push_back(std::move(cache_data_a));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this,
                    "union");

            cache_data_a = cache_machine_a->pullCacheData();
        }
        while(cache_data_b != nullptr ){
            std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
            inputs.push_back(std::move(cache_data_b));

            ral::execution::executor::get_instance()->add_task(
                    std::move(inputs),
                    this->output_cache(),
                    this,
                    "union");

            cache_data_b = cache_machine_b->pullCacheData();
        }

        if(logger != nullptr) {
            logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                                    "query_id"_a=context->getContextToken(),
                                    "step"_a=context->getQueryStep(),
                                    "substep"_a=context->getQuerySubstep(),
                                    "info"_a="Union Kernel tasks created",
                                    "duration"_a=timer.elapsed_time(),
                                    "kernel_id"_a=this->get_id());
        }

        std::unique_lock<std::mutex> lock(kernel_mutex);
        kernel_cv.wait(lock, [this]{
            return this->tasks.empty();
        });

        if(logger != nullptr) {
		    logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                        "query_id"_a=context->getContextToken(),
                        "step"_a=context->getQueryStep(),
                        "substep"_a=context->getQuerySubstep(),
                        "info"_a="Union Kernel Completed",
                        "duration"_a=timer.elapsed_time(),
                        "kernel_id"_a=this->get_id());
        }

		return kstatus::proceed;
	}

private:
    std::vector<cudf::data_type> common_types;
};

} // namespace batch
} // namespace ral
