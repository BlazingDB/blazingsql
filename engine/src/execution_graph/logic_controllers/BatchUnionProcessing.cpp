#include "BatchUnionProcessing.h"
#include <cudf/types.hpp>
#include "CodeTimer.h"
#include "parser/expression_utils.hpp"
#include <src/utilities/CommonOperations.h>
#include "taskflow/executor.h"

namespace ral {
namespace batch {

UnionKernel::UnionKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::UnionKernel} {
    this->query_graph = query_graph;
    this->input_.add_port("input_a", "input_b");
}

void UnionKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable>> inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t stream, const std::map<std::string, std::string>& args) {

    auto & input = inputs[0];
    input->setNames(common_names);
    ral::utilities::normalize_types(input, common_types);
    this->add_to_output_cache(std::move(input));
}

kstatus UnionKernel::run() {
    CodeTimer timer;

    bool isUnionAll = (get_named_expression(this->expression, "all") == "true");
    RAL_EXPECTS(isUnionAll, "In UnionKernel: UNION is not supported, use UNION ALL");

    auto cache_machine_a = this->input_.get_cache("input_a");
    auto cache_machine_b = this->input_.get_cache("input_b");
    std::unique_ptr<ral::cache::CacheData> cache_data_a = cache_machine_a->pullCacheData();
    std::unique_ptr<ral::cache::CacheData> cache_data_b = cache_machine_b->pullCacheData();

    common_names = cache_data_a->names();

    bool strict = false;
    common_types = ral::utilities::get_common_types(cache_data_a->get_schema(), cache_data_b->get_schema(), strict);

    BlazingThread left_thread([this, &cache_machine_a, &cache_data_a](){
        while(cache_data_a != nullptr) {
            std::vector<cudf::data_type> data_types = cache_data_a->get_schema();
            std::vector<std::string> names = cache_data_a->names();
            if (!std::equal(common_types.cbegin(), common_types.cend(), data_types.cbegin(), data_types.cend())
                || !std::equal(common_names.cbegin(), common_names.cend(), names.cbegin(), names.cend())){
                std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
                inputs.push_back(std::move(cache_data_a));

                ral::execution::executor::get_instance()->add_task(
                        std::move(inputs),
                        this->output_cache(),
                        this);
            } else {
                this->add_to_output_cache(std::move(cache_data_a));
            }
            cache_data_a = cache_machine_a->pullCacheData();
        }
    });

    BlazingThread right_thread([this, &cache_machine_b, &cache_data_b](){
        while(cache_data_b != nullptr){
            std::vector<cudf::data_type> data_types = cache_data_b->get_schema();
            std::vector<std::string> names = cache_data_b->names();
            if (!std::equal(common_types.cbegin(), common_types.cend(), data_types.cbegin(), data_types.cend())
                || !std::equal(common_names.cbegin(), common_names.cend(), names.cbegin(), names.cend())){
                std::vector<std::unique_ptr<ral::cache::CacheData>> inputs;
                inputs.push_back(std::move(cache_data_b));

                ral::execution::executor::get_instance()->add_task(
                        std::move(inputs),
                        this->output_cache(),
                        this);
            } else {
                this->add_to_output_cache(std::move(cache_data_b));
            }
            cache_data_b = cache_machine_b->pullCacheData();
        }
    });

    left_thread.join();
    right_thread.join();

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

} // namespace batch
} // namespace ral
