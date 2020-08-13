#pragma once

#include "kernel_type.h"
#include "port.h"
#include "graph.h"
#include "communication/CommunicationData.h"
#include "CodeTimer.h"

namespace ral {
namespace cache {
class kernel;
class graph;
using kernel_pair = std::pair<kernel *, std::string>;

/**
	@brief This interface represents a computation unit in the execution graph.
	Each kernel has basically and input and output ports and the expression asocciated to the computation unit.
	Each class that implements this interface should define how the computation is executed. See `run()` method.
*/
class kernel {
public:
	kernel(std::size_t kernel_id, std::string expr, std::shared_ptr<Context> context, kernel_type kernel_type_id) : expression{expr}, kernel_id(kernel_id), context{context}, kernel_type_id{kernel_type_id} {

		parent_id_ = -1;
		has_limit_ = false;
		limit_rows_ = -1;

		logger = spdlog::get("batch_logger");
		events_logger = spdlog::get("events_logger");
		cache_events_logger = spdlog::get("cache_events_logger");

		std::shared_ptr<spdlog::logger> kernels_logger;
		kernels_logger = spdlog::get("kernels_logger");

		kernels_logger->info("{ral_id}|{query_id}|{kernel_id}|{is_kernel}|{kernel_type}",
								"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
								"query_id"_a=(this->context ? std::to_string(this->context->getContextToken()) : "null"),
								"kernel_id"_a=this->get_id(),
								"is_kernel"_a=1, //true
								"kernel_type"_a=get_kernel_type_name(this->get_type_id()));
	}
	void set_parent(size_t id) { parent_id_ = id; }
	bool has_parent() const { return parent_id_ != -1; }

	virtual ~kernel() = default;

	virtual kstatus run() = 0;

	std::int32_t get_id() const { return (kernel_id); }

	kernel_type get_type_id() const { return kernel_type_id; }

	void set_type_id(kernel_type kernel_type_id_) { kernel_type_id = kernel_type_id_; }

	virtual bool can_you_throttle_my_input() = 0;

	std::shared_ptr<ral::cache::CacheMachine>  input_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->input_.get_cache(kernel_id);
	}

	std::shared_ptr<ral::cache::CacheMachine>  output_cache(std::string cache_id = "") {
		cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
		return this->output_.get_cache(cache_id);
	}

	void add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string cache_id = "") {
		CodeTimer cacheEventTimer(false);

		auto num_rows = table->num_rows();
		auto num_bytes = table->sizeInBytes();

		cacheEventTimer.start();

		std::string message_id = get_message_id();
		message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
		cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
		this->output_.get_cache(cache_id)->addToCache(std::move(table), message_id);

		cacheEventTimer.stop();

		cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
						"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
						"query_id"_a=context->getContextToken(),
						"source"_a=this->get_id(),
						"sink"_a=this->output_.get_cache(cache_id)->get_id(),
						"num_rows"_a=num_rows,
						"num_bytes"_a=num_bytes,
						"event_type"_a="addCache",
						"timestamp_begin"_a=cacheEventTimer.start_time(),
						"timestamp_end"_a=cacheEventTimer.end_time());
	}

	void add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string cache_id = "") {
		CodeTimer cacheEventTimer(false);

		auto num_rows = cache_data->num_rows();
		auto num_bytes = cache_data->sizeInBytes();

		cacheEventTimer.start();

		std::string message_id = get_message_id();
		message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
		cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
		this->output_.get_cache(cache_id)->addCacheData(std::move(cache_data), message_id);

		cacheEventTimer.stop();

		cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
						"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
						"query_id"_a=context->getContextToken(),
						"source"_a=this->get_id(),
						"sink"_a=this->output_.get_cache(cache_id)->get_id(),
						"num_rows"_a=num_rows,
						"num_bytes"_a=num_bytes,
						"event_type"_a="addCache",
						"timestamp_begin"_a=cacheEventTimer.start_time(),
						"timestamp_end"_a=cacheEventTimer.end_time());
	}

	void add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string cache_id = "") {
		CodeTimer cacheEventTimer(false);

		auto num_rows = host_table->num_rows();
		auto num_bytes = host_table->sizeInBytes();

		cacheEventTimer.start();

		std::string message_id = get_message_id();
		message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
		cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
		this->output_.get_cache(cache_id)->addHostFrameToCache(std::move(host_table), message_id);

		cacheEventTimer.stop();

		cache_events_logger->info("{ral_id}|{query_id}|{source}|{sink}|{num_rows}|{num_bytes}|{event_type}|{timestamp_begin}|{timestamp_end}",
						"ral_id"_a=context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode()),
						"query_id"_a=context->getContextToken(),
						"source"_a=this->get_id(),
						"sink"_a=this->output_.get_cache(cache_id)->get_id(),
						"num_rows"_a=num_rows,
						"num_bytes"_a=num_bytes,
						"event_type"_a="addCache",
						"timestamp_begin"_a=cacheEventTimer.start_time(),
						"timestamp_end"_a=cacheEventTimer.end_time());
	}

	Context * get_context() const {
		return context.get();
	}

	std::string get_message_id(){
		return std::to_string((int)this->get_type_id()) + "_" + std::to_string(this->get_id());
	}

	// returns true if all the caches of an input are finished
	bool input_all_finished() {
		return this->input_.all_finished();
	}

	// returns sum of all the rows added to all caches of the input port
	uint64_t total_input_rows_added() {
		return this->input_.total_rows_added();
	}

	// returns true if a specific input cache is finished
	bool input_cache_finished(const std::string & port_name) {
		return this->input_.is_finished(port_name);
	}

	// returns the number of rows added to a specific input cache
	uint64_t input_cache_num_rows_added(const std::string & port_name) {
		return this->input_.get_num_rows_added(port_name);
	}

	// this function gets the estimated num_rows for the output
	// the default is that its the same as the input (i.e. project, sort, ...)
	virtual std::pair<bool, uint64_t> get_estimated_output_num_rows();

	void wait_if_output_is_saturated(std::string cache_id = ""){
		std::string message_id = get_message_id();
		message_id = !cache_id.empty() ? cache_id + "_" + message_id : message_id;
		cache_id = cache_id.empty() ? std::to_string(this->get_id()) : cache_id;
		this->output_.get_cache(cache_id)->wait_if_cache_is_saturated();
	}

protected:


public:
	std::string expression;
	port input_{this};
	port output_{this};
	const std::size_t kernel_id;
	std::int32_t parent_id_;
	bool execution_done = false;
	kernel_type kernel_type_id;
	std::shared_ptr<graph> query_graph;
	std::shared_ptr<Context> context;

	// useful when the Algebra Relacional only contains: LogicalTableScan (or BindableTableScan) and LogicalLimit
	bool has_limit_;
	int64_t limit_rows_;

	std::shared_ptr<spdlog::logger> logger;
	std::shared_ptr<spdlog::logger> events_logger;
	std::shared_ptr<spdlog::logger> cache_events_logger;
};


}  // namespace cache
}  // namespace ral
