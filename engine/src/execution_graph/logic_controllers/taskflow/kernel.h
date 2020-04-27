#pragma once

#include "kernel_type.h"
#include "port.h"
#include "graph.h"

namespace ral {
namespace cache { 
class kernel;
class graph;
using kernel_pair = std::pair<kernel *, std::string>;

class kernel {
public:
	kernel(std::string expr = "") : expr{expr}, kernel_id(kernel::kernel_count) {
		kernel::kernel_count++;
		parent_id_ = -1;
	}
	void set_parent(size_t id) { parent_id_ = id; }
	bool has_parent() const { return parent_id_ != -1; }

	virtual ~kernel() = default;

	virtual kstatus run() = 0;

	kernel_pair operator[](const std::string & portname) { return std::make_pair(this, portname); }

	std::int32_t get_id() const { return (kernel_id); }

	kernel_type get_type_id() const { return kernel_type_id; }

	void set_type_id(kernel_type kernel_type_id_) { kernel_type_id = kernel_type_id_; }

	virtual std::string expression() { return expr; }

    std::shared_ptr<ral::cache::CacheMachine>  input_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->input_.get_cache(kernel_id);
	}
    
	std::shared_ptr<ral::cache::CacheMachine>  output_cache() {
		auto kernel_id = std::to_string(this->get_id());
		return this->output_.get_cache(kernel_id);
	}

    void add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string kernel_id = "") {
		if (kernel_id.empty()) {
			kernel_id = std::to_string(this->get_id());
		}
		
		std::string message_id = std::to_string((int)this->get_type_id()) + "_" + kernel_id;
		this->output_.get_cache(kernel_id)->addToCache(std::move(table), message_id);
	}

	void add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string kernel_id = "") {
		if (kernel_id.empty()) {
			kernel_id = std::to_string(this->get_id());
		}

		std::string message_id = std::to_string((int)this->get_type_id()) + "_" + kernel_id;
		this->output_.get_cache(kernel_id)->addCacheData(std::move(cache_data), message_id);
	}
	
	void add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string kernel_id = "") {
		if (kernel_id.empty()) {
			kernel_id = std::to_string(this->get_id());
		}
		
		std::string message_id = std::to_string((int)this->get_type_id()) + "_" + kernel_id;
		this->output_.get_cache(kernel_id)->addHostFrameToCache(std::move(host_table), message_id);
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

protected:
	static std::size_t kernel_count;

public:
	std::string expr;
	port input_{this};
	port output_{this};
	const std::size_t kernel_id;
	std::int32_t parent_id_;
	bool execution_done = false;
	kernel_type kernel_type_id;
	std::shared_ptr<graph> query_graph;
};

 
}  // namespace cache
}  // namespace ral