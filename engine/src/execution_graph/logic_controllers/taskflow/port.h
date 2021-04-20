#pragma once

#include <vector>
#include <memory>
#include <string>
#include "execution_graph/logic_controllers/CacheMachine.h"

namespace ral {
namespace cache { 
enum kstatus { stop, proceed };

class kernel;

/**
	@brief This class represent a diccionary of tuples <port_name, cache_machine> used by
	each kernel (input and outputs) into the execution graph.
*/
class port {
public:
	port(kernel * const k) { this->kernel_ = k; }

	virtual ~port() = default;

	port& add_port(std::string port_name) {
		register_port(port_name);
		return *this;
	}

	size_t count() const { return cache_machines_.size(); }

	void register_port(std::string port_name);

	std::shared_ptr<CacheMachine> & get_cache(const std::string & port_name = "");

	void register_cache(const std::string & port_name, std::shared_ptr<CacheMachine> cache_machine);

	void finish();

	std::shared_ptr<CacheMachine> & operator[](const std::string & port_name) { return cache_machines_[port_name]; }

	bool all_finished();

	bool is_finished(const std::string & port_name);

	uint64_t total_bytes_added();

	uint64_t total_rows_added();

	uint64_t total_batches_added();

	uint64_t get_num_rows_added(const std::string & port_name);


public:
	kernel * kernel_;
	std::map<std::string, std::shared_ptr<CacheMachine>> cache_machines_;
};
 
}  // namespace cache
}  // namespace ral