
#include "TaskFlowProcessor.h"
namespace ral {
namespace cache {

std::size_t kernel::kernel_count(0);

void port::register_port(std::string port_name) { cache_machines_[port_name] = nullptr; }

std::shared_ptr<CacheMachine> & port::get_cache(const std::string & port_name) {
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		auto it = cache_machines_.find(id);
		return it->second;
	}	
	auto it = cache_machines_.find(port_name);
	return it->second;
}

void port::register_cache(const std::string & port_name, std::shared_ptr<CacheMachine> cache_machine) {
	this->cache_machines_[port_name] = cache_machine;
}
void port::finish() {
	for (auto it :cache_machines_) {
		it.second->finish();
	}
}

std::mutex print::print_lock{};




} //end namespace cache
} //end namespace ral

