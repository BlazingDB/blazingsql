#include "port.h"
#include "kernel.h"

namespace ral {
namespace cache {
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
	for(auto it : cache_machines_) {
		it.second->finish();
	}
}

bool port::all_finished(){
	for (auto cache : cache_machines_){
		if (!cache.second->is_finished())
			return false;
	}
	return true;
}

uint64_t port::total_bytes_added(){
	uint64_t total = 0;
	for (auto cache : cache_machines_){
		total += cache.second->get_num_bytes_added();
	}
	return total;
}

uint64_t port::total_rows_added(){
	uint64_t total = 0;
	for (auto cache : cache_machines_){
		total += cache.second->get_num_rows_added();
	}
	return total;
}


}  // end namespace cache
}  // end namespace ral