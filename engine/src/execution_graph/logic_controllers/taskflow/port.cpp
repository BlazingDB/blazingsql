#include "port.h"
#include "kernel.h"

namespace ral {
namespace cache {
void port::register_port(std::string port_name) {
	cache_machines_[port_name].push_back(nullptr);
}

std::shared_ptr<CacheMachine> & port::get_cache(const std::string & port_name) {
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		auto it = cache_machines_.find(id);
		return it->second[0]; //default
	}
	auto it = cache_machines_.find(port_name);
	if (it == cache_machines_.end()){
        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
		std::string all_cache_names = "";
		for(auto it2 = cache_machines_.begin(); it2 != cache_machines_.end(); ++it2) {
			all_cache_names += it2->first + ", ";
		}
		std::string log_detail = "ERROR get_cache did not find cache " + port_name + " the caches available are: " + all_cache_names;
		if(logger){
		    logger->error("|||{info}|||||","info"_a=log_detail);
		}
	}
	return it->second[0]; //default
}

void port::register_cache(const std::string & port_name, std::shared_ptr<CacheMachine> cache_machine) {
	if(this->cache_machines_[port_name].empty()){
		this->cache_machines_[port_name].push_back(cache_machine); //todo
	}
	else{
		this->cache_machines_[port_name][0] = cache_machine;
	}
}
void port::finish() {
	for (auto cache_vector : cache_machines_){
		for (auto cache : cache_vector.second){
			cache->finish();
		}
	}
}

bool port::all_finished(){
	for (auto cache_vector : cache_machines_){
		for (auto cache : cache_vector.second){
			if (!cache->is_finished())
				return false;
		}
	}
	return true;
}

bool port::is_finished(const std::string & port_name){
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		auto cache_vector = cache_machines_.find(id);
		bool is_finished = true;
		for (auto cache : cache_vector->second){
			is_finished = is_finished && cache->is_finished();
		}
		return is_finished;
	}
	bool is_finished = true;
	auto cache_vector = cache_machines_.find(port_name);
	for (auto cache : cache_vector->second){
		is_finished = is_finished && cache->is_finished();
	}
	return is_finished;
}

uint64_t port::total_bytes_added(){
	uint64_t total = 0;
	for (auto cache_vector : cache_machines_){
		for (auto cache : cache_vector.second){
			total += cache->get_num_bytes_added();
		}
	}
	return total;
}

uint64_t port::total_rows_added(){
	uint64_t total = 0;
	for (auto cache_vector : cache_machines_){
		for (auto cache : cache_vector.second){
			total += cache->get_num_rows_added();
		}
	}
	return total;
}

uint64_t port::total_batches_added(){
	uint64_t total = 0;
	for (auto cache_vector : cache_machines_){
		for (auto cache : cache_vector.second){
			total += cache->get_num_batches_added();
		}
	}
	return total;
}

uint64_t port::get_num_rows_added(const std::string & port_name){
	if(port_name.length() == 0) {
		// NOTE: id is the `default` cache_machine name
		auto id = std::to_string(kernel_->get_id());
		uint64_t num_rows_added = 0;
		auto cache_vector = cache_machines_.find(id);
		for (auto cache : cache_vector->second){
			num_rows_added += cache->get_num_rows_added();
		}
		return num_rows_added;
	}
	uint64_t num_rows_added = 0;
	auto cache_vector = cache_machines_.find(port_name);
	for (auto cache : cache_vector->second){
		num_rows_added += cache->get_num_rows_added();
	}
	return num_rows_added;
}


}  // end namespace cache
}  // end namespace ral