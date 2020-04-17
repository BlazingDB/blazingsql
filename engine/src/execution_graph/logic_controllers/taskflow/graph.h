#pragma once

#include "kernel.h"
#include "kpair.h"

namespace ral {
namespace cache {

class kernel;

static std::shared_ptr<ral::cache::CacheMachine> create_cache_machine(const cache_settings& config) {
	std::shared_ptr<ral::cache::CacheMachine> machine;
	if (config.type == CacheType::NON_WAITING) {
		machine =  std::make_shared<ral::cache::NonWaitingCacheMachine>();
	}
	else if (config.type == CacheType::SIMPLE or config.type == CacheType::FOR_EACH) {
		machine =  std::make_shared<ral::cache::CacheMachine>();
	} else if (config.type == CacheType::CONCATENATING) {
		machine =  std::make_shared<ral::cache::ConcatenatingCacheMachine>();
	}
	return machine;
}

static std::vector<std::shared_ptr<ral::cache::CacheMachine>> create_cache_machines(const cache_settings& config) {
	std::vector<std::shared_ptr<ral::cache::CacheMachine>> machines;
	for (size_t i = 0; i < config.num_partitions; i++) {
		machines.push_back(create_cache_machine(config));
	}
	return machines;
}
class graph {
protected:
	struct Edge {
		std::int32_t source;
		std::int32_t target;
		std::string source_port_name;
		std::string target_port_name;

		bool operator<(const Edge & e) const { return this->target < e.target || (this->target == e.target && this->source < e.source); }
		bool operator==(const Edge & e) const { return this->target == e.target && this->source == e.source; }

		void print() const {
			std::cout<<"Edge: source id: "<<source<<" name: "<<source_port_name<<" target id: "<<target<<" name: "<<target_port_name<<std::endl;
		}
	};

public:
	graph() {
		container_[head_id_] = nullptr;	 // sentinel node
	}
	graph(const graph &) = default;
	graph & operator=(const graph &) = default;

	kpair operator+=(kpair p);

	void check_and_complete_work_flow();

	void execute();

	void show();

	void show_from_kernel (int32_t id);

	std::pair<bool, uint64_t> get_estimated_input_rows_to_kernel(int32_t id);

	std::pair<bool, uint64_t> get_estimated_input_rows_to_cache(int32_t id, const std::string & port_name);	

	kernel & get_last_kernel();

	size_t num_nodes() const;

	size_t add_node(kernel * k);

	void add_edge(kernel * source,
		kernel * target,
		std::string source_port,
		std::string target_port,
		const cache_settings & config);

	kernel * get_node(size_t id);

	std::set<Edge> get_neighbours(kernel * from);
	std::set<Edge> get_neighbours(int32_t id);
	std::set<Edge> get_reverse_neighbours(kernel * from);
	std::set<Edge> get_reverse_neighbours(int32_t id);

private:
	const std::int32_t head_id_{-1};
	std::vector<kernel *> kernels_;
	std::map<std::int32_t, kernel *> container_;
	std::map<std::int32_t, std::set<Edge>> edges_;
	std::map<std::int32_t, std::set<Edge>> reverse_edges_;
};


}  // namespace cache
}  // namespace ral