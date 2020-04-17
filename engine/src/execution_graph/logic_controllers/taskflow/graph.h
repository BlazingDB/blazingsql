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

		bool operator<(const Edge & e) const { return this->target < e.target; }
		bool operator==(const Edge & e) const { return this->target == e.target; }
	};

public:
	graph() {
		container_[head_id_] = nullptr;	 // sentinel node
	}
	graph(const graph &) = default;
	graph & operator=(const graph &) = default;

	kpair operator+=(kpair p) {
		std::string source_port_name = std::to_string(p.src->get_id());
		std::string target_port_name = std::to_string(p.dst->get_id());

		if(p.has_custom_source()) {
			source_port_name = p.src_port_name;
		}
		if(p.has_custom_target()) {
			target_port_name = p.dst_port_name;
		}
		this->add_edge(p.src, p.dst, source_port_name, target_port_name, p.cache_machine_config);
		return p;
	}

	void check_and_complete_work_flow() {
		for(auto node : container_) {
			kernel * kernel_node = node.second;
			if(kernel_node) {
				if(get_neighbours(kernel_node).size() == 0) {
					Edge fake_edge = {.source = (int32_t) kernel_node->get_id(),
						.target = -1,
						.source_port_name = std::to_string(kernel_node->get_id()),
						.target_port_name = ""};
					edges_[kernel_node->get_id()].insert(fake_edge);
				}
			}
		}
	}

	void execute() {
		check_and_complete_work_flow();

		std::vector<BlazingThread> threads;
		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		for(auto start_node : get_neighbours(head_id_)) {
			Q.push_back(start_node.target);
		}
		while(not Q.empty()) {
			auto source_id = Q.front();
			Q.pop_front();
			auto source = get_node(source_id);
			if(source) {
				for(auto edge : get_neighbours(source)) {
					auto target_id = edge.target;
					auto target = get_node(target_id);
					auto edge_id = std::make_pair(source_id, target_id);
					if(visited.find(edge_id) == visited.end()) {
						visited.insert(edge_id);
						Q.push_back(target_id);
						BlazingThread t([this, source, target, edge] {
							auto state = source->run();
							if(state == kstatus::proceed) {
								source->output_.finish();
							}
						});
						threads.push_back(std::move(t));
					} else {
						// TODO: and circular graph is defined here. Report and error
					}
				}
			}
		}
		for(auto & thread : threads) {
			thread.join();
		}
	}
	void show() {
		check_and_complete_work_flow();

		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		for(auto start_node : get_neighbours(head_id_)) {
			Q.push_back(start_node.target);
		}
		std::cout << "kernel id -> kernel type id\n";
		for(kernel * k : kernels_) {
			std::cout << (int) k->get_id() << " -> " << (int) k->get_type_id() << std::endl;
		}
		while(not Q.empty()) {
			auto source_id = Q.front();
			Q.pop_front();
			auto source = get_node(source_id);
			if(source) {
				for(auto edge : get_neighbours(source)) {
					auto target_id = edge.target;
					auto target = get_node(target_id);
					auto edge_id = std::make_pair(source_id, target_id);
					if(visited.find(edge_id) == visited.end()) {
						std::cout << "source_id: " << source_id << " -> " << target_id << std::endl;
						visited.insert(edge_id);
						Q.push_back(target_id);
					} else {
					}
				}
			}
		}
	}

	void show_from_kernel (int32_t id) {
		std::cout<<"show_from_kernel "<<id<<std::endl;
		check_and_complete_work_flow();

		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		for(auto start_node : get_reverse_neighbours(id)) {
			Q.push_back(start_node.source);
		}
		while(not Q.empty()) {
			auto target_id = Q.front();
			Q.pop_front();
			auto target = get_node(target_id);
			if(target) {
				for(auto edge : get_reverse_neighbours(target)) {
					auto source_id = edge.source;
					auto source = get_node(source_id);
					auto edge_id = std::make_pair(target_id, source_id);
					if(visited.find(edge_id) == visited.end()) {
						std::cout << "target_id: " << target_id << " <- " << source_id << std::endl;
						visited.insert(edge_id);
						Q.push_back(source_id);
					} else {

					}
				}
			}

		}
	}

	// std::pair<bool, uint64_t> get_estimated_input_rows_to_kernel(int32_t id){
	// 	auto target_kernel = get_node(id);
	// 	if (target_kernel->input_all_finished()){
	// 		return std::make_pair(true,target_kernel->total_rows_added());
	// 	}
	// 	std::set<std::pair<size_t, size_t>> visited;
	// 	std::deque<size_t> Q;
	// 	std::set<Edge> source_edges = get_reverse_neighbours(id);
	// 	if (source_edges.size() == 1){
	// 		target_kernel = get_node(source_edges.source);
	// 		return target_kernel->get_estimated_output_num_rows();
	// 		// get_estimated_output would just call get_estimated_input_rows_to_kernel for simple in/out kernels
	// 		// or do something more complicated for other kernels
	// 	}

	// }

	kernel & get_last_kernel() { return *kernels_.at(kernels_.size() - 1); }
	size_t num_nodes() const { return kernels_.size(); }
	size_t add_node(kernel * k) {
		if(k != nullptr) {
			container_[k->get_id()] = k;
			kernels_.push_back(k);
			return k->get_id();
		}
		return head_id_;
	}

	void add_edge(kernel * source,
		kernel * target,
		std::string source_port,
		std::string target_port,
		const cache_settings & config) {
		add_node(source);
		add_node(target);
		Edge edge = {.source = (std::int32_t) source->get_id(),
			.target = target->get_id(),
			.source_port_name = source_port,
			.target_port_name = target_port};
		edges_[source->get_id()].insert(edge);
		reverse_edges_[target->get_id()].insert(edge);

		target->set_parent(source->get_id());
		{
			std::vector<std::shared_ptr<CacheMachine>> cache_machines = create_cache_machines(config);
			if(config.type == CacheType::FOR_EACH) {
				for(size_t index = 0; index < cache_machines.size(); index++) {
					source->output_.register_cache("output_" + std::to_string(index), cache_machines[index]);
					target->input_.register_cache("input_" + std::to_string(index), cache_machines[index]);
				}
			} else {
				source->output_.register_cache(source_port, cache_machines[0]);
				target->input_.register_cache(target_port, cache_machines[0]);
			}
		}
		if(not source->has_parent()) {
			Edge fake_edge = {.source = head_id_,
				.target = source->get_id(),
				.source_port_name = "",
				.target_port_name = target_port};
			edges_[head_id_].insert(fake_edge);
		}
	}
	kernel * get_node(size_t id) { return container_[id]; }
	std::set<Edge> get_neighbours(kernel * from) { return edges_[from->get_id()]; }
	std::set<Edge> get_neighbours(int32_t id) { return edges_[id]; }
	std::set<Edge> get_reverse_neighbours(kernel * from) { return reverse_edges_[from->get_id()]; }
	std::set<Edge> get_reverse_neighbours(int32_t id) { return reverse_edges_[id]; }

private:
	const std::int32_t head_id_{-1};
	std::vector<kernel *> kernels_;
	std::map<std::int32_t, kernel *> container_;
	std::map<std::int32_t, std::set<Edge>> edges_;
	std::map<std::int32_t, std::set<Edge>> reverse_edges_;
};


}  // namespace cache
}  // namespace ral