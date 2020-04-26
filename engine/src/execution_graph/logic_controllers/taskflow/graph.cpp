#include "graph.h"

namespace ral {
namespace cache {

	kpair graph::operator+=(kpair p) {
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

	void graph::check_and_complete_work_flow() {
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

	void graph::execute() {
		check_and_complete_work_flow();

		std::vector<BlazingThread> threads;
		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		for(auto start_node : get_neighbours(head_id_)) {
			Q.push_back(start_node.target);
		}
		while(not Q.empty()) {
			auto source_id = Q.front();
			auto source = get_node(source_id);
			auto source_edges = get_reverse_neighbours(source);
			bool node_has_all_dependencies = source_edges.size() == 0 ? true : 
				std::all_of(source_edges.begin(), source_edges.end(), [visited](Edge edge) { 
					auto edge_id = std::make_pair(edge.source, edge.target);
					return visited.find(edge_id) != visited.end();});
			Q.pop_front();
			if (node_has_all_dependencies){
				if(source) {
					for(auto edge : get_neighbours(source)) {
						auto target_id = edge.target;
						auto target = get_node(target_id);
						auto edge_id = std::make_pair(source_id, target_id);
						if(visited.find(edge_id) == visited.end()) {
							visited.insert(edge_id);
							Q.push_back(target_id);
							BlazingThread t([this, source, source_id, edge] {
								auto state = source->run();
								if(state == kstatus::proceed) {
									source->output_.finish();
								} else if (edge.target != -1) { // not a dummy node
									std::cout<<"ERROR kernel "<<source_id<<" did not finished successfully"<<std::endl;
								}
							});
							threads.push_back(std::move(t));
						} else {
							// TODO: and circular graph is defined here. Report and error
						}
					}
				}
			} else { // if we dont have all the dependencies, lets put it back at the back and try it later
				Q.push_back(source_id);
			}
		}
		for(auto & thread : threads) {
			thread.join();
		}
	}

	void graph::show() {
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

	void graph::show_from_kernel (int32_t id) {
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

	std::pair<bool, uint64_t> graph::get_estimated_input_rows_to_kernel(int32_t id){
		auto target_kernel = get_node(id);
		if (target_kernel->input_all_finished()){
			return std::make_pair(true, target_kernel->total_input_rows_added());
		}
		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		std::set<Edge> source_edges = get_reverse_neighbours(id);
		if (source_edges.size() == 1){
			target_kernel = get_node((*(source_edges.begin())).source);
			// get_estimated_output would just call get_estimated_input_rows_to_kernel for simple in/out kernels
			// or do something more complicated for other kernels
			return target_kernel->get_estimated_output_num_rows();			
		} else {
			return std::make_pair(false, 0);
		}
	}

	std::pair<bool, uint64_t> graph::get_estimated_input_rows_to_cache(int32_t id, const std::string & port_name){
		auto target_kernel = get_node(id);
		if (target_kernel->input_cache_finished(port_name)){
			return std::make_pair(true, target_kernel->input_cache_num_rows_added(port_name));
		}
		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		std::set<Edge> source_edges = get_reverse_neighbours(id);
		for (auto edge : source_edges){
			if (edge.target_port_name == port_name){
				target_kernel = get_node(edge.source);
				// get_estimated_output would just call get_estimated_input_rows_to_kernel for simple in/out kernels
				// or do something more complicated for other kernels
				return target_kernel->get_estimated_output_num_rows();				
			}
		}
		std::cout<<"ERROR: In get_estimated_input_rows_to_cache could not find edge for kernel "<<id<<" cache "<<port_name<<std::endl;
		return std::make_pair(false, 0);
	}

	kernel & graph::get_last_kernel() { return *kernels_.at(kernels_.size() - 1); }
	
	size_t graph::num_nodes() const { return kernels_.size(); }
	
	size_t graph::add_node(kernel * k) {
		if(k != nullptr) {
			container_[k->get_id()] = k;
			kernels_.push_back(k);
			return k->get_id();
		}
		return head_id_;
	}

	void graph::add_edge(kernel * source,
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

	kernel * graph::get_node(size_t id) { return container_[id]; }

	std::set<graph::Edge> graph::get_neighbours(kernel * from) { return edges_[from->get_id()]; }
	std::set<graph::Edge> graph::get_neighbours(int32_t id) { return edges_[id]; }
	std::set<graph::Edge> graph::get_reverse_neighbours(kernel * from) { 
		if (from) {
			return get_reverse_neighbours(from->get_id());
		} else {
			return std::set<graph::Edge>();
		}
	}
	std::set<graph::Edge> graph::get_reverse_neighbours(int32_t id) { 
		if (reverse_edges_.find(id) != reverse_edges_.end()){
			return reverse_edges_[id]; 
		} else {
			return std::set<graph::Edge>();
		}
	}
	
}  // namespace cache
}  // namespace ral
