#pragma once

#include "utilities/random_generator.cuh"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <src/operators/OrderBy.h>
#include <src/utilities/DebuggingUtils.h>
#include <stack>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

namespace ral {
namespace cache {

enum kstatus { stop, proceed, keep_processing };

static const std::uint32_t MAX_SYSTEM_SIGNAL(0xfff);

enum signal : std::uint32_t { none = 0, quit, term, eof = MAX_SYSTEM_SIGNAL };

/** some helper structs for recursive port adding **/
template <class PORT, class... PORTNAMES>
struct port_helper {};

/** stop recursion **/
template <class PORT>
struct port_helper<PORT> {
	static void register_port(PORT & port) { return; }
};
class port;

/** continue recursion **/
template <class PORT, class PORTNAME, class... PORTNAMES>
struct port_helper<PORT, PORTNAME, PORTNAMES...> {
	static void register_port(PORT & port, PORTNAME && portname, PORTNAMES &&... portnames) {
		port.register_port(portname);
		port_helper<PORT, PORTNAMES...>::register_port(port, std::forward<PORTNAMES>(portnames)...);
		return;
	}
};
/** kicks off recursion for adding ports **/
template <class PORT, class... PORTNAMES>
static void kick_port_helper(PORT & port, PORTNAMES &&... ports) {
	port_helper<PORT, PORTNAMES...>::register_port(port, std::forward<PORTNAMES>(ports)...);
	return;
}
using frame_type = std::unique_ptr<ral::frame::BlazingTable>;
class kernel;

class port {
public:
	port(kernel * const k) { this->kernel_ = k; }

	virtual ~port() = default;

	template <class... PORTNAMES>
	void addPort(PORTNAMES &&... ports) {
		kick_port_helper<port, PORTNAMES...>((*this), std::forward<PORTNAMES>(ports)...);
	}

	size_t count() const { return cache_machines_.size(); }

	void register_port(std::string port_name);

	std::shared_ptr<CacheMachine> & get_cache(const std::string & port_name = "");
	void register_cache(const std::string & port_name, std::shared_ptr<CacheMachine> cache_machine);

	void finish();

	std::shared_ptr<CacheMachine> & operator[](const std::string & port_name) { return cache_machines_[port_name]; }

public:
	kernel * kernel_;
	std::map<std::string, std::shared_ptr<CacheMachine>> cache_machines_;
};

class kernel;
using kernel_pair = std::pair<kernel *, std::string>;

class kernel {
public:
	kernel() : kernel_id(kernel::kernel_count) {
		kernel::kernel_count++;
		parent_id_ = -1;
	}
	void set_parent(size_t id) { parent_id_ = id; }
	bool has_parent() const { return parent_id_ != -1; }

	virtual ~kernel() = default;

	virtual kstatus run() = 0;

	kernel_pair operator[](const std::string & portname) { return std::make_pair(this, portname); }

	std::int32_t get_id() { return (kernel_id); }

protected:
	static std::size_t kernel_count;

public:
	port input_{this};
	port output_{this};
	const std::size_t kernel_id;
	std::int32_t parent_id_;
	bool execution_done = false;
};


class kpair {
public:
	kpair(kernel & a, kernel & b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
		src = &a;
		dst = &b;
		this->cache_machine = cache_machine;
	}
	kpair(kernel & a, kernel_pair b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
		src = &a;
		dst = b.first;
		dst_port_name = b.second;
		this->cache_machine = cache_machine;
	}
	kpair(kernel_pair a, kernel & b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
		src = a.first;
		src_port_name = a.second;
		dst = &b;
		this->cache_machine = cache_machine;
	}
	bool has_custom_source() const { return not src_port_name.empty(); }
	bool has_custom_target() const { return not dst_port_name.empty(); }

	kernel * src = nullptr;
	kernel * dst = nullptr;
	std::shared_ptr<CacheMachine> cache_machine = nullptr;
	std::string src_port_name;
	std::string dst_port_name;
};

static kpair & operator>>(kernel & a, kernel & b) {
	auto pair = new kpair(a, b);
	return *pair;
}

static kpair & operator>>(kernel & a, kernel_pair b) {
	auto pair = new kpair(a, std::move(b));
	return *pair;
}
static kpair & operator>>(kernel_pair a, kernel & b) {
	auto pair = new kpair(std::move(a), b);
	return *pair;
}

static kpair & link(kernel & a, kernel & b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
	auto pair = new kpair(a, b, cache_machine);
	return *pair;
}

static kpair & link(kernel & a, kernel_pair b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
	auto pair = new kpair(a, std::move(b), cache_machine);
	return *pair;
}
static kpair & link(kernel_pair a, kernel & b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
	auto pair = new kpair(std::move(a), b, cache_machine);
	return *pair;
}


namespace order {
enum spec : std::uint8_t { in = 0, out = 1 };
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
	kpair operator+=(kpair & p) {
		std::string source_port_name = std::to_string(p.src->get_id());
		std::string target_port_name = std::to_string(p.dst->get_id());

		if(p.has_custom_source()) {
			source_port_name = p.src_port_name;
		}
		if(p.has_custom_target()) {
			target_port_name = p.dst_port_name;
		}
		this->add_edge(p.src, p.dst, source_port_name, target_port_name, p.cache_machine);
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

		std::vector<std::thread> threads;
		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		for(auto start_node : get_neighbours(head_id_)) {
			Q.push_back(start_node.target);
		}
		while(not Q.empty()) {
			auto source_id = Q.front();
			Q.pop_front();
			auto source = get_node(source_id);
			if(not source) {
				break;
			}
			std::cout << "source_id: " << source_id << "->";
			for(auto edge : get_neighbours(source)) {
				auto target_id = edge.target;
				std::cout << " " << target_id;
				auto target = get_node(target_id);
				auto edge_id = std::make_pair(source_id, target_id);
				if(visited.find(edge_id) == visited.end()) {
					visited.insert(edge_id);
					Q.push_back(target_id);
					std::thread t([this, source, target, edge] {
					  auto state = source->run();
					  if (state == kstatus::proceed) {
						  source->output_.finish();
					  } else if (state == kstatus::keep_processing) {
						  while (state == kstatus::keep_processing) {
							  std::cout << "keep_processing...\n";
							  auto state = source->run();
							  if (state == kstatus::stop) {
								  break;
							  }
						  }
						  source->output_.finish();
					  }
					});
					threads.push_back(std::move(t));
				} else {
					// TODO: and circular graph is defined here. Report and error
				}
			}
		}
		for(auto & thread : threads) {
			std::cout << "thread_id: " << thread.get_id() << std::endl;
			thread.join();
		}
	}
	size_t add_node(kernel * k) {
		if(k != nullptr) {
			container_[k->get_id()] = k;
			return k->get_id();
		}
		return head_id_;
	}

	std::shared_ptr<ral::cache::CacheMachine> create_cache_machine() {
		unsigned long long gpuMemory = 1024;
		std::vector<unsigned long long> memoryPerCache = {INT_MAX};
		std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
		return std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
	}

	void add_edge(kernel * source, kernel * target, std::string source_port, std::string target_port, std::shared_ptr<CacheMachine> cache_machine) {
		add_node(source);
		add_node(target);

		Edge edge = {.source = (std::int32_t) source->get_id(),
			.target = target->get_id(),
			.source_port_name = source_port,
			.target_port_name = target_port};
		edges_[source->get_id()].insert(edge);
		target->set_parent(source->get_id());
		{
			// update and link cacheMachine references
			if (cache_machine == nullptr)
				cache_machine = create_cache_machine();
			source->output_.register_cache(source_port, cache_machine);
			target->input_.register_cache(target_port, cache_machine);
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

private:
	const std::int32_t head_id_{-1};
	std::map<std::int32_t, kernel *> container_;
	std::map<std::int32_t, std::set<Edge>> edges_;
};

using SingleProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(const ral::frame::BlazingTableView & table,
																		 const std::string & expression,
																		 blazingdb::manager::experimental::Context * context);


template <SingleProcessorFunctor processor>
class SingleSourceKernel : public kernel {
public:
	SingleSourceKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->context = context;
		this->expression = queryString;
	}

	virtual kstatus run() {
		frame_type input = std::move(this->input_.get_cache()->pullFromCache());
		if (input) {
			auto output = processor(input->toBlazingTableView(), expression, context);
			this->output_.get_cache()->addToCache(std::move(output));
			return kstatus::proceed;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

using DoubleProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(const ral::frame::BlazingTableView & tableA,
																		 const ral::frame::BlazingTableView & tableB,
																		 const std::string & expression,
																		 blazingdb::manager::experimental::Context * context);

template <DoubleProcessorFunctor processor>
class DoubleSourceKernel : public kernel {
public:
	DoubleSourceKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->input_.addPort("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type input_a = std::move(this->input_["input_a"]->pullFromCache());
			frame_type input_b = std::move(this->input_["input_b"]->pullFromCache());
			auto output =
				processor(input_a->toBlazingTableView(), input_b->toBlazingTableView(), this->expression, this->context);
			this->output_.get_cache()->addToCache(std::move(output));
			return kstatus::proceed;
		} catch (std::exception e) {
			std::cerr << "Exception-DoubleSourceKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

using FilterKernel = SingleSourceKernel<ral::processor::process_filter>;
using ProjectKernel = SingleSourceKernel<ral::processor::process_project>;
using JoinKernel = DoubleSourceKernel<ral::processor::process_join>;
using SortKernel = SingleSourceKernel<ral::operators::experimental::sort>;
using SampleKernel = SingleSourceKernel<ral::operators::experimental::sample>;


class PartitionKernel : public kernel {
public:
	PartitionKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->input_.addPort("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type input_a = std::move(this->input_["input_a"]->pullFromCache());
			frame_type input_b = std::move(this->input_["input_b"]->pullFromCache());
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> output =
				ral::operators::experimental::partition(input_a->toBlazingTableView(), input_b->toBlazingTableView(), this->expression, this->context);
			for (auto& item : output) {
				this->output_.get_cache()->addToCache(std::move(item));
			}
			return kstatus::proceed;
		} catch (std::exception e) {
			std::cerr << "Exception-PartitionKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};
class MergeStreamKernel : public kernel {
public:
	MergeStreamKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->input_.addPort("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			std::vector<ral::frame::BlazingTableView> partitions_to_merge;
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge_holder;
			while ( not this->input_["input"]->is_finished()) {
				auto input = std::move(this->input_["input"]->pullFromCache());
				partitions_to_merge.emplace_back(std::move(input->toBlazingTableView()));
				partitions_to_merge_holder.emplace_back(std::move(input));
			}
			auto output = ral::operators::experimental::merge(partitions_to_merge, this->expression, this->context);
			this->output_.get_cache()->addToCache(std::move(output));
			return kstatus::proceed;
		} catch (std::exception e) {
			std::cerr << "Exception-DoubleSourceKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

// TODO: Find use case
template <SingleProcessorFunctor processor>
class StreamSingleSourceKernel : public kernel {
public:
	StreamSingleSourceKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->context = context;
		this->expression = queryString;
	}

	virtual kstatus run() {
		if (this->input_.get_cache()->is_finished()) {
			return kstatus::stop;
		}
		frame_type input = std::move(this->input_.get_cache()->pullFromCache());
		auto output = processor(input->toBlazingTableView(), expression, context);
		this->output_.get_cache()->addToCache(std::move(output));
		return kstatus::keep_processing;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};


class print : public kernel {
public:
	print() : kernel() { ofs = &(std::cout); }
	print(std::ostream & stream) : kernel() { ofs = &stream; }
	virtual kstatus run() {
		const std::string delim = "\n";
		std::lock_guard<std::mutex> lg(print::print_lock);
		frame_type table = std::move(this->input_.get_cache()->pullFromCache());
		ral::utilities::print_blazing_table_view(table->toBlazingTableView());
		return kstatus::stop;
	}

protected:
	std::ostream * ofs = nullptr;
	static std::mutex print_lock;
};

namespace test {
class generate : public kernel {
public:
	generate(std::int64_t count = 1000) : kernel(), count(count) {}
	virtual kstatus run() {

		cudf::test::fixed_width_column_wrapper<int32_t> column1{{0, 1, 2, 3, 4, 5}, {1, 1, 1, 1, 1, 1}};

		CudfTableView cudfTableView{{column1} };

		const std::vector<std::string> columnNames{"column1"};
		ral::frame::BlazingTableView blazingTableView{cudfTableView, columnNames};

		std::unique_ptr<ral::frame::BlazingTable> table = ral::generator::generate_sample(blazingTableView, 4);

		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	std::int64_t count;
};

namespace cudf_io = cudf::experimental::io;

class file_reader_kernel : public kernel {
public:
	file_reader_kernel(std::vector<std::string> file_paths) : kernel(), file_paths(file_paths) {}

	virtual kstatus run() {
		for(auto file_path : file_paths) {
			auto  open_table_from_path = [](std::string filepath) {
				cudf_io::read_parquet_args in_args{cudf_io::source_info{filepath}};
				auto output = cudf_io::read_parquet(in_args);
				return std::make_unique<ral::frame::BlazingTable>(std::move(output.tbl), output.metadata.column_names);
			};

			auto table = open_table_from_path(file_path);
			this->output_.get_cache()->addToCache(std::move(table));
		}
		return (kstatus::proceed);
	}

private:
	std::vector<std::string> file_paths;
};
}  // namespace test
using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;
using GeneratorKernel = ral::cache::test::generate;
using PrinterKernel = ral::cache::print;
using TableScanKernel = ral::cache::test::file_reader_kernel;

}  // namespace cache
}  // namespace ral
