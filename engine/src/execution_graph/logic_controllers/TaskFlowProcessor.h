#pragma once

#include "utilities/random_generator.cuh"
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include "execution_graph/logic_controllers/TableScan.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <src/operators/OrderBy.h>
#include <src/operators/GroupBy.h>
#include <src/utilities/DebuggingUtils.h>
#include <stack>
#include "io/DataLoader.h"
#include "io/Schema.h"
#include <Util/StringUtil.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include "parser/expression_utils.hpp"

#include <cudf/copying.hpp>
#include <cudf/merge.hpp>
#include <src/utilities/CommonOperations.h>

namespace ral {
namespace cache {

enum kstatus { stop, proceed };

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
	void add_port(PORTNAMES &&... ports) {
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


enum class CacheType {
	SIMPLE, CONCATENATING, FOR_EACH
};

struct cache_settings {
	CacheType type = CacheType::SIMPLE;
	const int num_partitions = 1;
};

class kpair {
public:
	kpair(kernel & a, kernel & b, const cache_settings &config = cache_settings{})
		: cache_machine_config(config)
	{
		src = &a;
		dst = &b;
	}
	kpair(kernel & a, kernel_pair b, const cache_settings &config = cache_settings{})
		: cache_machine_config(config)
	{
		src = &a;
		dst = b.first;
		dst_port_name = b.second;
	}
	kpair(kernel_pair a, kernel & b, const cache_settings &config = cache_settings{})
		: cache_machine_config(config)
	{
		src = a.first;
		src_port_name = a.second;
		dst = &b;
	}
	kpair(kernel_pair a, kernel_pair b, const cache_settings &config = cache_settings{})
		: cache_machine_config(config)
	{
		src = a.first;
		src_port_name = a.second;
		dst = b.first;
		dst_port_name = b.second;
	}

	kpair(const kpair& ) = default;
	kpair(kpair&& ) = default;

	bool has_custom_source() const { return not src_port_name.empty(); }
	bool has_custom_target() const { return not dst_port_name.empty(); }

	kernel * src = nullptr;
	kernel * dst = nullptr;
	const cache_settings cache_machine_config;
	std::string src_port_name;
	std::string dst_port_name;
};

static kpair operator>>(kernel & a, kernel & b) {
	return kpair(a, b);
}

static kpair  operator>>(kernel & a, kernel_pair b) {
	return kpair(a, std::move(b));
}
static kpair  operator>>(kernel_pair a, kernel & b) {
	return kpair(std::move(a), b);
}

static kpair operator>>(kernel_pair a, kernel_pair b) {
	return kpair(std::move(a), std::move(b));
}

static kpair link(kernel & a, kernel & b, const cache_settings &config = cache_settings{}) {
	return kpair(a, b, config);
}

static kpair link(kernel & a, kernel_pair b, const cache_settings &config = cache_settings{}) {
	return kpair(a, std::move(b), config);
}

static kpair link(kernel_pair a, kernel & b, const cache_settings &config = cache_settings{}) {
	return kpair(std::move(a), b, config);
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
	graph(const graph& ) = default;
	graph& operator = (const graph& ) = default;
	
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
					Edge fake_edge = {
						.source = (int32_t) kernel_node->get_id(),
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
			for(auto edge : get_neighbours(source)) {
				auto target_id = edge.target;
				auto target = get_node(target_id);
				auto edge_id = std::make_pair(source_id, target_id);
				if(visited.find(edge_id) == visited.end()) {
//					std::cout << "source_id: " << source_id << " ->";
//					std::cout << " " << target_id << std::endl;
					visited.insert(edge_id);
					Q.push_back(target_id);
					std::thread t([this, source, target, edge] {
					  auto state = source->run();
					  if (state == kstatus::proceed) {
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
//			std::cout << "thread_id: " << thread.get_id() << std::endl;
			thread.join();
		}
	}
	kernel& get_last_kernel () {
		return *kernels_.at(kernels_.size() - 1);
	}
	size_t num_nodes() const {
		return kernels_.size();
	}
	size_t add_node(kernel * k) {
		if(k != nullptr) {
			container_[k->get_id()] = k;
			kernels_.push_back(k);
			return k->get_id();
		}
		return head_id_;
	}

	std::vector<std::shared_ptr<ral::cache::CacheMachine>> create_cache_machines(const cache_settings& config) {
		unsigned long long gpuMemory = 4294967296; //TODO: @alex, Fix this latter, default 4Gb
		std::vector<unsigned long long> memoryPerCache = {INT_MAX};
		std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};

		std::vector<std::shared_ptr<ral::cache::CacheMachine>> machines;
		for (size_t i = 0; i < config.num_partitions; i++) {
			std::shared_ptr<ral::cache::CacheMachine> machine;
			if (config.type == CacheType::SIMPLE or config.type == CacheType::FOR_EACH) {
				machine =  std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
			} else if (config.type == CacheType::CONCATENATING) {
				machine =  std::make_shared<ral::cache::ConcatenatingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
			}
			machines.push_back(machine);
		}
		return machines;
	}

	void add_edge(kernel * source, kernel * target, std::string source_port, std::string target_port,
		const cache_settings &config) {
		add_node(source);
		add_node(target);

		Edge edge = {.source = (std::int32_t) source->get_id(),
			.target = target->get_id(),
			.source_port_name = source_port,
			.target_port_name = target_port};
		edges_[source->get_id()].insert(edge);
		target->set_parent(source->get_id());
		{
			std::vector<std::shared_ptr<CacheMachine>> cache_machines = create_cache_machines(config);
			if (config.type == CacheType::FOR_EACH) {
				for (size_t index = 0; index < cache_machines.size(); index++) {
					source->output_.register_cache("output_" + std::to_string(index), cache_machines[index]);
					target->input_.register_cache("input_" + std::to_string(index), cache_machines[index]);
				}
			} else {
				source->output_.register_cache(source_port, cache_machines[0]);
				target->input_.register_cache(target_port, cache_machines[0]);
			}
		}
		if(not source->has_parent()) {
			Edge fake_edge = {
				.source = head_id_,
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
	std::vector<kernel *> kernels_;
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
			context->incrementQueryStep();
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
		this->input_.add_port("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type input_a = std::move(this->input_["input_a"]->pullFromCache());
			frame_type input_b = std::move(this->input_["input_b"]->pullFromCache());
			auto output =
				processor(input_a->toBlazingTableView(), input_b->toBlazingTableView(), this->expression, this->context);
			context->incrementQueryStep();
			this->output_.get_cache()->addToCache(std::move(output));
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-DoubleSourceKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

using FilterKernel = SingleSourceKernel<ral::processor::process_filter>;

 class ProjectKernel : public kernel {
public:
	 ProjectKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->context = context;
		this->expression = queryString;
	}

	virtual kstatus run() {
		frame_type input = std::move(this->input_.get_cache()->pullFromCache());
		if (input) {
			auto output = ral::processor::process_project(std::move(input), expression, context);
			this->output_.get_cache()->addToCache(std::move(output));
			context->incrementQueryStep();
			return kstatus::proceed;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

using AggregateKernel = SingleSourceKernel<ral::operators::experimental::process_aggregate>;
using JoinKernel = DoubleSourceKernel<ral::processor::process_join>;
// using UnionKernel = DoubleSourceKernel<ral::operators::experimental::sample>;

using SortKernel = SingleSourceKernel<ral::operators::experimental::process_sort>;
using SampleKernel = SingleSourceKernel<ral::operators::experimental::sample>;

 class UnionKernel : public kernel {
 public:
	 UnionKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
 		this->input_.add_port("input_a", "input_b");
 		this->context = context;
 		this->expression = queryString;
 	}
 	virtual kstatus run() {
 		try {
 			frame_type input_a = std::move(this->input_["input_a"]->pullFromCache());
 			frame_type input_b = std::move(this->input_["input_b"]->pullFromCache());

 			int numLeft = input_a->num_rows();
 			int numRight = input_b->num_rows();

 			frame_type output;
 			if (numLeft == 0){
 				output = std::move(input_b);
 			} else if (numRight == 0) {
 				output = std::move(input_a);
 			} else {
				auto left = input_a->toBlazingTableView();
				auto right =  input_b->toBlazingTableView();

				bool isUnionAll = (get_named_expression(this->expression, "all") == "true");
				if(!isUnionAll) {
					throw std::runtime_error{"In process_union function: UNION is not supported, use UNION ALL"};
				}
				// Check same number of columns
				if(left.num_columns() != right.num_columns()) {
					throw std::runtime_error{
						"In process_union function: left frame and right frame have different number of columns"};
				}
				std::vector<ral::frame::BlazingTableView> tables{left, right};
				output = ral::utilities::experimental::concatTables(tables);
 			}
 			context->incrementQueryStep();
 			this->output_.get_cache()->addToCache(std::move(output));
 			return kstatus::proceed;
 		} catch (std::exception &e) {
 			std::cerr << "Exception-UnionKernel: " << e.what() << std::endl;
 		}
 		return kstatus::stop;
 	}

 private:
 	blazingdb::manager::experimental::Context * context;
 	std::string expression;
 };

class SortAndSampleKernel : public kernel {
public:
	SortAndSampleKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->output_.add_port("output_a", "output_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type input = std::move(this->input_.get_cache()->pullFromCache());
			std::unique_ptr<ral::frame::BlazingTable> sortedTable;
			std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
			std::tie(sortedTable, partitionPlan) =
				ral::operators::experimental::sort_and_sample(input->toBlazingTableView(), this->expression, this->context);

			context->incrementQueryStep();
			this->output_["output_a"]->addToCache(std::move(sortedTable));
			if (context->getTotalNodes() > 1) {
				this->output_["output_b"]->addToCache(std::move(partitionPlan));
			}
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-SortAndSampleKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

class PartitionKernel : public kernel {
public:
	PartitionKernel(std::string queryString, blazingdb::manager::experimental::Context * context) {
		this->input_.add_port("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type sortedTable = std::move(this->input_["input_a"]->pullFromCache());
			std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions;
			if (context->getTotalNodes() > 1) {
				partitionPlan = std::move(this->input_["input_b"]->pullFromCache());
				partitions =
					ral::operators::experimental::partition(partitionPlan->toBlazingTableView(),
					sortedTable->toBlazingTableView(), this->expression, this->context);
			} else {
				partitions =
					ral::operators::experimental::partition({}, sortedTable->toBlazingTableView(), this->expression, this->context);
			}
			for(auto& partition : partitions)
				ral::utilities::print_blazing_table_view(partition->toBlazingTableView());

			auto range_generator = [](cudf::size_type begin, cudf::size_type end, cudf::size_type num_parts) {
				std::vector<cudf::size_type> split_indexes;
	  		  	cudf::size_type step;
				if (end - begin <= num_parts)
					return split_indexes;
				step = (end - begin) / num_parts;
				for (auto i = step; i < end; i += step) {
					split_indexes.push_back(i);
				} 
				return split_indexes;
			};

			cudf::size_type num_subparts = 2; //TODO: Optimize this number: default 10
			for (size_t index = 0; index < partitions.size(); index++) {
				std::unique_ptr<ral::frame::BlazingTable> &part = partitions[index];
				std::vector<cudf::size_type> split_indexes = range_generator(0, part->num_rows(), num_subparts);
				std::vector<CudfTableView> partitioned;
				if (split_indexes.size() == 0) {
					partitioned.push_back(part->view());
				} else {
					partitioned = cudf::experimental::split(part->view(), split_indexes);
				}
				for (auto& sub_part_view : partitioned) {
					std::string cache_id = "output_" + std::to_string(index);
					std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(sub_part_view);
 					auto sub_part = std::make_unique<ral::frame::BlazingTable>(std::move(cudfTable), part->names());
					std::cout << "partitions-subpart : " << cache_id  << "\n";
					ral::utilities::print_blazing_table_view(sub_part->toBlazingTableView());

					this->output_[cache_id]->addToCache(std::move(sub_part));
				}
			}
			return kstatus::proceed;
		} catch (std::exception &e) {
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
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			for (size_t index = 0; index < this->input_.count(); index++) {
				auto cache_id = "input_" + std::to_string(index);
				std::vector<ral::frame::BlazingTableView> partitions_to_merge;
				std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge_holder;
				while (not this->input_.get_cache(cache_id)->is_finished()) {
					auto input = std::move(this->input_.get_cache(cache_id)->pullFromCache());
					if (input) {
						partitions_to_merge.emplace_back(input->toBlazingTableView());
						partitions_to_merge_holder.emplace_back(std::move(input));
					} else {
						break;
					}
				}
				std::cout << "merge-parts: " << std::endl;
				for (auto view : partitions_to_merge)
					ral::utilities::print_blazing_table_view(view);

				auto output = ral::operators::experimental::merge(partitions_to_merge, this->expression, this->context);
				std::cout << "merge: " << cache_id << "|" << this->expression<< std::endl;
				ral::utilities::print_blazing_table_view(output->toBlazingTableView());

				this->output_.get_cache()->addToCache(std::move(output));
			}
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-DoubleSourceKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
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

class parquet_file_reader_kernel : public kernel {
public:
	parquet_file_reader_kernel(std::vector<std::string> file_paths) : kernel(), file_paths(file_paths) {}

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

class BindableTableScanKernel : public kernel {
public:
	BindableTableScanKernel(std::string expr, ral::io::data_loader &loader, ral::io::Schema & schema, blazingdb::manager::experimental::Context* context)
	: kernel(), expr(expr), context(context), loader(loader), schema(schema)
	{}

	virtual kstatus run() {
		auto output = ral::processor::process_table_scan(loader, expr, schema, context);
		this->output_.get_cache()->addToCache(std::move(output));
		return (kstatus::proceed);
	}

private:
	blazingdb::manager::experimental::Context *context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	std::string expr;
};

class TableScanKernel : public kernel {
public:
	TableScanKernel(ral::io::data_loader &loader, ral::io::Schema & schema, blazingdb::manager::experimental::Context* context)
		: kernel(), context(context), loader(loader), schema(schema)
	{}

	virtual kstatus run() {
		auto table = loader.load_data(context, {}, schema);
		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	blazingdb::manager::experimental::Context *context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
};

class OutputKernel : public kernel {
public:
	OutputKernel() : kernel() {  }
	virtual kstatus run() {
		output = std::move(this->input_.get_cache()->pullFromCache());
		return kstatus::stop;
	}
	
	frame_type	release() {
		return std::move(output);
	}

protected:
	frame_type output;
};

using GeneratorKernel = ral::cache::test::generate;
using PrinterKernel = ral::cache::print;
using ParquetFileReaderKernel = ral::cache::test::parquet_file_reader_kernel;


namespace parser{

struct expr_tree_processor {
	struct node {
		std::string expr;               // expr
		int level;                      // level
		std::shared_ptr<kernel>            kernel_unit;
		std::vector<std::shared_ptr<node>> children;  // children nodes
	} root;
	blazingdb::manager::experimental::Context * context;
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::vector<std::string> table_names;

	void expr_tree_from_json(boost::property_tree::ptree const& p_tree, expr_tree_processor::node * root_ptr, int level) {
		auto expr = p_tree.get<std::string>("expr", "");
//		for(int i = 0; i < level*2 ; ++i) {
//			std::cout << " ";
//		}
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = get_kernel(expr);
//		std::cout << expr << std::endl;
		for (auto &child : p_tree.get_child("children"))
		{
			auto child_node_ptr = std::make_shared<expr_tree_processor::node>();
			root_ptr->children.push_back(child_node_ptr);
			expr_tree_from_json(child.second, child_node_ptr.get(), level + 1);
		}
	}

	std::shared_ptr<kernel>  get_kernel(std::string expr) {
		if ( is_project(expr) )
			return std::make_shared<ProjectKernel>(expr, this->context);
		else if ( is_filter(expr) )
			return std::make_shared<FilterKernel>(expr, this->context);
		else if ( is_join(expr) )
			return std::make_shared<JoinKernel>(expr, this->context);
		else if ( is_union(expr) )
			return std::make_shared<UnionKernel>(expr, this->context);
		else if ( is_project(expr) )
			return std::make_shared<ProjectKernel>(expr, this->context);
		else if ( is_sort(expr) ) {
			// TODO transform sort-sample-partition-merge machine
			return std::make_shared<SortKernel>(expr, this->context);
		} else if ( is_aggregate(expr) ) {
			return std::make_shared<AggregateKernel>(expr, this->context);
		} else if ( is_logical_scan(expr) ) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			return std::make_shared<TableScanKernel>(this->input_loaders[table_index], this->schemas[table_index], this->context);
		} else if (is_bindable_scan(expr)) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			return std::make_shared<BindableTableScanKernel>(expr, this->input_loaders[table_index], this->schemas[table_index], this->context);
		}		
		return nullptr;
	}

	ral::cache::graph build_graph(std::string json) {
		try {
			std::replace( json.begin(), json.end(), '\'', '\"');
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			expr_tree_from_json(p_tree, &this->root, 0);

		} catch (std::exception & e) {
			std::cerr << e.what() <<  std::endl;
		}

		ral::cache::graph graph;
		if (this->root.kernel_unit != nullptr) {
			graph.add_node(this->root.kernel_unit.get()); // register first node
			visit(graph, &this->root, this->root.children);
		}
		return graph;
	}

	void visit(ral::cache::graph& graph, node * parent, std::vector<std::shared_ptr<node>>& children) {
		for (size_t index = 0; index < children.size(); index++) {
			auto& child  =  children[index];
			visit(graph, child.get(), child->children);

			std::string port_name = "input";
			if (children.size() > 1) {
				char index_char = 'a' + index;
				port_name = std::string("input_");
				port_name.push_back(index_char); 
//				std::cout << "link: " << child->kernel_unit->get_id() << " -> " << parent->kernel_unit->get_id() << "["  << port_name<< "]" << std::endl;
				graph +=  *child->kernel_unit >> (*parent->kernel_unit)[port_name];
			} else {
				graph +=  *child->kernel_unit >> (*parent->kernel_unit);
			}
		}
	}

};

} // namespace parser


}  // namespace cache
}  // namespace ral
