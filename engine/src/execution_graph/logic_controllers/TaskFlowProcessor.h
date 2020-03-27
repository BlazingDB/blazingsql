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
#include <blazingdb/io/Library/Logging/Logger.h>

#include <src/operators/OrderBy.h>
#include <src/operators/GroupBy.h>
#include <src/utilities/DebuggingUtils.h>
#include <stack>
#include "io/DataLoader.h"
#include "io/Schema.h"
#include <Util/StringUtil.h>
#include "CodeTimer.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include "parser/expression_utils.hpp"

#include <cudf/copying.hpp>
#include <cudf/merge.hpp>
#include <cudf/search.hpp>
#include <cudf/sorting.hpp>
#include <src/CalciteInterpreter.h>
#include <src/utilities/CommonOperations.h>

#include "distribution/primitives.h"
#include "config/GPUManager.cuh"
#include "CacheMachine.h"
#include "blazingdb/concurrency/BlazingThread.h"

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

enum class kernel_type {
	ProjectKernel,
	FilterKernel,
	JoinKernel,
	UnionKernel,
	SortKernel,
	MergeStreamKernel,
	PartitionKernel,
	SortAndSampleKernel,
	PartitionSingleNodeKernel,
	SortAndSampleSingleNodeKernel,
	AggregateKernel,  // this is the base AggregateKernel that gets replaced
	ComputeAggregateKernel,
	DistributeAggregateKernel,
	MergeAggregateKernel,
	AggregateAndSampleKernel, // to be deprecated
	AggregatePartitionKernel, // to be deprecated
	AggregateMergeStreamKernel, // to be deprecated
	TableScanKernel,
	BindableTableScanKernel
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

	kernel_type get_type_id() { return kernel_type_id; }

	void set_type_id(kernel_type kernel_type_id_) { kernel_type_id = kernel_type_id_; }

protected:
	static std::size_t kernel_count;

public:
	port input_{this};
	port output_{this};
	const std::size_t kernel_id;
	std::int32_t parent_id_;
	bool execution_done = false;
	kernel_type kernel_type_id;
};


enum class CacheType {
	NON_WAITING, SIMPLE, CONCATENATING, FOR_EACH
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

static std::shared_ptr<ral::cache::CacheMachine> create_cache_machine(const cache_settings& config) {
	size_t gpuMemory = ral::config::gpuMemorySize() * 0.75;
	assert(gpuMemory > 0);
	size_t cpuMemory = ral::config::gpuMemorySize();

	std::vector<unsigned long long> memoryPerCache = {cpuMemory, INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::CPU, ral::cache::CacheDataType::LOCAL_FILE};
	std::shared_ptr<ral::cache::CacheMachine> machine;
	if (config.type == CacheType::NON_WAITING) {
		machine =  std::make_shared<ral::cache::NonWaitingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
	}
	else if (config.type == CacheType::SIMPLE or config.type == CacheType::FOR_EACH) {
		machine =  std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
	} else if (config.type == CacheType::CONCATENATING) {
		machine =  std::make_shared<ral::cache::ConcatenatingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
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
			if(not source) {
				break;
			}
			for(auto edge : get_neighbours(source)) {
				auto target_id = edge.target;
				auto target = get_node(target_id);
				auto edge_id = std::make_pair(source_id, target_id);
				if(visited.find(edge_id) == visited.end()) {
					visited.insert(edge_id);
					Q.push_back(target_id);
					BlazingThread t([this, source, target, edge] {
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
			thread.join();
		}
	}
	void show () {
		check_and_complete_work_flow();

		std::set<std::pair<size_t, size_t>> visited;
		std::deque<size_t> Q;
		for(auto start_node : get_neighbours(head_id_)) {
			Q.push_back(start_node.target);
		}
		std::cout << "kernel id -> kernel type id\n";
		for(kernel *k : kernels_) {
			std::cout << (int)k->get_id() << " -> " << (int)k->get_type_id() << std::endl;
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
					std::cout << "source_id: " << source_id << " -> " << target_id << std::endl;
					visited.insert(edge_id);
					Q.push_back(target_id);
				} else {

				}
			}
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

using Context = blazingdb::manager::experimental::Context;
using SingleProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(const ral::frame::BlazingTableView & table,
																		 const std::string & expression,
																		 Context* context);


template <SingleProcessorFunctor processor>
class SingleSourceKernel : public kernel {
public:
	SingleSourceKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->context = context;
		this->expression = queryString;
	}

	virtual kstatus run() {
		frame_type input = std::move(this->input_.get_cache()->pullFromCache());
		if (input) {
			auto output = processor(input->toBlazingTableView(), expression, context.get());
			this->output_.get_cache()->addToCache(std::move(output));
			context->incrementQueryStep();
			return kstatus::proceed;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

using DoubleProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(const ral::frame::BlazingTableView & tableA,
																		 const ral::frame::BlazingTableView & tableB,
																		 const std::string & expression,
																		 Context* context);

template <DoubleProcessorFunctor processor>
class DoubleSourceKernel : public kernel {
public:
	DoubleSourceKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->input_.add_port("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type input_a = std::move(this->input_["input_a"]->pullFromCache());
			frame_type input_b = std::move(this->input_["input_b"]->pullFromCache());
			auto output =
				processor(input_a->toBlazingTableView(), input_b->toBlazingTableView(), this->expression, this->context.get());
			context->incrementQueryStep();
			this->output_.get_cache()->addToCache(std::move(output));
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-DoubleSourceKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

using FilterKernel = SingleSourceKernel<ral::processor::process_filter>;

 class ProjectKernel : public kernel {
public:
	 ProjectKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->context = context;
		this->expression = queryString;
	}

	virtual kstatus run() {
		frame_type input = std::move(this->input_.get_cache()->pullFromCache());
		if (input) {
			auto output = ral::processor::process_project(std::move(input), expression, context.get());
			// std::cout<< ">>>>>>>> PROJECT: " << std::endl;
			// ral::utilities::print_blazing_table_view(output->toBlazingTableView());
			this->output_.get_cache()->addToCache(std::move(output));
			context->incrementQueryStep();
			return kstatus::proceed;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

using AggregateKernel = SingleSourceKernel<ral::operators::experimental::process_aggregate>;
//using JoinKernel = DoubleSourceKernel<ral::processor::process_join>;
// using UnionKernel = DoubleSourceKernel<ral::operators::experimental::sample>;

using SortKernel = SingleSourceKernel<ral::operators::experimental::process_sort>;

class JoinKernel :  public kernel {
public:
	JoinKernel(const std::string & queryString, std::shared_ptr<Context> context) {
			this->input_.add_port("input_a", "input_b");
			this->context = context;
			this->expression = queryString;
		}
		virtual kstatus run() {
			try {
				CodeTimer blazing_timer;
				blazing_timer.reset();   

				frame_type left_frame_result = std::move(this->input_["input_a"]->pullFromCache());
				frame_type right_frame_result = std::move(this->input_["input_b"]->pullFromCache());
				int numLeft = left_frame_result->num_rows();
				int numRight = right_frame_result->num_rows();

				std::string new_join_statement, filter_statement;
				StringUtil::findAndReplaceAll(expression, "IS NOT DISTINCT FROM", "=");
				split_inequality_join_into_join_and_filter(expression, new_join_statement, filter_statement);

				std::unique_ptr<ral::frame::BlazingTable> result_frame = ral::processor::process_logical_join(context.get(), left_frame_result->toBlazingTableView(), right_frame_result->toBlazingTableView(), new_join_statement);
				std::string extraInfo = "left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
				Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query process_join", "num rows result", result_frame->num_rows(), extraInfo));
				blazing_timer.reset();
				context->incrementQueryStep();
				if (filter_statement != ""){
					result_frame = ral::processor::process_filter(result_frame->toBlazingTableView(), filter_statement, context.get());
					Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query inequality join process_filter", "num rows", result_frame->num_rows()));
					blazing_timer.reset();
					context->incrementQueryStep();
				}
				this->output_.get_cache()->addToCache(std::move(result_frame));
				return kstatus::proceed;
			} catch (std::exception &e) {
				std::cerr << "Exception-JoinKernel: " << e.what() << std::endl;
			}
			return kstatus::stop;
		}

	private:
		std::shared_ptr<Context> context;
		std::string expression;
	};
class UnionKernel : public kernel {
 public:
	 UnionKernel(const std::string & queryString, std::shared_ptr<Context> context) {
 		this->input_.add_port("input_a", "input_b");
 		this->context = context;
 		this->expression = queryString;
 	}
 	virtual kstatus run() {
		CodeTimer blazing_timer;
		blazing_timer.reset();

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
		std::string extraInfo =	"left_side_num_rows:" + std::to_string(numLeft) + ":right_side_num_rows:" + std::to_string(numRight);
		Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context,
				"evaluate_split_query process_union", "num rows result", output->num_rows(), extraInfo));
		blazing_timer.reset();
		context->incrementQueryStep();
		this->output_.get_cache()->addToCache(std::move(output));
		return kstatus::proceed; 
 		return kstatus::stop;
 	}

 private:
 	std::shared_ptr<Context> context;
 	std::string expression;
 };

class SortAndSampleKernel : public kernel {
public:
	SortAndSampleKernel(const std::string & queryString, std::shared_ptr<Context> context) {
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
				ral::operators::experimental::sort_and_sample(input->toBlazingTableView(), this->expression, this->context.get());

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
	std::shared_ptr<Context> context;
	std::string expression;
};

class PartitionKernel : public kernel {
public:
	PartitionKernel(const std::string & queryString, std::shared_ptr<Context> context) {
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
					ral::operators::experimental::partition_sort(partitionPlan->toBlazingTableView(),
					sortedTable->toBlazingTableView(), this->expression, context.get());
			} else {
				partitions =
					ral::operators::experimental::partition_sort({}, sortedTable->toBlazingTableView(), this->expression, context.get());
			}
//			for(auto& partition : partitions)
//				ral::utilities::print_blazing_table_view(partition->toBlazingTableView());

			// Split Partitions
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> samples;
			for (auto i = 0; i < partitions.size(); i++) {
				auto part_samples = ral::distribution::sampling::experimental::generateSamplesFromRatio(partitions[i]->toBlazingTableView(), 0.25);
				samples.push_back(std::move(part_samples));
			}

			std::vector<ral::frame::BlazingTableView> samples_view;
			for (auto &&s : samples) {
				samples_view.push_back(s->toBlazingTableView());
			}
			auto concat_samples = ral::utilities::experimental::concatTables(samples_view);

			std::vector<cudf::order> orders(samples_view[0].num_columns(), cudf::order::ASCENDING);
			std::vector<cudf::null_order> null_orders(samples_view[0].num_columns(), cudf::null_order::AFTER);
			auto sorted_samples = cudf::experimental::sort(concat_samples->view(), orders, null_orders);

//			std::cout<< "SORTED SAMPLES: " <<std::endl;
//			for (auto &&c : sorted_samples->view())
//			{
//				cudf::test::print(c);
//				std::cout << std::endl;
//			}

			cudf::size_type samples_rows = sorted_samples->view().num_rows();
			cudf::size_type pivots_size = samples_rows > 0 ? 3 /* How many subpartitions? */ : 0;

			int32_t step = samples_rows / (pivots_size + 1);
			auto sequence_iter = cudf::test::make_counting_transform_iterator(0, [step](auto i) { return int32_t(i * step) + step;});
			cudf::test::fixed_width_column_wrapper<int32_t> gather_map_wrapper(sequence_iter, sequence_iter + pivots_size);
			auto pivots = cudf::experimental::gather(sorted_samples->view(), gather_map_wrapper);
			
//			std::cout<< "PIVOTS: " <<std::endl;
//			for (auto &&c : pivots->view())
//			{
//				cudf::test::print(c);
//				std::cout << std::endl;
//			}

			for (auto i = 0; i < partitions.size(); i++) {
				auto pivot_indexes = cudf::experimental::upper_bound(partitions[i]->view(),
																															pivots->view(),
																															orders,
																															null_orders);
		
//				std::cout<< "PIVOTS indices: " <<std::endl;
//				cudf::test::print(pivot_indexes->view());
//				std::cout << std::endl;
				
				auto host_pivot_col = cudf::test::to_host<cudf::size_type>(pivot_indexes->view());
				auto host_pivot_indexes = host_pivot_col.first;

				auto partitioned_data = cudf::experimental::split(partitions[i]->view(), host_pivot_indexes);

				// std::cout<< ">>>>>> TOTAL PARTITIONS : " << partitioned_data.size() << std::endl;
				std::string cache_id = "output_" + std::to_string(i);
				for (auto &&subpartition : partitioned_data) {
					this->output_[cache_id]->addToCache(
						std::make_unique<ral::frame::BlazingTable>(
							std::make_unique<cudf::experimental::table>(subpartition),
							partitions[i]->names())
						);
				}
			}

			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-PartitionKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class MergeStreamKernel : public kernel {
public:
	MergeStreamKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {

			while (not this->input_.get_cache("input_0")->is_finished())
			{
				std::vector<ral::frame::BlazingTableView> partitions_to_merge;
				std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge_holder;
				for (size_t index = 0; index < this->input_.count(); index++) {
					auto cache_id = "input_" + std::to_string(index);
					if (not this->input_.get_cache(cache_id)->is_finished()) {
						auto input = std::move(this->input_.get_cache(cache_id)->pullFromCache());
						if (input) {
							partitions_to_merge.emplace_back(input->toBlazingTableView());
							partitions_to_merge_holder.emplace_back(std::move(input));
						}
					}
				}

				if (partitions_to_merge.empty()) {
					// noop
				} else if(partitions_to_merge.size() == 1) {
					this->output_.get_cache()->addToCache(std::move(partitions_to_merge_holder.front()));
				}	else {
					for (auto view : partitions_to_merge)
						ral::utilities::print_blazing_table_view(view);

					auto output = ral::operators::experimental::merge(partitions_to_merge, this->expression);

//					ral::utilities::print_blazing_table_view(output->toBlazingTableView());

					this->output_.get_cache()->addToCache(std::move(output));
				}
			}
			
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-DoubleSourceKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class AggregateAndSampleKernel : public kernel {
public:
	AggregateAndSampleKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->output_.add_port("output_a", "output_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type input = std::move(this->input_.get_cache()->pullFromCache());
			std::unique_ptr<ral::frame::BlazingTable> groupedTable;
			std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
			// WSM commenting this out because groupby is being refactored and this is getting deprecated
			// std::tie(groupedTable, partitionPlan) =
			// 	ral::operators::experimental::group_and_sample(input->toBlazingTableView(), this->expression, this->context.get());

			context->incrementQueryStep();
			this->output_["output_a"]->addToCache(std::move(groupedTable));
			if (context->getTotalNodes() > 1) {
				this->output_["output_b"]->addToCache(std::move(partitionPlan));
			}
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-AggregateAndSampleKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class AggregatePartitionKernel : public kernel {
public:
	AggregatePartitionKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->input_.add_port("input_a", "input_b");
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {
			frame_type groupedTable = std::move(this->input_["input_a"]->pullFromCache());
			std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions;

			if(ral::operators::experimental::is_aggregations_without_groupby(this->expression)){
				// aggregations_without_groupby does not need partitions
				this->output_["output_0"]->addToCache(std::move(groupedTable));
				return kstatus::proceed;
			}

			// WSM commenting this out because GroupBy was getting refactored and this will be deprecated
			// if (context->getTotalNodes() > 1) {
			// 	partitionPlan = std::move(this->input_["input_b"]->pullFromCache());
			// 	partitions = ral::operators::experimental::partition_group(partitionPlan->toBlazingTableView(),
			// 																														groupedTable->toBlazingTableView(),
			// 																														this->expression,
			// 																														this->context.get());
			// } else {
			// 	partitions =
			// 		ral::operators::experimental::partition_group({}, groupedTable->toBlazingTableView(), this->expression, this->context.get());
			// }
			// for(auto& partition : partitions)
			// 	ral::utilities::print_blazing_table_view(partition->toBlazingTableView());

			auto group_column_indices = ral::operators::experimental::get_group_column_indices(this->expression);

			// Split Partitions
			std::vector<std::unique_ptr<ral::frame::BlazingTable>> samples;
			for (auto i = 0; i < partitions.size(); i++) {
				auto part_samples = ral::distribution::sampling::experimental::generateSamplesFromRatio(ral::frame::BlazingTableView{partitions[i]->view().select(group_column_indices), partitions[i]->names()}, 0.25);
				samples.push_back(std::move(part_samples));
			}

			std::vector<ral::frame::BlazingTableView> samples_view;
			for (auto &&s : samples) {
				samples_view.push_back(s->toBlazingTableView());
			}
			auto concat_samples = ral::utilities::experimental::concatTables(samples_view);

			std::vector<cudf::order> orders(samples_view[0].num_columns(), cudf::order::ASCENDING);
			std::vector<cudf::null_order> null_orders(samples_view[0].num_columns(), cudf::null_order::AFTER);
			auto sorted_samples = cudf::experimental::sort(concat_samples->view(), orders, null_orders);

			// std::cout<< "SORTED SAMPLES: " <<std::endl;
			// for (auto &&c : sorted_samples->view())
			// {
			// 	cudf::test::print(c);
			// 	std::cout << std::endl;
			// }	

			cudf::size_type samples_rows = sorted_samples->view().num_rows();
			cudf::size_type pivots_size = samples_rows > 0 ? 3 /* How many subpartitions? */ : 0;

			int32_t step = samples_rows / (pivots_size + 1);
			auto sequence_iter = cudf::test::make_counting_transform_iterator(0, [step](auto i) { return int32_t(i * step) + step;});
			cudf::test::fixed_width_column_wrapper<int32_t> gather_map_wrapper(sequence_iter, sequence_iter + pivots_size);
			auto pivots = cudf::experimental::gather(sorted_samples->view(), gather_map_wrapper);
			
			// std::cout<< "PIVOTS: " <<std::endl;
			// for (auto &&c : pivots->view())
			// {
			// 	cudf::test::print(c);
			// 	std::cout << std::endl;
			// }	

			for (auto i = 0; i < partitions.size(); i++) {
				auto pivot_indexes = cudf::experimental::upper_bound(partitions[i]->view().select(group_column_indices),
																															pivots->view(),
																															orders,
																															null_orders);
		
				// std::cout<< "PIVOTS indices: " <<std::endl;
				// cudf::test::print(pivot_indexes->view());
				// std::cout << std::endl;
				
				auto host_pivot_col = cudf::test::to_host<cudf::size_type>(pivot_indexes->view());
				auto host_pivot_indexes = host_pivot_col.first;

				auto partitioned_data = cudf::experimental::split(partitions[i]->view(), host_pivot_indexes);

				// std::cout<< ">>>>>> TOTAL PARTITIONS : " << partitioned_data.size() << std::endl;
				std::string cache_id = "output_" + std::to_string(i);
				for (auto &&subpartition : partitioned_data) {
					this->output_[cache_id]->addToCache(
						std::make_unique<ral::frame::BlazingTable>(
							std::make_unique<cudf::experimental::table>(subpartition),
							partitions[i]->names())
						);
				}
			}

			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-AggregatePartitionKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class AggregateMergeStreamKernel : public kernel {
public:
	AggregateMergeStreamKernel(const std::string & queryString, std::shared_ptr<Context> context) {
		this->context = context;
		this->expression = queryString;
	}
	virtual kstatus run() {
		try {

			while (not this->input_.get_cache("input_0")->is_finished())
			{
				std::vector<ral::frame::BlazingTableView> partitions_to_merge;
				std::vector<std::unique_ptr<ral::frame::BlazingTable>> partitions_to_merge_holder;
				for (size_t index = 0; index < this->input_.count(); index++) {
					auto cache_id = "input_" + std::to_string(index);
					if (not this->input_.get_cache(cache_id)->is_finished()) {
						auto input = std::move(this->input_.get_cache(cache_id)->pullFromCache());
						if (input) {
							partitions_to_merge.emplace_back(input->toBlazingTableView());
							partitions_to_merge_holder.emplace_back(std::move(input));
						}
					}
				}

				if (partitions_to_merge.empty()) {
					// noop
				} else if(partitions_to_merge.size() == 1) {
					// std::cout << ">>>>> ONLY 1 PARTITION NOTHING TO MERGE : " << std::endl;
					this->output_.get_cache()->addToCache(std::move(partitions_to_merge_holder.front()));
				}	else {
					// std::cout << ">>>>> merge-parts: " << std::endl;
					for (auto view : partitions_to_merge)
						ral::utilities::print_blazing_table_view(view);

					// WSM commenting this out because groupby is getting some refactoring and this should be getting deprecated soon
					// auto output = ral::operators::experimental::merge_group(partitions_to_merge, this->expression, this->context.get());

					// ral::utilities::print_blazing_table_view(output->toBlazingTableView());

					// this->output_.get_cache()->addToCache(std::move(output));
				}
			}
			
			return kstatus::proceed;
		} catch (std::exception &e) {
			std::cerr << "Exception-AggregateMergeStreamKernel: " << e.what() << std::endl;
		}
		return kstatus::stop;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
};

class print : public kernel {
public:
	print() : kernel() { ofs = &(std::cout); }
	print(std::ostream & stream) : kernel() { ofs = &stream; }
	virtual kstatus run() {
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
	BindableTableScanKernel(std::string expr, ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
	: kernel(), expr(expr), context(context), loader(loader), schema(schema)
	{}

	virtual kstatus run() {
		auto output = ral::processor::process_table_scan(loader, expr, schema, context.get());
		this->output_.get_cache()->addToCache(std::move(output));
		return (kstatus::proceed);
	}

private:
	std::shared_ptr<Context>context;
	ral::io::data_loader loader;
	ral::io::Schema  schema;
	std::string expr;
};

class TableScanKernel : public kernel {
public:
	TableScanKernel(ral::io::data_loader &loader, ral::io::Schema & schema, std::shared_ptr<Context> context)
		: kernel(), context(context), loader(loader), schema(schema)
	{}

	virtual kstatus run() {
		CodeTimer blazing_timer;
		blazing_timer.reset();  // doing a reset before to not include other calls to evaluate_split_query
		auto table = loader.load_data(context.get(), {}, schema);
		context->incrementQueryStep();
		int num_rows = table->num_rows();
		Library::Logging::Logger().logInfo(blazing_timer.logDuration(*context, "evaluate_split_query load_data", "num rows", num_rows));
		blazing_timer.reset();
		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	std::shared_ptr<Context> context;
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

		std::unique_ptr<ral::frame::BlazingTable> execute_plan() {
			if (children.size() == 0) {
				// base case
				std::shared_ptr<ral::cache::CacheMachine> cache = create_cache_machine(cache_settings{.type = CacheType::NON_WAITING});
				auto kernel_id = std::to_string(kernel_unit->get_id());
				kernel_unit->output_.register_cache(kernel_id, cache);
				kernel_unit->run();
				kernel_unit->output_.finish();
				return kernel_unit->output_.get_cache(kernel_id)->pullFromCache();
			} else {
				if(children.size() == 2) {				
					frame_type input_a;
					BlazingThread t1([this, &input_a]() mutable{
						input_a = this->children[0]->execute_plan();
					}); 
					
					frame_type input_b;
					BlazingThread t2([this, &input_b]() mutable{
						input_b = this->children[1]->execute_plan(); 
					});
					t1.join();
					t2.join();
					
					std::shared_ptr<ral::cache::CacheMachine> source_cache_a = create_cache_machine(cache_settings{.type = CacheType::NON_WAITING});
					source_cache_a->addToCache(std::move(input_a));
					source_cache_a->finish();

					std::shared_ptr<ral::cache::CacheMachine> source_cache_b = create_cache_machine(cache_settings{.type = CacheType::NON_WAITING});
					source_cache_b->addToCache(std::move(input_b));
					source_cache_b->finish();

					std::shared_ptr<ral::cache::CacheMachine> sink_cache = create_cache_machine(cache_settings{.type = CacheType::NON_WAITING});
					auto kernel_id = std::to_string(kernel_unit->get_id());
					kernel_unit->input_.register_cache("input_a", source_cache_a);
					kernel_unit->input_.register_cache("input_b", source_cache_b);
					kernel_unit->output_.register_cache(kernel_id, sink_cache);
					kernel_unit->run();
					kernel_unit->output_.finish();
					return kernel_unit->output_.get_cache(kernel_id)->pullFromCache();
				} else if(children.size() == 1) {
					auto current_input = children[0]->execute_plan();
					std::shared_ptr<ral::cache::CacheMachine> source_cache = create_cache_machine(cache_settings{.type = CacheType::NON_WAITING});
					source_cache->addToCache(std::move(current_input));
					source_cache->finish();

					std::shared_ptr<ral::cache::CacheMachine> sink_cache = create_cache_machine(cache_settings{.type = CacheType::NON_WAITING});
					auto kernel_id = std::to_string(kernel_unit->get_id());
					kernel_unit->input_.register_cache(kernel_id, source_cache);
					kernel_unit->output_.register_cache(kernel_id, sink_cache);
					kernel_unit->run();
					kernel_unit->output_.finish();
					return kernel_unit->output_.get_cache(kernel_id)->pullFromCache();
				}
			}
		}
	} root;
	std::shared_ptr<Context> context;
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::vector<std::string> table_names;
	const bool transform_operators_bigger_than_gpu = false;

	void expr_tree_from_json(boost::property_tree::ptree const& p_tree, expr_tree_processor::node * root_ptr, int level) {
		auto expr = p_tree.get<std::string>("expr", "");
		// for(int i = 0; i < level*2 ; ++i) {
		// 	std::cout << " ";
		// }
		// std::cout << expr << std::endl;
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = make_kernel(expr);
		for (auto &child : p_tree.get_child("children")) {
			auto child_node_ptr = std::make_shared<expr_tree_processor::node>();
			root_ptr->children.push_back(child_node_ptr);
			expr_tree_from_json(child.second, child_node_ptr.get(), level + 1);
		}
	}

	std::shared_ptr<kernel> make_kernel(std::string expr) {
		std::shared_ptr<kernel> k;
		auto kernel_context = this->context->clone();
		if ( is_project(expr) ) {
			k = std::make_shared<ProjectKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::ProjectKernel);
		} else if ( is_filter(expr) ) {
			k = std::make_shared<FilterKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::FilterKernel);
		} else if ( is_join(expr) ) {
			k = std::make_shared<JoinKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::JoinKernel);
		} else if ( is_union(expr) ) {
			k = std::make_shared<UnionKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::UnionKernel);
		} else if ( is_sort(expr) ) {
			k = std::make_shared<SortKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortKernel);
		} else if ( is_merge(expr) ) {
			k = std::make_shared<MergeStreamKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeStreamKernel);
		} else if ( is_partition(expr) ) {
			k = std::make_shared<PartitionKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionKernel);
		} else if ( is_sort_and_sample(expr) ) {
			k = std::make_shared<SortAndSampleKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleKernel);
		} else if ( is_aggregate(expr) ) {
			k = std::make_shared<AggregateKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::AggregateKernel);
		} else if ( is_aggregate_merge(expr) ) {
			k = std::make_shared<AggregateMergeStreamKernel>(expr, kernel_context);
			k->set_type_id(kernel_type::AggregateMergeStreamKernel);
		} else if ( is_aggregate_partition(expr) ) {
			k = std::make_shared<AggregatePartitionKernel>(expr, kernel_context);
			k->set_type_id(kernel_type::AggregatePartitionKernel);
		} else if ( is_aggregate_and_sample(expr) ) {
			k = std::make_shared<AggregateAndSampleKernel>(expr, kernel_context);
			k->set_type_id(kernel_type::AggregateAndSampleKernel);
		} else if ( is_logical_scan(expr) ) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<TableScanKernel>(*loader, schema, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::TableScanKernel);
		} else if (is_bindable_scan(expr)) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<BindableTableScanKernel>(expr, *loader, schema, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::BindableTableScanKernel);
		}
		return k;
	}

	std::unique_ptr<ral::frame::BlazingTable> execute_plan(std::string json) {
		try {
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			expr_tree_from_json(p_tree, &this->root, 0);
		} catch (std::exception & e) {
			std::cerr << e.what() <<  std::endl;
		}
		// if (transform_operators_bigger_than_gpu) {
		// 	transform_operator_bigger_than_gpu(nullptr, &this->root);
		// }

		if (this->root.kernel_unit != nullptr) {
			return this->root.execute_plan();
		}
		return nullptr;
	}
	void transform_operator_bigger_than_gpu(node* parent, node * current) {
		if (current->kernel_unit->get_type_id() == kernel_type::SortKernel) {
			auto merge_expr = current->expr;
			auto partition_expr = current->expr;
			auto sort_and_sample_expr = current->expr;
			StringUtil::findAndReplaceAll(merge_expr, "LogicalSort", LOGICAL_MERGE_TEXT);
			StringUtil::findAndReplaceAll(partition_expr, "LogicalSort", LOGICAL_PARTITION_TEXT);
			StringUtil::findAndReplaceAll(sort_and_sample_expr, "LogicalSort", LOGICAL_SORT_AND_SAMPLE_TEXT);

			auto merge = std::make_shared<node>();
			merge->expr = merge_expr;
			merge->level = current->level;
			merge->kernel_unit = make_kernel(merge_expr);

			auto partition = std::make_shared<node>();
			partition->expr = partition_expr;
			partition->level = current->level;
			partition->kernel_unit = make_kernel(partition_expr);
			merge->children.push_back(partition);

			auto ssample = std::make_shared<node>();
			ssample->expr = sort_and_sample_expr;
			ssample->level = current->level;
			ssample->kernel_unit = make_kernel(sort_and_sample_expr);
			partition->children.push_back(ssample);

			ssample->children = current->children;

			if (parent == nullptr) { // root is sort
				// new root
				current->expr = merge->expr;
				current->kernel_unit = merge->kernel_unit;
				current->children = merge->children;
			} else {
				auto it = std::find_if(parent->children.begin(), parent->children.end(), [current] (std::shared_ptr<node> tmp) {
					return tmp->kernel_unit->get_id() == current->kernel_unit->get_id();
				});
				parent->children.erase( it );
				parent->children.push_back(merge);
			}
		}	else if(current->kernel_unit->get_type_id() == kernel_type::AggregateKernel) {
			auto merge_expr = current->expr;
			auto partition_expr = current->expr;
			auto aggregate_and_sample_expr = current->expr;
			StringUtil::findAndReplaceAll(merge_expr, "LogicalAggregate", LOGICAL_AGGREGATE_MERGE_TEXT);
			StringUtil::findAndReplaceAll(partition_expr, "LogicalAggregate", LOGICAL_AGGREGATE_PARTITION_TEXT);
			StringUtil::findAndReplaceAll(aggregate_and_sample_expr, "LogicalAggregate", LOGICAL_AGGREGATE_AND_SAMPLE_TEXT);

			auto merge = std::make_shared<node>();
			merge->expr = merge_expr;
			merge->level = current->level;
			merge->kernel_unit = make_kernel(merge_expr);

			auto partition = std::make_shared<node>();
			partition->expr = partition_expr;
			partition->level = current->level;
			partition->kernel_unit = make_kernel(partition_expr);
			merge->children.push_back(partition);

			auto ssample = std::make_shared<node>();
			ssample->expr = aggregate_and_sample_expr;
			ssample->level = current->level;
			ssample->kernel_unit = make_kernel(aggregate_and_sample_expr);
			partition->children.push_back(ssample);

			ssample->children = current->children;

			if (parent == nullptr) { // root is sort
				// new root
				current->expr = merge->expr;
				current->kernel_unit = merge->kernel_unit;
				current->children = merge->children;
			} else {
				auto it = std::find_if(parent->children.begin(), parent->children.end(), [current] (std::shared_ptr<node> tmp) {
					return tmp->kernel_unit->get_id() == current->kernel_unit->get_id();
				});
				parent->children.erase( it );
				parent->children.push_back(merge);
			}
		}	else if (current) {
			for (auto& child : current->children) {
				transform_operator_bigger_than_gpu(current, child.get());
			}
		}
	}

	void tree_show(node* node_ptr){
		for (auto &&c : node_ptr->children)
		{
			tree_show(c.get());
		}
		std::cout<< ">>>>> type kernel - id : "<< (int)(node_ptr->kernel_unit->get_type_id()) << " - "<<(int)(node_ptr->kernel_unit->get_id()) <<std::endl;
	}

	ral::cache::graph build_graph(std::string json) {
		try {
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			expr_tree_from_json(p_tree, &this->root, 0);

		} catch (std::exception & e) {
			std::cerr << e.what() <<  std::endl;
		}

		if (transform_operators_bigger_than_gpu) {
			transform_operator_bigger_than_gpu(nullptr, &this->root);
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
				graph +=  *child->kernel_unit >> (*parent->kernel_unit)[port_name];
			} else {
				auto a = child->kernel_unit->get_type_id();
				auto b = parent->kernel_unit->get_type_id();
				if ((child->kernel_unit->get_type_id() == kernel_type::SortAndSampleKernel &&	parent->kernel_unit->get_type_id() == kernel_type::PartitionKernel) ||
						(child->kernel_unit->get_type_id() == kernel_type::AggregateAndSampleKernel &&	parent->kernel_unit->get_type_id() == kernel_type::AggregatePartitionKernel)) {
					graph += (*(child->kernel_unit))["output_a"] >> (*(parent->kernel_unit))["input_a"];
					graph += (*(child->kernel_unit))["output_b"] >> (*(parent->kernel_unit))["input_b"];
				} else if ((child->kernel_unit->get_type_id() == kernel_type::PartitionKernel && parent->kernel_unit->get_type_id() == kernel_type::MergeStreamKernel) ||
									 (child->kernel_unit->get_type_id() == kernel_type::AggregatePartitionKernel && parent->kernel_unit->get_type_id() == kernel_type::AggregateMergeStreamKernel))
				{
					auto cache_machine_config =	cache_settings{.type = CacheType::FOR_EACH, .num_partitions = this->context->getTotalNodes()};
					graph += link(*child->kernel_unit, *parent->kernel_unit, cache_machine_config);
				} else if ((child->kernel_unit->get_type_id() == kernel_type::AggregateMergeStreamKernel ||	child->kernel_unit->get_type_id() == kernel_type::MergeStreamKernel))
				{
					auto cache_machine_config =	cache_settings{.type = CacheType::CONCATENATING, .num_partitions = this->context->getTotalNodes()};
					graph += link(*child->kernel_unit, *parent->kernel_unit, cache_machine_config);
				} else {
					graph +=  *child->kernel_unit >> (*parent->kernel_unit);
				}				
			}
		}
	}

};

} // namespace parser


}  // namespace cache
}  // namespace ral
