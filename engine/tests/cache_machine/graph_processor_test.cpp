#include "data_builder.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/utilities/DebuggingUtils.h>
#include <stack>


struct GraphProcessorTest : public cudf::test::BaseFixture {
	GraphProcessorTest() {}

	~GraphProcessorTest() {}
};

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

std::size_t kernel::kernel_count(0);

void port::register_port(std::string port_name) { cache_machines_[port_name] = nullptr; }

std::shared_ptr<CacheMachine> & port::get_cache(const std::string & port_name) {
	if(port_name.length() == 0) {
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

kpair & operator>>(kernel & a, kernel & b) {
	auto pair = new kpair(a, b);
	return *pair;
}

kpair & operator>>(kernel & a, kernel_pair b) {
	auto pair = new kpair(a, std::move(b));
	return *pair;
}
kpair & operator>>(kernel_pair a, kernel & b) {
	auto pair = new kpair(std::move(a), b);
	return *pair;
}

kpair & link(kernel & a, kernel & b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
	auto pair = new kpair(a, b, cache_machine);
	return *pair;
}

kpair & link(kernel & a, kernel_pair b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
	auto pair = new kpair(a, std::move(b), cache_machine);
	return *pair;
}
kpair & link(kernel_pair a, kernel & b, std::shared_ptr<CacheMachine> cache_machine = nullptr) {
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
				std::cout << " " << target_id << std::endl;
				auto target = get_node(target_id);
				auto edge_id = std::make_pair(source_id, target_id);
				if(visited.find(edge_id) == visited.end()) {
					visited.insert(edge_id);
					Q.push_back(target_id);
					std::thread t([this, source, target, edge] {
					});

					auto state = source->run();
					if (state == kstatus::proceed) {
						source->output_.finish();
					}
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
		return std::make_shared<ral::cache::WaitingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
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
		auto output = processor(input->toBlazingTableView(), expression, context);
		this->output_.get_cache()->addToCache(std::move(output));
		return kstatus::proceed;
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
std::mutex print::print_lock{};

namespace test {
class generate : public kernel {
public:
	generate(std::int64_t count = 1000) : kernel(), count(count) {}
	virtual kstatus run() {
		auto table = build_custom_one_column_table();
		this->output_.get_cache()->addToCache(std::move(table));
		return (kstatus::proceed);
	}

private:
	std::int64_t count;
};

class file_reader_kernel : public kernel {
public:
	file_reader_kernel(std::vector<std::string> file_paths) : kernel(), file_paths(file_paths) {}

	virtual kstatus run() {
		for(auto file_path : file_paths) {
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

TEST_F(GraphProcessorTest, JoinWorkFlowTest) {
	GeneratorKernel a(10), b(10);

	std::string expression = "LogicalJoin(condition=[=($1, $0)], joinType=[inner])";
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};
	JoinKernel s(expression, &queryContext);
	PrinterKernel print;
	ral::cache::graph g;
	try {
		g += a >> s["input_a"];
		g += b >> s["input_b"];
		g += s >> print;
		g.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}


TEST_F(GraphProcessorTest, ComplexWorkFlowTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	GeneratorKernel a(10), b(10);
	FilterKernel filterA("BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])", &queryContext);
	FilterKernel filterB("BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])", &queryContext);
	JoinKernel join("LogicalJoin(condition=[=($1, $0)], joinType=[inner])", &queryContext);
	ProjectKernel project("LogicalProject(INT64=[$0])", &queryContext);

	PrinterKernel print;
	ral::cache::graph m;
	try {
		m += a >> filterA;
		m += b >> filterB;
		m += filterA >> join["input_a"];
		m += filterB >> join["input_b"];
		m += join >> project;
		m += project >> print;
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}
// 20 files.
// -> data_provider (LocalDataFile?)
//     	->
//			->

std::shared_ptr<ral::cache::CacheMachine> create_concatenating_machine() {
	unsigned long long gpuMemory = 1024;
	std::vector<unsigned long long> memoryPerCache = {INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
	return std::make_shared<ral::cache::ConcatenatingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
}


//sql: select c_custkey, c_nationkey, c_acctbal from orders as o inner join customer as c on o.o_custkey = c.c_custkey where o.o_orderkey < 100

//# LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])
//#   LogicalFilter(condition=[<($0, 100)])
//#     LogicalJoin(condition=[=($1, $9)], joinType=[inner])
//#       LogicalTableScan(table=[[main, orders]])
//#       LogicalTableScan(table=[[main, customer]])

//# DEBUG: com.blazingdb.calcite.application.RelationalAlgebraGenerator - optimized
//# LogicalProject(c_custkey=[$1], c_nationkey=[$2], c_acctbal=[$3])
//#   LogicalJoin(condition=[=($0, $1)], joinType=[inner])
//#     LogicalProject(o_custkey=[$1])
//#       BindableTableScan(table=[[main, orders]], filters=[[<($0, 100)]], projects=[[0, 1]], aliases=[[$f0, o_custkey]])
//#     BindableTableScan(table=[[main, customer]], projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey, c_acctbal]])
TEST_F(GraphProcessorTest, IOWorkFlowTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	std::string folder_path = "/home/aocsa/tpch/DataSet5Part100MB/";
	int n_files = 1; // not limited to use just one file. See cudf issue: https://github.com/rapidsai/cudf/issues/4055
	std::vector<std::string> order_path_list;
	std::vector<std::string> customer_path_list;
	for (int index = 0; index < n_files; index++){
		auto filepath = folder_path + "orders_" + std::to_string(index) + "_0.parquet";
		order_path_list.push_back(filepath);
		filepath = folder_path + "customer_" + std::to_string(index) + "_0.parquet";
		customer_path_list.push_back(filepath);
	}

	TableScanKernel order_generator(order_path_list);
	TableScanKernel customer_generator(customer_path_list);
	FilterKernel filter("LogicalFilter(condition=[<($0, 100)])", &queryContext);
	JoinKernel join("LogicalJoin(condition=[=($1, $9)], joinType=[inner])", &queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])", &queryContext);

	PrinterKernel print;
	ral::cache::graph m;
	try {
		auto concatenating_machine_1 = create_concatenating_machine();
		auto concatenating_machine_2 = create_concatenating_machine();
		m += link(order_generator, join["input_a"], concatenating_machine_1);
		m += link(customer_generator, join["input_b"], concatenating_machine_2);
		m += join >> filter;
		m += filter >> project;
		m += project >> print;

		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

//			-> sample
// (cache)     			  -> partition ->
//			->  sort
//
//

}  // namespace cache
}  // namespace ral
