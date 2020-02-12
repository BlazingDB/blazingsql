
#include "data_builder.h"
#include "execution_graph/logic_controllers/LogicalProject.h"
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <stack>

struct CacheMachineTest : public cudf::test::BaseFixture {
  CacheMachineTest() {

  }

  ~CacheMachineTest() {
  }

};

 TEST_F(CacheMachineTest, CacheMachineTest) {
     unsigned long long  gpuMemory = 1024;
     std::vector<unsigned long long > memoryPerCache = {INT_MAX};
     std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
     ral::cache::CacheMachine cacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes);

     for (int i = 0; i < 10; ++i) {
         auto table = build_custom_table();
         std::cout << ">> " << i << "|" <<  table->sizeInBytes() << std::endl;
         cacheMachine.addToCache(std::move(table));
         if (i % 5 == 0) {
             auto cacheTable = cacheMachine.pullFromCache();
         }
     }
     std::this_thread::sleep_for (std::chrono::seconds(1));
 }

std::shared_ptr<ral::cache::CacheMachine>  createSourceCacheMachine() {
    unsigned long long  gpuMemory = 1024;
    std::vector<unsigned long long > memoryPerCache = {INT_MAX};
    std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
    auto source =  std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
    auto table = build_custom_table();
    source->addToCache(std::move(table));
    return source;
}

std::shared_ptr<ral::cache::CacheMachine>  createSinkCacheMachine() {
    unsigned long long  gpuMemory = 1024;
    std::vector<unsigned long long > memoryPerCache = {INT_MAX};
    std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
    return std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
}


 TEST_F(CacheMachineTest, FilterTest){
     using ProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable> (
     const ral::frame::BlazingTableView & table,
     const std::string & query_part,
     blazingdb::manager::experimental::Context * context);

     std::shared_ptr<ral::cache::CacheMachine> cacheSource = createSourceCacheMachine();
     std::shared_ptr<ral::cache::CacheMachine> cacheSink  = createSinkCacheMachine();
     ProcessorFunctor *process_project = &ral::processor::process_filter;
     std::string queryString = "BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])";
     blazingdb::manager::experimental::Context * context = nullptr;
     int numWorkers = 1;
     ral::cache::ProcessMachine<ProcessorFunctor> processor(cacheSource, cacheSink, process_project, queryString, context, numWorkers);

     std::cout << ">> processor.run()\n";
         processor.run();
     std::cout << "<<> processor.run()\n";
     std::this_thread::sleep_for (std::chrono::seconds(1));
 }

 TEST_F(CacheMachineTest, ProjectTest) {
     using ProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable> (
             const ral::frame::BlazingTableView & table,
             const std::string & query_part,
             blazingdb::manager::experimental::Context * context);

     std::shared_ptr<ral::cache::CacheMachine> cacheSource = createSourceCacheMachine();
     std::shared_ptr<ral::cache::CacheMachine> cacheSink  = createSinkCacheMachine();
     ProcessorFunctor *process_project = &ral::processor::process_project;
     std::string queryString = "LogicalProject(INT64=[$0], INT32=[$1], FLOAT64=[$2])";
     blazingdb::manager::experimental::Context * context = nullptr;
     int numWorkers = 1;
     ral::cache::ProcessMachine<ProcessorFunctor> processor(cacheSource, cacheSink, process_project, queryString, context, numWorkers);

     std::cout << ">> processor.run()\n";
     processor.run();
     std::cout << "<<> processor.run()\n";
     std::this_thread::sleep_for (std::chrono::seconds(1));
 }

std::shared_ptr<ral::cache::WaitingCacheMachine>  createSourceCacheMachineOneColumn() {
    unsigned long long  gpuMemory = 1024;
    std::vector<unsigned long long > memoryPerCache = {INT_MAX};
    std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
    auto source =  std::make_shared<ral::cache::WaitingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
    auto table = build_custom_one_column_table();
    source->addToCache(std::move(table));
    return source;
} 


TEST_F(CacheMachineTest, LogicalJoinTest) {
     using ProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable> (
            blazingdb::manager::experimental::Context *,
            const ral::frame::BlazingTableView & ,
            const ral::frame::BlazingTableView & ,
            const std::string & );

   std::shared_ptr<ral::cache::WaitingCacheMachine> cacheLeftSource = createSourceCacheMachineOneColumn();
   std::shared_ptr<ral::cache::WaitingCacheMachine> cacheRightSource = createSourceCacheMachineOneColumn();
   std::shared_ptr<ral::cache::CacheMachine> cacheSink  = createSinkCacheMachine();
   ProcessorFunctor *process_project = &ral::processor::process_logical_join;

   std::string queryString =  "LogicalJoin(condition=[=($1, $0)], joinType=[inner])";
   using blazingdb::manager::experimental::Context;
   using blazingdb::transport::experimental::Node;
   using blazingdb::transport::experimental::Address;
   std::vector<Node> contextNodes;
   auto address = Address::TCP("127.0.0.1", 8089, 0);
   contextNodes.push_back(Node(address));
   uint32_t ctxToken = 123;
   Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

   int numWorkers = 1;

    ral::cache::ProcessMachine<ProcessorFunctor> processor(cacheLeftSource, cacheRightSource, cacheSink, process_project, queryString, &queryContext, numWorkers);

    std::cout << ">> processor.run()\n";
    processor.run();
    std::cout << "<<> processor.run()\n";
    std::this_thread::sleep_for (std::chrono::seconds(1));
}
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
	static void add_port(PORT & port) {
 		return;
	}
};
class port;

/** continue recursion **/
template <class PORT, class PORTNAME, class... PORTNAMES>
struct port_helper<PORT, PORTNAME, PORTNAMES...> {
	static void add_port(PORT & port, PORTNAME && portname, PORTNAMES &&... portnames) {
		port.add_port(portname);
		port_helper<PORT, PORTNAMES...>::add_port(port, std::forward<PORTNAMES>(portnames)...);
		return;
	}
};
/** kicks off recursion for adding ports **/
template <class PORT, class... PORTNAMES>
static void kick_port_helper(PORT & port, PORTNAMES &&... ports) {
	port_helper<PORT, PORTNAMES...>::add_port(port, std::forward<PORTNAMES>(ports)...);
	return;
}
using frame_type = std::unique_ptr<ral::frame::BlazingTable>;
class kernel;

class port {
public:
	port(kernel * const k){
		this->kernel_ = k;
	}

	virtual ~port() = default;

	template <class... PORTNAMES>
	void addPort(PORTNAMES &&... ports) {
		kick_port_helper<port, PORTNAMES...>((*this), std::forward<PORTNAMES>(ports)...);
	}

	size_t count() const {
		return container_.size();
	}

	void add_port(std::string port_name);

	std::shared_ptr<WaitingCacheMachine> & operator[](const std::string & port_name) {
		auto it = container_.find(port_name);
		return it->second;
	}
private:
	kernel* kernel_;
	std::map<std::string, std::shared_ptr<WaitingCacheMachine> > container_;
};

class kernel {
public:
	kernel() : kernel_id(kernel::kernel_count) {
		kernel::kernel_count++;
		input.add_port("input");
		input.add_port("output");
	}

	virtual ~kernel() = default;

	virtual kstatus run() {
		//TODO run kernel!
	}

	kernel & operator[](const std::string && portname) {
		// TODO !! get source or target.

//		output[portname]->
	}
	std::shared_ptr<WaitingCacheMachine> getCacheMachine() {
		// TODO finish this
		return nullptr;
	}
	kernel * clone() { return nullptr; }
	std::size_t get_id() { return (kernel_id); }

	static std::size_t kernel_count;

public:
	port input{this};
	port output{this};
	const std::size_t kernel_id;
	bool execution_done = false;
};

std::size_t kernel::kernel_count(0);

void port::add_port(std::string port_name) {
	container_[port_name] = kernel_->getCacheMachine();
}


class kernel_pair_t {
	/** container with ref wrapper **/
	using kernel_pair_t_container = std::vector<std::reference_wrapper<kernel>>;

public:
	/**
	 * define iterator type publicly
	 */
	using kernel_iterator_type = kernel_pair_t_container::iterator;
	/**
	 * endpoint ret type is a std::pair with
	 * two iterators, one for begin, the second
	 * for end
	 */
	using endpoint_ret_type = std::pair<kernel_iterator_type, kernel_iterator_type>;
	/**
	 * define a size type that matches the container
	 * type, whatever that container type may end
	 * up being
	 */
	using size_type = typename kernel_pair_t_container::size_type;

	kernel_pair_t() {
		source.reserve(2);
		destination.reserve(2);
	}
	kernel_pair_t(kernel * const src, kernel * const dst) {
		source.emplace_back(*src);
		destination.emplace_back(*dst);
	}

	kernel_pair_t::endpoint_ret_type getSrc() { return (endpoint_ret_type(source.begin(), source.end())); }

	kernel_pair_t::size_type getSrcSize() noexcept { return (source.size()); }


	kernel_pair_t::endpoint_ret_type getDst() { return (endpoint_ret_type(destination.begin(), destination.end())); }

	kernel_pair_t::size_type getDstSize() noexcept { return (destination.size()); }


	void addSrc(kernel & k) noexcept { source.emplace_back(k); }

	void addDst(kernel & k) noexcept { destination.emplace_back(k); }

	void clearSrc() noexcept { source.clear(); }

	void clearDst() noexcept { destination.clear(); }

private:
	/** type is determined by using type aliases above the first public: **/
	kernel_pair_t_container source;
	kernel_pair_t_container destination;
};
using core_id_t = std::int64_t;

class kpair {
public:
	kpair(kernel & a, kernel & b) {
		src = &a;
		dst = &b;
	}
	kernel * src = nullptr;
	kernel * dst = nullptr;
};

kpair & operator>>(kernel & a, kernel & b) {
	auto pair = new kpair(a, b);
	return *pair;
}

using split_stack_t = std::stack<std::size_t>;
using group_t = std::vector<kernel *>;
using up_group_t = std::unique_ptr<group_t>;
using kernels_t = std::vector<up_group_t>;

namespace order {
enum spec : std::uint8_t { in = 0, out = 1 };
}


class graph{
public:
	kernel_pair_t operator += (kpair & p) {
		kernel_pair_t ret_kernel_pair;
		this->add_edge(p.src, p.dst);
		return ret_kernel_pair;
	}

	void execute() {

	}

	size_t find_node(kernel* key) {
		auto it = container_.find(key->get_id());
		if (it != container_.end()) {
			return key->get_id();
		}
		return -1;
	}
	size_t add_node(kernel* k) {
		container_[k->get_id()] = k;
		return k->get_id();
	}
	void add_edge(kernel* from, kernel *to){
		add_node(from);
		add_node(to);
		edges_[from->get_id()].insert(to->get_id());
	}
	std::set<std::size_t> get_neighbours(kernel* from) {
		return edges_[from->get_id()];
	}

private:
	std::map<std::size_t, kernel*> 	container_;
	std::map<std::size_t, std::set<std::size_t>> 	edges_;
};

class FilterKernel : public kernel {
public:
	FilterKernel() : kernel() {
		input.addPort("input");
		output.addPort("output");
	}
	virtual kstatus run() {
		frame_type a = std::move(input["input"]->pullFromCache());
		//      auto out = output[ "output" ];
		//      out = ral::processor::process_filter(a, b, expression, context);
		return kstatus::proceed;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};
class ProjectKernel : public kernel {
public:
	ProjectKernel() : kernel() {
		input.addPort("input");
		output.addPort("output");
	}
	virtual kstatus run() {
		frame_type a = std::move(input["input"]->pullFromCache());
		//      auto out = output[ "output" ];
		//      out = ral::processor::process_filter(a, b, expression, context);
		return kstatus::proceed;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};
class JoinKernel : public kernel {
public:
	JoinKernel() : kernel() {
		input.addPort("input_a", "input_b");
		output.addPort("output");
	}
	virtual kstatus run() {
		frame_type b = std::move(input["input_b"]->pullFromCache());
		frame_type a = std::move(input["input_a"]->pullFromCache());
		//      auto c = output[ "output" ];
		//      c = ral::processor::process_join(a, b, expression, context);
		return kstatus::proceed;
	}

private:
	blazingdb::manager::experimental::Context * context;
	std::string expression;
};

class print : public kernel {
public:
	print() : kernel() {
		input.addPort("in");
		ofs = &(std::cout);
	}
	print(std::ostream & stream) : kernel() {
		input.addPort("in");
		ofs = &stream;
	}
	virtual kstatus run() {
		const std::string delim = "\n";
		std::lock_guard<std::mutex> lg(print::print_lock);
		auto & input_port((this)->input["in"]);
		//      auto &data( input_port.template peek< T >() );
		//      *((this)->ofs) << data << delim;
		//      input_port.unpeek();
		//      input_port.recycle( 1 );
		return kstatus::proceed;
	}

protected:
	std::ostream * ofs = nullptr;
	static std::mutex print_lock;
};
std::mutex print::print_lock{};

namespace test {
class generate : public kernel {
public:
	generate(std::int64_t count = 1000) : kernel(), count(count) { output.addPort("number_stream"); }
	virtual kstatus run() {
		//      if( count-- > 1 )
		//      {
		//          auto &ref( output[ "number_stream" ].template allocate< T >() );
		//          ref = static_cast< T >( (this)->count );
		//          output[ "number_stream"].send();
		//          return( kstatus::proceed );
		//      }
		//      /** else **/
		//      auto &ref( output[ "number_stream" ].template allocate< T >() );
		//      ref = static_cast< T >( (this)->count );
		//      output[ "number_stream" ].send( signal::eof );
		return (kstatus::stop);
	}

private:
	std::int64_t count;
};
} // end namespace

TEST_F(CacheMachineTest, JoinWorkFlowTest)  {
	using GeneratorKernel = ral::cache::test::generate;
	using PrinterKernel = ral::cache::print;
	GeneratorKernel a(10), b(10);
	JoinKernel s;
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
}

}//end namespace
}//end namespace
