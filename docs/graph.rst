Execution Engine
================

The execution engine is a graph composed of kernels which are connected by ports.

When a user runs a query using `bc.sql(query)`, that query is sent to Apache Calcite
where it is parsed into relational algebra and then optimized. That optimized
algebra comes back into python and can always be viewed by calling
`bc.explain(query)`. The optimized relational algebra is sent via dask to each
of its workers along with sources of information (cudfs, or files).

On each worker the relational algebra is converted into a physical plan. Every
relational algebra step maps to 1 or more physical plan steps. That physical
plan is then used to construct an execution graph where every step in the
physical plan corresponds to a kernel.

Relational algebra
------------------


The BlazingSQL engine executes a relational algebra plan. This plan is initially
created by Apache Calcite, which in turn receives a SQL query.
The initial relational algebra is converted into an physical plan,
which is effectively a modified version of the original relational algebra plan,
wherein some of the relational algebra steps is expanded into multiple steps.

SQL
^^^
::

    select o_custkey, sum(o_totalprice) from orders where o_orderkey < 10 group by o_custkey

Relational Algebra
^^^^^^^^^^^^^^^^^^
::

    LogicalProject(o_custkey=[$0], EXPR$1=[CASE(=($2, 0), null:DOUBLE, $1)])
      LogicalAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
        LogicalProject(o_custkey=[$1], o_totalprice=[$2])
          BindableTableScan(table=[[main, orders]], filters=[[<($0, 10)]], projects=[[0, 1, 3]], aliases=[[$f0, o_custkey, o_totalprice]])

Physical Plan Single GPU
^^^^^^^^^^^^^^^^^^^^^^^^
::

    LogicalProject(o_custkey=[$0], EXPR$1=[CASE(=($2, 0), null:DOUBLE, $1)])
      MergeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
        ComputeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
          LogicalProject(o_custkey=[$1], o_totalprice=[$2])
            BindableTableScan(table=[[main, orders]], filters=[[<($0, 10)]], projects=[[0, 1, 3]], aliases=[[$f0, o_custkey, o_totalprice]])

Physical Plan Multi GPU
^^^^^^^^^^^^^^^^^^^^^^^
::

    LogicalProject(o_custkey=[$0], EXPR$1=[CASE(=($2, 0), null:DOUBLE, $1)])
      MergeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
        DistributeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
          ComputeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
            LogicalProject(o_custkey=[$1], o_totalprice=[$2])
              BindableTableScan(table=[[main, orders]], filters=[[<($0, 10)]], projects=[[0, 1, 3]], aliases=[[$f0, o_custkey, o_totalprice]])


The conversion of the relational algebra gets done by the function ``transform_json_tree`` in 
:blazing_repo:`PhysicalPlanGenerator.h</engine/src/execution_graph/logic_controllers/PhysicalPlanGenerator.h>`. 
This function gets called by ``build_batch_graph``.

This new relational algebra plan is converted into a graph and each node in the graph becomes an execution kernel, while each edge becomes a ``CacheMachine``.

The graph is created by ``ral::batch::tree_processor`` that has a function called ``build_batch_graph``. This produces the actual graph object, 
which is what contains all the execution kernels and CacheMachines. The graph has a function called ``execute()`` which is what actually starts the ``run()`` function of every execution kernel, each on its own thread.

Column/Table Wrappers
---------------------
BlazingColumn
^^^^^^^^^^^^^

BlazingTable
^^^^^^^^^^^^
BlazingTableView
^^^^^^^^^^^^^^^^
Implements the same api as BlazingTable but wraps a ``cudf::table_view`` instead of
a vector of BlazingColumn.


Kernels
-------
:blazing_repo:`View in Github</engine/src/execution_graph/logic_controllers/taskflow/kernel.h>`

Every step in the physical plan maps to exactly one kernel. Kernels implement the
interface defined above and are found in
:blazing_repo:`Join Kernels</engine/src/execution_graph/logic_controllers/BatchJoinProcessing.h>`,
:blazing_repo:`Aggregation Kernels</engine/src/execution_graph/logic_controllers/BatchAggregationProcessing.h>`
:blazing_repo:`Order By Kernels</engine/src/execution_graph/logic_controllers/BatchOrderByProcessing.h>`,
:blazing_repo:`Filter Kernels</engine/src/execution_graph/logic_controllers/LogicalFilter.h>`,
:blazing_repo:`Project Kernels</engine/src/execution_graph/logic_controllers/LogicalProject.h>`.

Each execution kernel is a Class which implements the
:blazing_repo:`kernel</engine/src/execution_graph/logic_controllers/taskflow/kernel.h>`
interface. All kernels have an input port and an output port. Each of which
contains a map of named CacheMachines. A kernel might write to multiple outputs
and may  receive input from multiple inputs but they are all contained within
the input and output ports.

Only in the TableScan and BindableTableScan kernels are the input ports not defined.
In these two cases the kernels themselves generate data either by passing
through a cudf or by reading files.

A kernel will have a `run()` function which starts its execution. It pulls data
from its input ports, operates on them, then sends the results to its output ports.
The ports are just maps of name to CachedMachine.


All kernels basically take data in batches from one or more input cache machines, do some work, and put results into an output cache machine.
Almost all work done is done in batches, and usually the way the kernels iterate through those batches is via some form of a `DataSequencer` or which there are 4 kinds 
(these are defined in :blazing_repo:`Join Kernels</engine/src/execution_graph/logic_controllers/BatchProcessing.h>`):
BatchSequence
This is the standard data sequences that just pulls data from an input cache one batch at a time
BatchSequenceBypass
This data sequencer can pull data from a CacheMachine, but without decacheing the data. Serving as a bypass to take data from one input to an output without decacheing.
ExternalBatchColumnDataSequence
This data sequences connects a HostCacheMachine to a server receiving certain types of messages, so that basically the data sequences is effectively iterating through batches received from another node via out communication layer.
DataSourceSequence
This data sequences does not pull data from a CacheMachine, but it instead gets data from a data source, such as a set of files or from a DataFrame. These are the data sequences used by TableScans.



Caches
------

CacheData
^^^^^^^^^
:blazing_repo:`View in Github</engine/src/execution_graph/logic_controllers/CacheMachine.h#L43>`

There are different kinds of CacheData at the moment. GPU, CPU, LOCAL_FILE and
GPU_WITHMETADATA. The last of these being use exclusively in message routing.
Any implementer of CacheData must implement::

    virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;

The purpose of this class is that you can always call decache() on a CacheData
and get control of a BlazingTable that you own whose data is either moved in
the case of a GPU version, or brought into a GPU dataframe in the case it is a
non GPU version.

The LOCAL_FILE implementation uses ORC files as a temporary storage for data.

WaitingQueue
^^^^^^^^^^^^
:blazing_repo:`View in Github</engine/src/execution_graph/logic_controllers/CacheMachine.h#L167>`

Stores CacheData for us. Every CacheMachine has a WaitingQueue whose purpose it
is to hold the CacheData until they are needed by a kernel. Many of its methods
are waiting operations of the nature get_or_wait() which will wait on a
condition variable until something can actually be pulled from the WaitingQueue.

CacheMachine
^^^^^^^^^^^^

Cache Machines are an abstraction built on top of WaitingQueues that manage the
logic of knowing when a dataframe should stay on the gpu or be moved to RAM or
disk.

When you add data into a CacheMachine, it checks the memory consumption
of the node by asking the memory resource. If the consumption is below a certain
threshold, then the data is maintained in GPU memory. It is converted into a
GPUCacheData and added to the CacheMachine. If consumption is above the device
memory threshold, then it checks the next tier in the CacheMachine, the CPU
cache. It checks the memory consumption of the CPU memory resource. If it is
below that threshold, it converts the BlazingTable into a CPUCacheData, where it
copied all the data to host. If the CPU memory consumption is above a certain
threshold, then it goes into the next tier, the Disk Cache. For the disk cache,
the data is placed in an ORC file and a CacheDataLocalFile is created to keep track of it.

Aside from the standard CacheMachine, there are two specialty types: HostCacheMachine and ConcatenatingCacheMachine. The HostCacheMachine is only used to place data received by other nodes and the ConcatenatingCacheMachine is used as the output of TableScans. The ConcatenatingCacheMachine will concatenate batches so that the resulting batch is not too small. This is configurable, and its done to increase performance. Operating on really small batches can be detrimental to performance.


CacheMachines and CacheData are defined :blazing_repo:`CacheMachine.h</engine/src/execution_graph/logic_controllers/CacheMachine.h>`
