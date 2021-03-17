
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
of the node by asking the memory resource (see below). If the consumption is below a certain
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