

Memory Management
-----------------

BlazingMemoryResource
^^^^^^^^^^^^^^^^^^^^^
BlazingSQL has a `BlazingMemoryResource` interface that it uses for tracking memory consumption.
There are three implementations `blazing_device_memory_resource`, `blazing_host_memory_resource` and `blazing_disk_memory_resource`
to manange to keep track of GPU, HOST and DISK memory consumption.

The `blazing_device_memory_resource` internally has a `internal_blazing_device_memory_resource` which implements the `rmm::mr::device_memory_resource` interface.
When a BlazingContext() is first created it will create a new `internal_blazing_device_memory_resource` and set it as the default resource using `rmm::mr::set_current_device_resource`.

What form the `internal_blazing_device_memory_resource` takes is dependent on what parameters are passed to `BlazingContext()` parameters **allocator** and **pool**.
Different allocators settings can make the allocator use different underlying RMM allocator types. If the allocator is set to **existing**, then it will take the current
default allocator that has been set and wrap it with `internal_blazing_device_memory_resource`

The `blazing_host_memory_resource` and `blazing_disk_memory_resource` only track allocations and deallocations when BSQL caches and decaches data in the CacheMachines. Therefore,
they do not track host memory consumption or disk memory consumption by anything else in the system. Similarly, the blazing_device_memory_resource cannot track GPU memory consumption
of anything that does not use an rmm memory resource. The blazing_device_memory_resource could be made to track all GPU memory consumption by asking the CUDA driver
but such an implementation would not be very performant.

Whenever data enters a CacheMachine, it will check the memory consumption of the three `BlazingMemoryResource` to see what type of :doc:`CacheData <caches>` to use. This is one mechanism
employed by BSQL to manage memory consumption.


MemoryMonitor
^^^^^^^^^^^^^
BlazingSQL has a `MemoryMonitor` class that it instantiates for every query that is run. This MemoryMonitor will wake up every 50ms (configurable by MEMORY_MONITOR_PERIOD)
and check the GPU memory consumption as tracked by `blazing_device_memory_resource`. If memory consumption is too high, it will traverse the execution graph from the last node (final output)
to the first nodes (TableScans) downgrading CacheData as it can, to bring GPU memory consumption underneath its threshold. Downgrading CacheData means, taking a GPU CacheData and moving
the data to Host or Disk.

The `MemoryMonitor` helps ensure that memory GPU consumption does not get too high and therefore helps prevent OOM errors.


Task Execution Resource Management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The task executor tries to ensure that when it schedules tasks to run, that it does not run out of resources. There are two configuration options that
control how many concurrent tasks can be executred:
- *EXECUTOR_THREADS*: This sets a hard maximum number of concurrent tasks that can be executed
- *BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD*: This is a percent of the total GPU memory that the executor will try to stay under for starting new tasks. 

Before every task it will compare how much GPU memory the task will need, plus the memory already being used and compare that against this threshold. 
If the task will take it over the threshold, it will not start the task. The exception to that is that if there are no tasks running, then it will always try to run a task.
The memory estimation for how much a task will need is the sum of the estimate of how much decacheing the inputs will need, plus an estimate of the size of the outputs, plut an estimate
of the memory overhead needed for that algorithm. The kernel interface requires to implement the functions necessary for these memory consumption estimates.