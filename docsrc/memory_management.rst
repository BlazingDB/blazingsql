

Memory Management
-----------------

BlazingMemoryResource
^^^^^^^^^^^^^^^^^^^^^
:blazing_repo:`View in Github</engine/src/bmr/BlazingMemoryResource.h>`

BlazingSQL has a `BlazingMemoryResource` interface that it uses for tracking memory consumption.
There are three implementations `blazing_device_memory_resource`, `blazing_host_memory_resource` and `blazing_disk_memory_resource`
to manange to keep track of GPU, HOST and DISK memory consumption.

The `blazing_device_memory_resource` internally has a `internal_blazing_device_memory_resource` which implements the `rmm::mr::device_memory_resource` interface.
When a BlazingContext() is first created it will create a new `internal_blazing_device_memory_resource` and set it as the default resource using `rmm::mr::set_current_device_resource`.

What form the `internal_blazing_device_memory_resource` takes is dependent on what parameters are passed to `BlazingContext()` parameters **allocator** and **pool**.
Different allocators settings can make the allocator use different underlying RMM allocator types. If the allocator is set to **existing**, then it will take the current
default allocator that has been set and wrap it with `internal_blazing_device_memory_resource`

The `blazing_host_memory_resource` and `blazing_disk_memory_resource` only track allocations and deallocations when BSQL caches and decaches data in the CacheMachines.

Whenever data enters a CacheMachine, it will check the memory consumption of the three `BlazingMemoryResource` to see where the CacheData should reside. This is one mechanism
employed by BSQL to manage memory consumption.


MemoryMonitor
^^^^^^^^^^^^^
:blazing_repo:`View in Github</engine/src/bmr/MemoryMonitor.h>`

BlazingSQL has a `MemoryMonitor` class that it instantiates for every query that is run. This MemoryMonitor will wake up every 50ms (configurable by MEMORY_MONITOR_PERIOD)
and check the GPU memory consumption as tracked by `blazing_device_memory_resource`. If memory consumption is too high, it will traverse the execution graph from the last node (final output)
to the first nodes (TableScans) downgrading CacheData as it can, to bring GPU memory consumption underneath its threshold. Downgrading CacheData means, taking a GPU CacheData and moving
the data to Host or Disk.

The `MemoryMonitor` helps ensure that memory GPU consumption does not get too high and therefore helps prevent OOM errors.



Future Resource Management
^^^^^^^^^^^^^^^^^^^^^^^^^^

Indeed, though we do not support it now, one could envision composing an executor from 2 or more executors to leverage multiple execution runtimes in parallel. Allowing the least busy or most appropriate executor to perform said operation. This would also require refactoring the kernels so that they either send a comamand which maps to a function that has been registered with the executor or have each executor be able to implement a run function per supported runtime.

