Task Executor
=============
The task executor manages a queue of tasks to be run. These are the tasks that are created by the different kernels.
The task executor's job is to take all of the jobs in the queue and actually schedule them to run on the hardware that we are targetting. 
Currently we only target Nvidia GPUs with our executor. 

Tasks
^^^^^
A task consists of a set of input CacheDatas, a pointer to a kernel, a pointer to an output cache and map of key and value optional arguments.
When the executor runs a task, it will first decahe the input CacheDatas, to materialize the input data into BlazingTables and then run the
kernel's ``do_process()`` function on that input data.

Resource Management
^^^^^^^^^^^^^^^^^^^
The task executor tries to ensure that when it schedules tasks to run, that it does not run out of resources. There are two configuration options that
control how many concurrent tasks can be executred:
- *EXECUTOR_THREADS*: This sets a hard maximum number of concurrent tasks that can be executed
- *BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD*: This is a percent of the total GPU memory that the executor will try to stay under for starting new tasks. 

Before every task it will compare how much GPU memory the task will need, plus the memory already being used and compare that against this threshold. 
If the task will take it over the threshold, it will not start the task. The exception to that is that if there are no tasks running, then it will always try to run a task.
The memory estimation for how much a task will need is the sum of the estimate of how much decacheing the inputs will need, plus an estimate of the size of the outputs, plut an estimate
of the memory overhead needed for that algorithm. The kernel interface requires to implement the functions necessary for these memory consumption estimates.

Task Queue
^^^^^^^^^^
Right now there is no priority for tasks and tasks are just processed FIFO. This is an obvious current weakness and should be improved upon when time permits. 

Task Retry
^^^^^^^^^^
If a task runs into a OOM error when executing a task, the executor will attempt to re-create the task with the same inputs and add it back to the queue. There are two places
where the task executor could detect an OOM error. The first is during the decacheing of the task's input CacheDatas. If an OOM happens during decacheing, it will try to recreate
the task using that CacheData it was trying to decache, and then add that task back to the task executor's queue. The second is inside the ``do_process()`` function of the kernel.
In the ``do_process()`` function, there should always be a try/catch block. If an OOM is caught, then it will try to take the input BlazingTables and output them
with an error flag, so that the BlazingTables can be placed back into a CacheData and the task can be re-created. In either case, there can be circumstances where
the input data cannot be recovered, and therefore the task cannot be recreated. One of those cases is when a task modifies the input data in place. In such a case,
the ``do_process()`` function would return with an error flag that indicates that it should not be retried. If a task cannot be recreated, 
then the query will be terminated with an error.
