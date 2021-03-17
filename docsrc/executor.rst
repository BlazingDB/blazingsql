Task Executor
=============

The task executor's job is to take all of the jobs in the queue and actually schedule them to run on the hardware that we are targetting. Currently we only target Nvidia GPUs with our executor. It manages access to resources like system and gpu memory and limits the number of tasks that are being executed concurrently. It also is what provides the ability to retry operations that failed due to lack of resources that can be retried when resources are more plentiful.

Tasks
^^^^^
A list of all tasks that are currently waiting to be executed. Right now there is no priority for tasks and tasks are just processed FIFO. This is an obvious current weakness and should be improved upon when time permits. Upon completion / failure the kernel that sent to the task to the executor is notified the task has completed /failed. The majority of tasks can be reattempted, the only ones that can't are those where the input remained in GPU and was modified in place.
