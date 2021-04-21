.. _config_options:

BlazingSQL Configuration Options
--------------------------------

The ``config_options`` parameter is a dictionary for setting certain parameters in the engine. 
These parameters will be used for all queries except if overriden by setting these parameters 
when running the query itself in the ``.sql()`` command. The possible parameters are:

.. list-table:: Parameters
   :widths: 50 50 30
   :header-rows: 1

   * - Option
     - Description
     - Default
   * - ``JOIN_PARTITION_SIZE_THRESHOLD``
     - Num bytes to try to have the partitions for each side of a join before doing the join. Too small can lead to overpartitioning, too big can lead to OOM errors.
     - 400000000
   * - ``MAX_JOIN_SCATTER_MEM_OVERHEAD``
     - The bigger this value, the more likely one of the tables of join will be scattered to all the nodes, instead of doing a standard hash based partitioning shuffle. Value is in bytes.
     - 500000000
   * - ``MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE``
     - The maximum number of partitions that will be made for an order by. Increse this number if running into OOM issues when doing ``ORDER BY`` with large amounts of data.
     - 8
   * - ``NUM_BYTES_PER_ORDER_BY_PARTITION``
     - The max number size in bytes for each order by partition. Note that, ``MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE`` will be enforced over this parameter.
     - 400000000
   * - ``MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE``
     - The max size in bytes to concatenate the batches read from the scan kernels.
     - 400000000
   * - ``MAX_ORDER_BY_SAMPLES_PER_NODE``
     - The max number order by samples to capture per node.
     - 10000
   * - ``BLAZING_PROCESSING_DEVICE_MEM_CONSUMPTION_THRESHOLD``
     - The percent (as a decimal) of total GPU memory that the memory that the task executor will be allowed to consume. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - 0.9
   * - ``BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD``
     - The percent (as a decimal) of total GPU memory that the memory resource will consider to be full. **NOTE:** This parameter only works when used in the ``BlazingContext`` 
     - 0.95
   * - ``BLAZ_HOST_MEM_CONSUMPTION_THRESHOLD``
     - The percent (as a decimal) of total host memory that the memory resource will consider to be full. In the presence of several GPUs per server, this resource will be shared  among all of them in equal parts. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - 0.75
   * - ``BLAZING_LOGGING_DIRECTORY``
     - A folder path to place all logging files. The path can be relative or absolute. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - blazing_log
   * - ``BLAZING_LOCAL_LOGGING_DIRECTORY``
     - A folder path to place the client logging file on a dask environment. The path can be relative or absolute. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - blazing_log
   * - ``BLAZING_CACHE_DIRECTORY``
     - A folder path to place all orc files when start caching on Disk. The path can be relative or absolute.
     - /tmp/
   * - ``MEMORY_MONITOR_PERIOD``
     - How often the memory monitor checks memory consumption. The value is in milliseconds.
     - 50  (milliseconds)
   * - ``MAX_KERNEL_RUN_THREADS``
     - The number of threads available to run kernels simultaneously.
     - 16
   * - ``EXECUTOR_THREADS``
     - The number of threads available to run executor tasks simultaneously.
     - 10
   * - ``MAX_SEND_MESSAGE_THREADS``
     - The number of threads available to send outgoing messages.
     - 20
   * - ``LOGGING_LEVEL``
     - Set the level (as string) to register into the logs for the current tool of logging. Log levels have order of priority:``{trace, debug, info, warn, err, critical, off}``. Using ``'trace'`` will registers all info. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - trace
   * - ``LOGGING_FLUSH_LEVEL``
     - Set the level (as string) of the flush for the current tool of logging. Log levels have order of priority: ``{trace, debug, info, warn, err, critical, off}`` **NOTE:** This parameter only works when used in the ``BlazingContext``
     - warn
   * - ``LOGGING_MAX_SIZE_PER_FILE``
     - Set the max size in bytes for the log files. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - 1073741824  (1 GB)
   * - ``ENABLE_GENERAL_ENGINE_LOGS``
     - Enables ``'batch_logger'`` logger
     - True
   * - ``ENABLE_COMMS_LOGS``
     - Enables ``'output_comms'`` and ``'input_comms'`` logger
     - False
   * - ``ENABLE_TASK_LOGS``
     - Enables ``'task_logger'`` logger
     - False
   * - ``ENABLE_OTHER_ENGINE_LOGS``
     - Enables ``'queries_logger'``, ``'kernels_logger'``, ``'kernels_edges_logger'``, ``'cache_events_logger'`` loggers
     - False
   * - ``TRANSPORT_BUFFER_BYTE_SIZE``
     - The size in bytes about the pinned buffer memory. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - 1048576  (10 Mb)
   * - ``TRANSPORT_POOL_NUM_BUFFERS``
     - The number of buffers in the pinned buffer memory pool. **NOTE:** This parameter only works when used in the ``BlazingContext``
     - 100
   * - ``PROTOCOL``
     - The communication protocol to use. The options are ``'TCP'``, ``'UCX'`` or ``'AUTO'``. ``'AUTO'`` will use whatever the dask client is using. **NOTE:** This parameter only works when used in the ``BlazingContext``.
     - AUT
    
The config options can also be set via environment variables. The corresponding environment variable is the same as the config option, but appying a prefix of ``BSQL_``. So for example you can set ``BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD`` using environment variables like this:

.. code-block:: bash

    export BSQL_BLAZING_DEVICE_MEM_CONSUMPTION_THRESHOLD=0.5

The config options set via environment variables will need to be set on the process creating the ``BlazingContext`` and before the ``BlazingContext`` is created.

.. toctree::
   :maxdepth: 3
   :hidden:
   :glob: