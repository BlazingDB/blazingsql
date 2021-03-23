.. _blazing_context:

================
BlazingContext()
================

BlazingContext is the Python API of BlazingSQL. Along with initialization arguments, the BlazingContext class has a number of methods which assist not only in creating and querying tables, but also in connecting remote data sources and understanding your ETL.

You must establish a BlazingContext to connect to a BlazingSQL instance to create tables, run queries, and basically do anything with BlazingSQL. First, import the required libraries. 

.. code-block:: python

    from blazingsql import BlazingContext

    bc = BlazingContext()

`Launch Demo <https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/docs/blazingsql/blazingcontext_api/blazingcontext.ipynb>`_


Below is a list of configuration parameters a user can specify to refine the ``BlazingContext``.

.. list-table:: List of ``BlazingContext()`` parameters
   :widths: 30 25 50 25
   :header-rows: 1

   * - Argument
     - Required
     - Description
     - Defaults
   * - ``allocator``
     - No
     - Options are ``"default"``, ``"managed"``. Where ``"managed"`` uses Unified Virtual Memory (UVM) and may use system memory if GPU memory runs out, or ``"existing"`` where it assumes you have already set the rmm allocator and therefore does not initialize it (this is for advanced users.)
     - ``"managed"``
   * - ``network_interface``
     - No
     - Network interface used for communicating with the dask-scheduler. See note below.
     - ``'eth0'``
   * - ``pool``
     - No
     - If True, allocate a memory pool in the beginning. This can greatly improve performance.
     - ``False``
   * - ``dask_client``
     - No
     - The dask client used for communicating with other nodes. This is only necessary for running BlazingSQL with multiple nodes.
     - ``None``
   * - ``allocator``
     - No
     - Options are \"default\ \"managed\". Where \"managed\" uses Unified Virtual Memory (UVM) and may use system memory if GPU memory runs out, or \"existing\" where it assumes you have already set the rmm allocator and therefore does not initialize it (this is for advanced users.).
     - ``"managed"``
   * - ``initial_pool_size``
     - No
     - ``None``
     - Initial size of memory pool in bytes (if pool=True). If None, it will default to using half of the GPU memory.
   * - ``enable_logging``
     - No
     - If set to True the memory allocator logging will be enabled, but can negatively impact performance. This is for advanced users.
     - ``False``
   * - ``config_options``
     - No
     - A dictionary for setting certain parameters in the engine.
     - See :doc:`config_options<blazing_context/config_options>`
   * - ``enable_progress_bar``
     - No
     - Set to true to display a progress bar during query executions
     - ``False``

**Note:**  When using BlazingSQL with multiple nodes, you will need to set the 
correct ``network_interface`` your servers are using to communicate with the 
IP address of the dask-scheduler. You can see the different network interfaces 
and what IP addresses they serve with the bash command ``ifconfig``. The default 
is set to ``'eth0'``.


.. toctree::
   :maxdepth: 4
   :glob:

   blazing_context/*