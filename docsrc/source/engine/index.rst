BlazingSQL Engine
=================

BlazingSQL is an open source, highly scalable, distributed analytical SQL engine that uses Nvidia GPUs to deliver very high performance.
BlazingSQL is part of the RAPIDS stack and therefore integrates well with all the RAPIDS libraries. Here in this documentation
you can learn about how the BlazingSQL engine works and its :doc:`tech stack <arch>`

Python User Interface
---------------------
The main user interface for BlazingSQL is through its python library ``blazingsql``. Via python, user's can create tables, register
filesystems, configure the engine and run queries. BlazingSQL returns query results as `cudf` DataFrames or `dask-cudf` DataFrames when running
in a distributed mode. You can lean more about the Python side of BlazingSQL :doc:`here <pyblazing>`.

Relational Algebra
------------------
When a user runs a SQL query, that SQL query gets converted into relational algebra by leveraging Apache Calcite. This relational algebra 
gets sent to the BlazingSQL Core engine for execution. You can lean more about Apache Calcite and the relational algebra produced :doc:`here <relational_algebra>`.

BlazingSQL Core
---------------
In the BlazingSQL Core engine, the relational algebra produced by Apache Calcite becomes a physical relational algebra plan, which in turn becomes a
directed acyclic graph (DAG), where each node is a :doc:`kernel <kernels>` and the edges which connect the nodes are :doc:`caches <caches>`. Each kernel
takes input data and generates a task to process the data, which is executed by the :doc:`task executor <executor>`. You can lean more about the BlazingSQL Core engine :doc:`here <graph>`.

Supporting components
---------------------
The BlazingSQL engine has several other very important components that lend to its extensibility, flexibility and performance:
* :doc:`Memory management <memory_management>` features
* :doc:`Communication <communication>` library that allow for very performant node to node communication using either TCP or UCX. 
* :doc:`Interops: <interops>` BlazingSQL's own row based operations engine.
* :doc:`I/O <io>` module to support for various file formats (text delimited, Apache Orc, Apache Parquet, JSON) and various filesystems (local, HDFS, AWS S3, GCS).
* :doc:`Data structures <data_structures>` to process and implement it all.


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Contents:

   arch
   pyblazing
   relational_algebra
   graph
   kernels
   caches
   executor
   memory_management
   communication
   interops
   io 
   data_structures 
   
   