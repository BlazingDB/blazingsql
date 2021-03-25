Python User Interface
=====================

The python side of BlazingSQL, sometimes refered to as pyblazing, is the main user interface for BlazingSQL. It is connected to Apache Calcite via Jpype and connected to the rest of
the BlazingSQL engine in C++ via Cython. Before a user can do anything with BlazingSQL, the user must first create a `BlazingContext`.

BlazingContext
--------------
The BlazingContext is what holds the user's context. It is through the BlazingContext that a user can create a table, run a SQL query, register a filesystem and more. When you create a 
BlazingContext the following things are set, and cannot be changed:

* The memory allocator
* Is the BlazingContext distributed or single node? It is a distributed BlazingContext, if a `dask_client` is passed to the BlazingContext constructor, and single node otherwise.
* Configuration options. (some of these can also be set on a query by query basis)

