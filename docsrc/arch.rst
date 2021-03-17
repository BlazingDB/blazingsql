Technology Stack
================

TODO add tech stack image

Python
------
All the user facing apis are in python. This includes creating a BlazingContext, creating tables and running queries.
The python side of BlazingSQL relies on cudf and dask, since the results of a query are either a cudf DataFrame or in distributed mode a  dask_cudf DataFrame

Dask
----
Dask is used for creating a distributed context for when BlazingSQL runs in distributed mode. Dask is used for running all python side operations in a distributed context.
But after a query execution has been started, the different nodes of BlazingSQL do not use Dask to communicate with each other, they just communicate with each other directly.

C/C++
-----
Most query execution happens in the C/C++ layer. BlazingSQL leverages libcudf for most of the data processing primitives, with the exception of row based transformations.
Row based transformations leverage an internal abstract syntax tree processor we call interops.

Java
----
Apache Calcite is used for converting SQL queries into relational algebra. It's
maturity and prevalence are a justification of including Java as a dependency
for BlazingSQL. All Java interaction starts in the Python layer and is handled
through the use of `jpype <https://jpype.readthedocs.io/en/latest/>`_.



