BlazingSQL Project Architecture
===============================

C/C++
-----
Most query execution happens in the C/C++ layer.


Python
------
User facing apis are in python.

Java
----
Apache Calcite is used for converting SQL queries into relational algebra. It's
maturity and prevalence are a justification of including Java as a dependency
for BlazingSQL. All Java interaction starts in the Python layer and is handled
through the use of `jpype <https://jpype.readthedocs.io/en/latest/>`_.
