Data Structures
===============

BlazingTable
------------
One of the main data structure used in the BlazingSQL engine is the BlazingTable. It is always used as a ``std::unique_ptr<BlazingTable>``, since its designed to own the data.
One simple way of thinking of a BlazingTable is as a wrapper for a cudf table that owns the data and that has names for all the columns, but its actually a little more complicated than that. 
A BlazingTable is actually a vector of BlazingColumn. BlazingColumn is an interface that has two implementations:
- *BlazingColumnOwner*: A simple wrapper around a *cudf column*.
- *BlazingColumnView*: A simple wrapper around a *cudf column_view*.
Since a BlazingTable is a vector of BlazingColumn, then it can have a combination of columns that it owns or columns it does not.

In general, it is assumed that a BlazingTable always owns it columns and therefore it always contains *BlazingColumnOwner*. In the logic,
the code always asumes that the data is owned by BlazingTable and when a BlazingTable object goes out of scope, the memory is freed.
The exception to this is when the data it contains comes from a cudf DataFrame that is owned by the user. This happens when a user creates a table
from a cudf DataFrame. In this case, the GPU data is owned by the user on the python layer. Only a view of that data is passed to the C++ layer.
Rather than making a copy, we create a BlazingTable around that cudf DataFrame view and pretend that we own it. Additionally this ownership and/or 
lackthereof is defined at the column level and not the table level, so that you can perform a Project operation on that table that only modifies certain columns,
then you can still keep some of the original data intact. 


BlazingTableView
^^^^^^^^^^^^^^^^
A BlazingTableView is a wrapper around a cudf table_view that also has column names. It is used for holding a non-owning view of a BlazingTable. 

