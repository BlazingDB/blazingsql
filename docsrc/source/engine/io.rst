I/O
===

The I/O module of the BlazingSQL engine is what connects the engine with data sources. 


Uri
---

In BlazingSQL file paths are encapsulated in the ``Uri`` class which contains information about the file path and its filesystem, 
plus it provides additional utility functions. The filesystem can be local (POSIX), Hadoop FileSystem (HDFS), Google Compute Storage (GCS), 
AWS S3. How files are read or manipulated in the different filesystems is handled by the ``FileSystemInterface``.


FileSystemInterface
-------------------

The ``FileSystemInterface`` provides methods for interacting with the different types of filesystems.  Therefore ``FileSystemInterface`` methods all use or refer
to ``Uri``'s. Here are some examples of ``FileSystemInterface`` methods:

* ``bool exists(const Uri & uri)``
* ``std::vector<Uri> list(const Uri & uri, const std::string & wildcard = "*")``
* ``bool makeDirectory(const Uri & uri)``
* ``bool remove(const Uri & uri)``
* ``bool move(const Uri & src, const Uri & dst)``
* ``std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri & uri)``
* ``std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri & uri)``

There is a ``FileSystemInterface`` implementation for each supported filesystems:

* LocalFileSystem
* HadoopFileSystem
* GoogleCloudStorage
* S3FileSystem

When reading and writting to files, all filesystems use the same file handle interfaces:

* ``arrow::io::RandomAccessFile`` for reading files
* ``arrow::io::OutputStream`` for writing files

This way there can be a common interface, even if the underlaying APIs for interacting with those filesystems is dramatically different.
Additionally since these are common OSS interfaces from Apache Arrow, these same intefaces are used by `cudf`.


FileSystemManager
-----------------

When a user registers a filesystem from the python layer, it also gets registered on the C++ side. 
There is a singleton class called ``FileSystemManager`` that can hold several ``FileSystemInterface`` that have been registered with it.
By default, it will always start with one filesystem, which is to the `LocalFileSystem's` root folder.

In the BlazingSQL code, most of the time we dont use the ``FileSystemInterface`` implementations directly, instead, we use ``Uri``'s and 
the ``FileSystemManager``. The ``FileSystemManager`` also has most of the same methods as the ``FileSystemInterface``. 
The ``FileSystemManager`` will figure out which ``FileSystemInterface`` the particular ``Uri`` needs, and 
it will call that interface's method in question.


data_loader
-----------
When performing a SQL query, the source of the data is obtained from a `TableScan` (or a `BindableTableScan`). 
The `TableScan` uses a ``data_loader`` which is a combination of a ``data_provider`` with a ``data_parser`` with which
it can take data from any source and produce a DataFrame. 


data_provider
-------------
The purpose of the DataProvider interface is to provide ``data_handle``s from any type of source that could be used by a `TableScan`.
For example if a table that you are querying consists of a set of 10 files, then the DataProvider could provide you with ``data_handle``s
for each file. In this case, the ``data_handle`` would contain a ``arrow::io::RandomAccessFile`` which would be decoupled from what file
path or filesystem it came from. Similarly, if the table consisted of a filepath that had a wildcard, then ``data_provider`` would take care
of figuring out how many or which ``data_handle``s that path with a wildcard would become.

Currently the ``data_provider`` interface has two implementations:

* ``UriDataProvider``: Which provides data_handles that come from files
* ``GDFDataProvider``: Which provides data_handles that come from cudf DataFrames


data_handle
-----------
A ``data_handle`` is a struct which can contain the data itself (a BlazingTableView), if its a data handle for data coming from a cudf DataFrame
or it can contain a ``arrow::io::RandomAccessFile`` and a ``Uri`` if the data will come from a file. Additionally it can contain a map of 
key value pairs (column name / column value) for holding ``column_values``.  The ``column_values`` are used for when a table comes from files 
that come from a partitioned dataset. In those cases you can have a file that represents part of a table, but does not actually contain all the columns, 
and the columns it does not have are filled with a single value.


data_parser
-----------
A ``data_parser`` is what takes a ``data_handle`` and obtains a BlazingTable from it. How it does that depends on the type of data parser,
which in turn depends on the data held by the ``data_handle``. The ``data_parser`` uses a ``Schema`` to know to parse the data, and
``column_indices`` to know which columns it needs to collect from the ``data_handle``. 
Currently the ``data_parser`` interdace has 5 implementations:

* ``gdf_parser``: A parser for data coming from a cudf DataFrame
* ``parquet_parser``: A parser for Apache Parquet files
* ``orc_parser``: A parser for Apache Orc files
* ``csv_parser``: A parser for delimited text files
* ``json_parser``: A parser for json files


Extensibility of I/O
--------------------
The system of interfaces in BlazingSQL's I/O module can be extended to provide support for other completely different types of data sources.
For example if we wanted to have a data source that was data comming from another database, another implementation of ``data_provider`` could
be created that would hold a connection to the database and information about the table. And the ``data_handle`` could be expanded
in its capabilities, to provide support for holding the results of a query to said database. While finally, a new ``data_parser`` 
interface could be created to parse the results of the query and convert it to a BlazingTable.