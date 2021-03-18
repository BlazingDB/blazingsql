.. BlazingSQL documentation master file, created by
   sphinx-quickstart on Mon Jul 20 12:19:33 2020.


Welcome to BlazingSQL's documentation!
======================================

BlazingSQL is a collection of configurable primitives for operating on distributed dataframes. It includes an I/O layer for interacting with filesystems like S3 or HDFS. It contains communication primitives that can leverage different backends like UCX and TCP. It seperates the.

Usage
^^^^^^^^^^^^^^^^^^^^^^^^^
BlazingSQL provides a high-performance distributed SQL engine in Python. Built on the RAPIDS GPU data science ecosystem, ETL massive datasets on GPUs. ::

    from blazingsql import BlazingContext
    # Start up BlazingSQL
    bc = BlazingContext()

    # Create table from CSV
    bc.create_table('taxi', '/blazingdb/data/taxi.csv')

    # Query table (Results return as cuDF DataFrame)
    gdf = bc.sql('SELECT count(*) FROM taxi GROUP BY year(key)')

    print(gdf)
    # Display query results

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   arch
   graph
   api

Indices and tables
==================

* :ref:`search`
