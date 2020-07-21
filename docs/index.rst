.. BlazingSQL documentation master file, created by
   sphinx-quickstart on Mon Jul 20 12:19:33 2020.


Welcome to BlazingSQL's documentation!
======================================

Open-Source SQL in Python
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



Indices and tables
==================

* :ref:`search`
