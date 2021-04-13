Query cuDF DataFrames
=====================

Everything in BlazingSQL is processed as a ``cudf.DataFrame``, much like almost 
everything in the RAPIDS ecosystem. Therefore you can easily build a BlazingSQL 
table off of a cudf DataFrame, by only providing the table name and the 
DataFrame itself.

.. code-block:: python
    
    import blazingsql
    import cudf

    bc = blazingsql.BlazingContext()

    table_gdf = cudf.read_csv('...file_path/table.csv')

    bc.create_table('table_name', table_gdf)

Parameters
~~~~~~~~~~

* **table_name** - string. **Required.** Name of the table you are creating. 
* **cudf.DataFrame** - ``cudf.DataFrame``. **Required.** The DataFrame you wish to query.

Query dask_cudf DataFrames
==========================

When using a Dask client to make BlazingSQL work in a distributed context, 
you can also create a BlazingSQL table from a dask_cudf DataFrame.

.. code-block:: python

    import blazingsql
    import dask_cudf
    from dask.distributed import Client
    from dask_cuda import LocalCUDACluster
    
    cluster = LocalCUDACluster()
    client = Client(cluster)
    bc = BlazingContext(dask_client=client, network_interface='lo')
    
    table_ddf = dask_cudf.read_parquet('...file_path/table.parquet')
    
    bc.create_table('table_name', table_ddf)
    
Parameters
~~~~~~~~~~

* **table_name** - string. **Required.** Name of the table you are creating. 
* **dask_cudf.DataFrame** - ``dask_cudf.DataFrame``. **Required.** The DataFrame you wish to query.