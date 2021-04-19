.. _getting_started:

===============
Getting started
===============

Getting started with Blazing Notebooks: From zero to working with RAPIDS/BlazingSQL in under 10 minutes.
This is a crash course on BlazingSQL. If you have used Apache Spark with PySpark, this should be very familiar to you. 
We are going to show off the main features of a BlazingSQL instance in this guide. 


Getting started
===============

Import packages
---------------

First, import the required libraries. BlazingSQL uses cuDF to handoff results, 
so it's always a good idea to import it as well. For more information on cuDF, 
please check out the `RAPIDS.ai Documentation Page <https://docs.rapids.ai>`_.

You must establish a BlazingContext to connect to a BlazingSQL 
instance to create tables, 
run queries, and basically do anything with BlazingSQL.

.. code-block:: python

    from blazingsql import BlazingContext

    bc = BlazingContext()


Connect a Storage Plugin
------------------------

.. TODO need to update the link to the proper docs

BlazingSQL can 
`connect multiple storage solutions <https://docs.blazingdb.com/docs/connecting-data-sources>`_ 
in order to query files from numerous local and/or distributed filesystems.

Here, we are showing how you would connect to an `AWS S3 <https://docs.blazingdb.com/docs/s3>`_ bucket.

.. code-block:: python

    bc.s3('dir_name', 
        bucket_name='bucket_name', 
        access_key_id='access_key', 
        secrect_key='secret_key')

Create Tables
-------------

BlazingSQL can query raw files or in-memory DataFrames, 
but you must create a table to run a query. 
We're going to create three tables; two, from files in AWS S3, 
and one from a local, already existent, GPU DataFrame (GDF).

For more info check the `Blazing SQL API documentation <https://docs.blazingdb.com/docs/creating-tables>`_. 

.. code-block:: python

    # create table 01 from CSV file
    bc.create_table('table_name_01', 's3://dir_name/file1.csv') 

    # create table 02 from Parquet file
    bc.create_table('table_name_02', 's3://dir_name/file2.parquet') 

    # create table 03 from cuDF DataFrame
    bc.create_table('table_name_03', existing_gdf) 

`Launch Demo <https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/docs/blazingsql/blazingcontext_api/create_tables.ipynb>`_

Run SQL
-------

Once you have created your tables, you will now be able to use SQL to query said tables. 
BlazingSQL processes every file as a GDF, which means you can run federated 
queries over multiple data sources and file formats. 

You can read more about our supported SQL in the `General SQL <https://docs.blazingdb.com/docs/queries>`_ 
reference section of the Blazing SQL API documentation.

.. code-block:: python

    query = ''' 
        SELECT a.* FROM table_name_01 AS a 
        UNION ALL 
        SELECT b.* FROM table_name_02 as b 
        UNION ALL 
        SELECT c.* FROM table_name_03 as c 
    '''
    # type(gdf) == cudf.core.dataframe.DataFrame
    gdf = bc.sql(query)

`Launch Demo <https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/docs/blazingsql/blazingcontext_api/create_tables.ipynb>`__

Explain
-------

To better understand what's going on, BlazingContext's :code:`.explain()` method can 
be called to break down a given query's algebra plan (query execution plan).

.. code-block:: python

    # define a query 
    query = ''' 
        SELECT a.* FROM table_name_01 AS a 
        UNION ALL 
        SELECT b.* FROM table_name_02 as b 
        UNION ALL 
        SELECT c.* FROM table_name_03 as c 
    '''
    # what's going on when this query runs?
    bc.explain(query)

`Launch Demo <https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/docs/blazingsql/blazingcontext_api/blazingcontext.ipynb#.explain()>`__

Drop Tables
-----------

To drop a table from BlazingContext we call :code:`drop_table` and pass in the name of the table to drop.

.. code-block:: python

    # drop table 01 
    bc.drop_table('table_name_01')

    # drop table 03
    bc.drop_table('table_name_03')

`Launch Demo <https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/docs/blazingsql/blazingcontext_api/blazingcontext.ipynb#.drop_table()>`__

Handing off a GDF
-----------------

The resulting GDF resides in GPU memory. 
The GDF is built on `Apache Arrow <https://arrow.apache.org>`_ and interoperates 
with the rest of the RAPIDS.ai ecosystem. 

For example, we can run typical pandas-like commands but using cuDF.

.. code-block:: python

    print(gdf.mean(), gdf.var())
    print(gdf.as_matrix())

|

Or, we can pass the result GDF to a Pandas DataFrame:

.. code-block:: python

    import pandas

    # from cuDF DataFrame to pandas DataFrame
    df = gdf.to_pandas()

|

But the real power is passing to other GPU accelerated libraries. 
cuML offers the algorithms found in SK-Learn but operates on GDFs. 
Here we are passing GDFs to cuML for use in training linear regression algorithm.

.. code-block:: python

    import cuml
    from cuml import LinearRegression

    # create model
    lr = LinearRegression(fit_intercept=True, normalize=False, algorithm="eig")

    # train model
    reg = lr.fit(X_train_gdf, y_train_gdf)

    print("Coefficients:")
    print(reg.coef_)
    print(" ")
    print(" Y intercept:")
    print(reg.intercept_)

Run Distributed SQL
-------------------

BlazingSQL strives to make it easy to scale up a workload. With minimal code changes, 
we can make queries run on an arbitrary number of GPUs.
The results are returned as a Dask-cuDF, a partitioned GPU DataFrame, 
distributed across the number of GPUs in a cluster.

.. code-block:: python

    from blazingsql import BlazingContext
    import cudf
    import dask_cudf
    import dask
    from dask.distributed import Client

    # set Dask Client
    client = Client('127.0.0.1:8786')
    # start Blazing Context
    bc = BlazingContext(dask_client=client)

    # register AWS S3 bucket to Blazing Context
    bc.s3('dir_name'
        , bucket_name='bucket_name'
        , access_key_id='access_key'
        , secret_key='secret_key'
    )

    table_list = [
        's3://dir_name/parquet_dir/1_0_0.parquet'
        , 's3://dir_name/parquet_dir/1_1_0.parquet'
        , 's3://dir_name/parquet_dir/1_2_0.parquet'
    ]

    # create table from multiple Parquet files
    bc.create_table('table_name', table_list)

    # query the table (returns cuDF DataFrame)
    gdf = bc.sql('SELECT COUNT(*) FROM table_name')

    print(gdf.head())

.. toctree::
    :maxdepth: 3