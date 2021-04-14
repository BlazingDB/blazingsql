``.create_table()``
===================

BlazingSQL can query raw files or in-memory DataFrames, but you must first create a table before you can run a query on it.

See [Creating Tables](doc:creating-tables) for more detailed information.

As an example we're going to create three tables; two, from files in AWS S3, and one from a local, already existent, GPU DataFrame (GDF).

.. code-block:: python

    # create table 01 from CSV file stored in AWS S3 bucket
    bc.create_table('table_name_01', 's3://dir_name/file1.csv') 

    # create table 02 from Parquet file stored in Google Storage bucket
    bc.create_table('table_name_02', 'gs://dir_name/file2.parquet') 

    # create table 03 from cuDF or pandas DataFrame
    bc.create_table('table_name_03', df) 

`Launch Demo <https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/docs/blazingsql/blazingcontext_api/blazingcontext.ipynb#.create_table()>`_

.. list-table:: Parameters
   :widths: 25 25 30 30
   :header-rows: 1

   * - Argument
     - Required
     - Description
     - Default
   * - ``table_name``
     - Yes
     - String of table name.
     - N/a
   * - ``input``
     - Yes
     - Data source for table.
     - N/a
   * - ``file_format``
     - No
     - A string describing the file format (``"csv"``, ``"orc"``, ``"parquet"``). This field must only be set if the files do not have an extension.
     - If this field is not set, BlazingSQL will try to infer what file type it is, depending on the extension on the files found by BlazingSQL

See each data source's doc for more information on additional parameters.

.. More info: [Apache Hive](doc:apache-hive), [Apache Parquet](doc:apache-parquet), [GPU DataFrame (GDF)](doc:gpu-dataframe-gdf), [Text Files (CSV, TSV, PSV)](doc:text-files).

.. toctree::
   :maxdepth: 3
   :glob:

