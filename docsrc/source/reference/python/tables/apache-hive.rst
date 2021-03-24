=================
Query Hive Tables
=================

If you have created tables using Hive, you can query those tables easily using 
BlazingSQL. You can simply connect to Hive and get a cursor, and give it to 
BlazingSQL and Hive can tell BlazingSQL where the data is located, its schema 
and table names to make querying those tables much simpler. BlazingSQL can also 
take advantage of any partitioning you have done with Hive, so that only certain 
partitions are read as necessary to process a query.

Basic Usage
-----------

You can query your Hive tables by leveraging *pyhive*.
All you have to do is obtain a *cursor* from Hive and pass it into the create table statement. 
Use the optional parameters if you want the BlazingSQL table to have a different name than how Hive had the name, or if the Hive table is not in the "default" Hive database.

Parameters
~~~~~~~~~~

* **hive_table_name** - string. The name of the table in Hive. **Default:** same as the table name being given in ``bc.create_table()``.
* **hive_database_name** - string. The name of the database or schema in Hive where the table resides. **Default:** ``"default"``.
* **file_format** - string. A string describing the file format (``"csv"``, ``"orc"``, ``"parquet"``). It is recommended to set this parameter because Hive sometimes stores files without an extension that is recognized by BlazingSQL. **Default:** If this field is not set, BlazingSQL will try to infer what file type it is, depending on the extension on the files found by BlazingSQL.

Example
~~~~~~~

.. code-block:: python

    from blazingsql 
    import BlazingContext
    from pyhive import hive
    
    # start up BlazingSQL
    bc = BlazingContext()
    
    # connect to Hive and obtain a cursor
    cursor = hive.connect(
        host="hive_server_ip_address",
        port="hive_server_port",  # usually 10000
        username="",
        auth="KERBEROS",  # optional
        kerberos_service_name="hive") # optional
        
    # give create_table the Hive cursor
    # the table name must match the same table name as in Hive
    bc.create_table("hive_table_name", cursor)
    
    # query table (result = cuDF DataFrame)
    result = bc.sql("select * from hive_table_name")
    
    # create a table called "hive_table_name2" from a hive table called my "my_hive_table_name" in database "db2"
    bc.create_table(
        "hive_table_name2", cursor, 
        hive_table_name="my_hive_table_name", 
        hive_database_name="db2") 

Advanced Usage: Hive cursor
---------------------------

If you want your BlazingSQL table to only be aware of certain partitions, you 
can select the partitions you want when you create the BlazingSQL table using 
the "partitions" parameter. When using "partitions" and also using a 
Hive cursor, if you don't specify any partitions for a partitioned column, 
the table will be created with all the partitions for that column.

Parameters
~~~~~~~~~~

* **partitions** - dictionary. A dictionary object where the keys are the column names for all partitioned columns, and the values are all the values of the partitions of interest. **Default:** ``None``.

Example
~~~~~~~

.. code-block:: python

    from blazingsql import BlazingContext

    # start up BlazingSQL
    bc = BlazingContext()
    
    # connect to Hive and obtain a cursor
    cursor = hive.connect(
        host="hive_server_ip_address",
        port="hive_server_port",  # usually 10000
        username="",
        auth="KERBEROS",  # optional
        kerberos_service_name="hive") # optional

    # Give create_table the Hive cursor
    # Here we are selecting only 6 partitions for this table 
    # (2 t_year X 3 t_company_id X 1 region)
    bc.create_table(
        "asia_transactions", 
        cursor, 
        file_format="parquet", 
        hive_table_name="fin_transactions", 
        partitions = {
            "t_year":[2017, 2018], 
            "t_company_id":[2, 4, 6], 
            "region": ["asia"]
        }
    )
                    
    # Here we are creating a similar table, but not specifying 
    # any region partitions, in which case it will create a table 
    # with all the partitions. We are selecting only 30 partitions 
    # for this table (2 t_year X 3 t_company_id X all region partitions 
    # (5 in this example)).
    bc.create_table(
        "all_transactions", 
        cursor, 
        file_format="parquet", 
        hive_table_name="fin_transactions", 
        partitions = {
            "t_year":[2017, 2018], 
            "t_company_id":[2, 4, 6]
        }
    )

Advanced Usage: No Hive Cursor
------------------------------

You can also create a table that has been partitioned by Hive, 
without using a Hive cursor. 

For this functionality, instead of providing the ``create_table`` 
statement with a Hive Cursor, you would provide it the base path of 
where the table is located in the Hive directory structure. If you only 
pass it the base path, BlazingSQL will attempt to traverse the whole 
directory structure and infer all the partitions and the partitions' schema.

You can also manually use the ``partitions`` argument and the 
``partitions_schema`` argument.
When using the ``partitions`` argument, you must also provide at least 
one partition for every partitioned column. The list of partitioned 
columns must also be provided in the order in which Hive partitioned 
those columns. With the ``partitions_schema`` argument you provide the 
column name and type for all partitioned columns. If using the 
``partitions`` argument, you must also use the ``partitions_schema`` argument.

Parameters
~~~~~~~~~~

* **partitions** - dictionary. A dictionary object where the keys are the column names for all partitioned columns, and the values are all the values of the partitions of interest. Note that not all partitions must be included, but all partitioned columns must be included and at least one partition per partitioned column. **Default:** ``None``.
* **partitions_schema** - list of tuples. A list of tuples of the column name and column type for the partitioned columns. **Default:** ``None``.

Example
~~~~~~~

.. code-block:: python

    from blazingsql import BlazingContext
    
    # start up BlazingSQL
    bc = BlazingContext()
    
    location="hdfs://localhost:54310/user/hive/warehouse/fin_transactions"
    
    # This is the same table as the "asia_transactions" example above, but 
    # without using the hive cursor
    bc.create_table(
        "asia_transactions2", 
        location, 
        file_format="parquet", 
        hive_table_name="fin_transactions", 
        partitions={
            "t_year":[2017, 2018], 
            "t_company_id":[2, 4, 6], 
            "region": ["asia"]
        }, 
        partitions_schema=[
            ("t_year","int"),
            ("t_company_id","int"),
            ("region","str")
        ]
    )