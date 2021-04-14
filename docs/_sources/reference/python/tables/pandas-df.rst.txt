Query pandas DataFrames
=======================

You can easily build a BlazingSQL table off of a pandas DataFrame, 
by only providing the table name and the DataFrame itself.

.. code-block:: python

    import blazingsql
    import pandas

    bc = blazingsql.BlazingContext()

    table_df = pandas.read_csv('...file_path/table.csv')

    bc.create_table('table_name', table_df)


Parameters
~~~~~~~~~~

* **table_name** - string. **Required.** Name of the table you are creating. 
* **pandas.DataFrame** - ``pandas.DataFrame``. **Required.** The DataFrame you wish to query.