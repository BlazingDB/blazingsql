SUM
~~~

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate sum of values of a numeric column.

.. seealso:: :ref:`sql_window_avg`, :ref:`sql_window_max`, :ref:`sql_window_min`

Examples
""""""""

Sum of values per partition. This is useful to later compute share of each 
value in the total value per partition.

.. code-block:: sql

    SELECT <col_1>
        , SUM(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>

Calculate cumulative sum.

.. code-block:: sql

    SELECT <col_1>
        , MIN(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
            ROWS UNBOUNDED PRECEDING
        )
    FROM <table_name>

 