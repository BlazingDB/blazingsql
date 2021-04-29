AVG
~~~

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate average of a numeric column.

.. seealso:: :ref:`sql_window_max`, :ref:`sql_window_min`, :ref:`sql_window_sum`

Examples
""""""""

Average of values per partition.

.. code-block:: sql

    SELECT <col_1>
        , AVG(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>
    GROUP BY <col_1>

Moving average.

.. code-block:: sql

    SELECT <col_1>
        , AVG(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        )
    FROM <table_name>
    GROUP BY <col_1>