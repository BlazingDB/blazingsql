MAX
~~~

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate maximum value of a numeric column.

.. seealso:: :ref:`sql_window_avg`, :ref:`sql_window_min`, :ref:`sql_window_sum`

Examples
""""""""

Maximum of values per partition.

.. code-block:: sql

    SELECT <col_1>
        , MAX(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>

Maximum of values in a moving window.

.. code-block:: sql

    SELECT <col_1>
        , MAX(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        )
    FROM <table_name>
