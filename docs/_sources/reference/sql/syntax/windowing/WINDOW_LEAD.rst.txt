LEAD
~~~~

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Return the value from the following row (or rows, if offset specified) in the specified window.

.. seealso:: :ref:`sql_window_lag`

Examples
""""""""

Return value from the following row.

.. code-block:: sql

    SELECT <col_1>
        , LEAD(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>

Offset by 2 rows.

.. code-block:: sql

    SELECT <col_1>
        , LEAD(<col_2>, 2) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>

