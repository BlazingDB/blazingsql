MIN
^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate a minimum value in the specified column.

.. seealso:: :ref:`sql_agg_max`


Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , MIN(<col_2>) AS minimum
    FROM <table_name>
    GROUP BY <col_1>
