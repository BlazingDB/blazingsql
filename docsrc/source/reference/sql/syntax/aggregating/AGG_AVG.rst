AVG
^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate average of a numeric column.

.. seealso:: :ref:`sql_agg_count`, :ref:`sql_agg_sum`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , AVG(<col_2>) AS average
    FROM <table_name>
    GROUP BY <col_1>
