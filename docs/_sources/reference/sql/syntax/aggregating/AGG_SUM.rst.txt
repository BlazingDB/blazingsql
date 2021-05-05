SUM
^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate a sum of all non-:code:`NULL` values in the specified column.

.. seealso:: :ref:`sql_agg_avg`, :ref:`sql_agg_count`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , SUM(<col_2>) AS sum_of_col_2
    FROM <table_name>
    GROUP BY <col_1>
