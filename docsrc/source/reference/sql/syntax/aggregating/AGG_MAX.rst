MAX
^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate a maximum value in the specified column.

.. seealso:: :ref:`sql_agg_min`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , MAX(<col_2>) AS maximum
    FROM <table_name>
    GROUP BY <col_1>
