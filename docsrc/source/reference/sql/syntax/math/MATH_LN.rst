LN
^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the value of a natural logarithm for each row in a numerical column.

.. warning:: Throws an error for values smaller than :code:`0`.

.. seealso:: :ref:`sql_math_log10`


Example
"""""""

.. code-block:: sql

    SELECT LN(<col_1>)
    FROM <table_name>
