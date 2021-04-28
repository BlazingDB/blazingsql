CEIL
^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the value rounded **up** to the nearest integer.

.. seealso:: :ref:`sql_math_floor`, :ref:`sql_math_round`


Example
"""""""

.. code-block:: sql

    SELECT CEIL(<col_1>)
    FROM <table_name>
