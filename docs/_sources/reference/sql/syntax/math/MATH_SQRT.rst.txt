SQRT
^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the square root of value.

.. seealso:: :ref:`sql_math_power`

Examples
""""""""

Find the square root of each value in a column.

.. code-block:: sql

    SELECT SQRT(<col_1>)
    FROM <table_name>

Equivalently.

.. code-block:: sql

    SELECT POWER(<col_1>, 0.5)
    FROM <table_name>
