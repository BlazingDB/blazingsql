ROUND
^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the value rounded to the nearest integer. Allows specifying the precision :code:`N` that indicates
the number of decimal places to use for rounding.

.. seealso:: :ref:`sql_math_ceil`, :ref:`sql_math_floor`

Examples
""""""""

Round to the nearest integer.

.. code-block:: sql

    SELECT ROUND(<col_1>)
    FROM <table_name>

Round to the nearest hundredth.

.. code-block:: sql

    SELECT ROUND(<col_1>, 2)
    FROM <table_name>
