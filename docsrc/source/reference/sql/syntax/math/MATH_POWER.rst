POWER
^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the result of raising value to a specified power: the first parameter to the :code:`POWER` function 
is the *base* and the second one is the *exponent*.

.. seealso:: :ref:`sql_math_sqrt`

Examples
""""""""

The base and the exponent are both columns.

.. code-block:: sql

    SELECT POWER(<col_1>, <col_2>)
    FROM <table_name>

The base is a column and the exponent is a literal.

.. code-block:: sql

    SELECT POWER(<col_1>, <literal>)
    FROM <table_name>
