LOG10
^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the value of a logarithm with base 10 for each row in a numerical column.

.. warning:: Returns :code:`NULL` for :code:`+inf` and :code:`-inf`. Throws an error for values smaller than :code:`0`.

.. seealso:: :ref:`sql_math_ln`


Example
"""""""

.. code-block:: sql

    SELECT LOG10(<col_1>)
    FROM <table_name>
