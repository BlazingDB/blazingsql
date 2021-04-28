ASIN
^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the degree value (in radians, range :math:`[0,\pi]`) for each row of a numeric column
where each value represents a value of a sine function.

.. warning:: Returns :code:`NULL` for :code:`+inf` and :code:`-inf`, and an actual :code:`NULL` value. 
    Throws an error for values outside of the :math:`[-1,1]` range.

.. seealso:: :ref:`sql_math_acos`, :ref:`sql_math_atan`, :ref:`sql_math_cos`, :ref:`sql_math_sin`, :ref:`sql_math_tan`

Example
"""""""

.. code-block:: sql

    SELECT ASIN(<col_1>)
    FROM <table_name>