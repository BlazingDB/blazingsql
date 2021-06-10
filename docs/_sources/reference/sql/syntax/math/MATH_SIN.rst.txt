SIN
^^^


**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the sine value for each row of a numeric column. The input value is interpreted as radians, not degrees.

.. seealso:: :ref:`sql_math_acos`, :ref:`sql_math_asin`, :ref:`sql_math_atan`, :ref:`sql_math_cos`, :ref:`sql_math_tan`


Example
"""""""

.. code-block:: sql

    SELECT SIN(<col_1>)
    FROM <table_name>
