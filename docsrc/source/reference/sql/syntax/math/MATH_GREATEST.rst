GREATEST
^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the greatest value from a list of columns or literals.

.. seealso:: :ref:`sql_math_least`

Example
"""""""

Compare values between columns.

.. code-block:: sql

    SELECT GREATEST(<col_1>, <col_2>)
    FROM <table_name>

An equivalent notation

.. code-block:: sql

    SELECT CASE 
        WHEN <col_1> >= <col_2> THEN <col_1>
        ELSE <col_2>
    END
    FROM <table_name>
