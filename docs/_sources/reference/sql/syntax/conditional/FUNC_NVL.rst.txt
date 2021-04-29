NVL
^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

This function returns a default if the value in the column is :code:`NULL`. 

.. note:: The column types and/or literals used in this function need to be of compatible data types or 
    types that can be converted to a compatible type.

.. seealso:: :ref:`sql_func_coalesce`, :ref:`sql_func_nullif`

Example
"""""""

.. code-block:: sql

    SELECT NVL(<col_1>, <literal>)
    FROM <table_name>