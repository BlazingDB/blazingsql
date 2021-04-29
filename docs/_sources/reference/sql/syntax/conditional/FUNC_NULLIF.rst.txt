NULLIF
^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

This function returns a :code:`NULL` if the value matches the expression. 

.. note:: The column types and/or literals used in this function need to be of compatible data types or 
    types that can be converted to a compatible type.

.. seealso:: :ref:`sql_func_coalesce`

Example
"""""""

.. code-block:: sql

    SELECT NULLIF(<col_1>, <literal_1>)
    FROM <table_name>
