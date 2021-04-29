COALESCE
^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

This function returns a default if the value in the column is :code:`NULL`. 

.. note:: The column types and/or literals used in this function need to be of compatible data types or 
    types that can be converted to a compatible type.

.. seealso:: :ref:`sql_func_nvl`, :ref:`sql_func_nullif`

Example
"""""""

The simplest example returns a literal for every :code:`NULL` in a column.

.. code-block:: sql

    SELECT COALESCE(<col_1>, <literal>)
    FROM <table_name>

More than one column can be included and the function will return
the first value from the series that is not :code:`NULL`.

.. note:: The list of columns will be evaluated left-to-right thus,
    i.e. in an expression :code:`COALESCE(<col_1, <col_2>, <literal>)`, 
    if :code:`col_1` is :code:`NULL` but :code:`col_2` is not, the value 
    found in :code:`col_2` will be returned, not the :code:`literal`.

.. code-block:: sql

    SELECT COALESCE(<col_1>, <col_2>, <literal>)
    FROM <table_name>