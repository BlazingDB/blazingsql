CONCAT
^^^^^^

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Obtain a string that is a concatenation of two or more text columns or literals.

.. warning:: This function returns :code:`NULL` if any of the elements is :code:`NULL`.

Examples
""""""""

Concatenating two text columns.

.. code-block:: sql

    SELECT CONCAT(<col_1>, <col_2>)
    FROM <table_name>

Concatenating two columns with an additional delimiter between.

.. code-block:: sql

    SELECT CONCAT(<col_1>, '_', <col_2>)
    FROM <table_name>