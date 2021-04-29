MOD
^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain the remainder of the division between two columns or literals: the first parameter to the :code:`MOD` function 
is the *divident* and the second one is the *divider*.

.. warning:: Throws an error if the divider equals :code:`0`.


Examples
""""""""

The divident and the divider are both columns.

.. code-block:: sql

    SELECT MOD(<col_1>, <col_2>)
    FROM <table_name>

The divident is a column and the divider is a literal.

.. code-block:: sql

    SELECT MOD(<col_1>, <literal>)
    FROM <table_name>
