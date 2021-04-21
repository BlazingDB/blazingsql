SELECT
------

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Returns rows and columns from a table.

Examples
""""""""

:code:`SELECT *`
~~~~~~~~~~~~~~~~

Selects all the data from the data, including all the columns and all the rows.

.. code-block:: sql

    SELECT *
    FROM <table_name>

:code:`SELECT expr`
~~~~~~~~~~~~~~~~~~~

Allows selecting one or a subset of columns from a table. It also supports
aliasing columns with different names using the :code:`AS` qualifier.

.. code-block:: sql

    SELECT <col_1>
        , <col_2> AS <bar>
    FROM <table_name>

Tables can also be aliased; BlazingSQL supports selecting all 

.. code-block:: sql

    SELECT t.*
    FROM <table_name> AS t

or a subset of columns from an aliased 
table.

.. code-block:: sql

    SELECT t.<col_1>
        , t.<col_2> AS <bar>
    FROM <table_name> AS t

The :code:`SELECT expr` further allows for including unary and binary functions such as :ref:`arithmetic<sql_arithmetic>`,
:ref:`string<sql_strings>`, :ref:`timestamp<sql_dates>`, :ref:`windowing<sql_windowing>`, and :ref:`conditional<sql_functions>`.

:code:`LIMIT`
~~~~~~~~~~~~~

Returns only a specified number of rows, normally from the top of the table, 
or from the top of the table in the first partition in case of a distributed
table.

.. code-block:: sql

    SELECT *
    FROM <table_name>
    LIMIT <number_of_rows>